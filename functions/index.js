const { onSchedule } = require('firebase-functions/v2/scheduler');
const { onCall, HttpsError } = require('firebase-functions/v2/https');
const logger = require('firebase-functions/logger');
const { initializeApp } = require('firebase-admin/app');
const admin = require('firebase-admin');

initializeApp();

const TRIM_RETENTION_MS = 1000 * 60 * 60; // 1 hora
const TRIM_BATCH_LIMIT = 200;
const ADMIN_USER = 'fundador666';
const ADMIN_PASS = 'linea2bet';
const ADMIN_CLEAR_TOKEN = Buffer.from(`${ADMIN_USER}:${ADMIN_PASS}`).toString('base64');

async function deleteCollectionBatched(colName, batchSize = 500) {
  const db = admin.firestore();
  let lastDoc = null;
  let total = 0;
  for (;;) {
    let q = db.collection(colName).orderBy(admin.firestore.FieldPath.documentId()).limit(batchSize);
    if (lastDoc) q = q.startAfter(lastDoc);
    const snap = await q.get();
    if (snap.empty) break;
    const batch = db.batch();
    snap.docs.forEach((doc) => batch.delete(doc.ref));
    await batch.commit();
    total += snap.size;
    lastDoc = snap.docs[snap.docs.length - 1];
  }
  return total;
}

async function resetConversationsMetadata(db) {
  const snap = await db.collection('conversaciones').get();
  if (snap.empty) return 0;

  let total = 0;
  let batch = db.batch();
  let count = 0;
  const template = {
    lastMessageType: null,
    lastMessagePreview: '',
    lastMessageTs: null,
    lastSender: null,
    lastSenderRole: null,
    hasHistory: false,
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  };

  for (const docSnap of snap.docs) {
    const data = docSnap.data() || {};
    const alreadyReset = !data.hasHistory && !data.lastMessageTs && !data.lastMessageType && !data.lastMessagePreview;
    if (alreadyReset) {
      continue;
    }

    batch.set(docSnap.ref, template, { merge: true });
    count += 1;
    total += 1;
    if (count === 400) {
      await batch.commit();
      batch = db.batch();
      count = 0;
    }
  }

  if (count > 0) {
    await batch.commit();
  }

  return total;
}

function buildMetadataPayload(userKey, msg) {
  if (!msg) {
    return {
      userKey,
      hasHistory: false,
      lastMessageType: null,
      lastMessagePreview: '',
      lastMessageTs: null,
      lastSender: null,
      lastSenderRole: null,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    };
  }

  const type = msg.audioDataUrl
    ? 'audio'
    : (msg.imageDataUrl || msg.imageUrl)
      ? 'image'
      : (msg.type || 'text');

  let preview = '';
  if (type === 'text' && msg.text) {
    preview = String(msg.text).slice(0, 120);
  } else if (type === 'image') {
    preview = msg.name ? `📷 ${msg.name}` : '📷 Imagen enviada';
  } else if (type === 'audio') {
    preview = '🎤 Audio enviado';
  }

  return {
    userKey,
    hasHistory: true,
    lastMessageType: type,
    lastMessagePreview: preview,
    lastMessageTs: msg.ts || null,
    lastSender: msg.sender || null,
    lastSenderRole: msg.role || null,
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  };
}

async function updateConversationMetadata(db, userKey, msg) {
  const payload = buildMetadataPayload(userKey, msg);
  await db.collection('conversaciones').doc(userKey).set(payload, { merge: true });
}

async function trimConversation(rtdb, db, userKey, cutoffTs) {
  const convRef = rtdb.ref(`chat/conversaciones/${userKey}`);
  let removed = 0;

  for (;;) {
    const snap = await convRef
      .orderByChild('ts')
      .endAt(cutoffTs)
      .limitToFirst(TRIM_BATCH_LIMIT)
      .once('value');

    if (!snap.exists()) break;

    const updates = {};
    snap.forEach(child => {
      updates[child.key] = null;
    });

    const batchSize = Object.keys(updates).length;
    if (!batchSize) break;

    await convRef.update(updates);
    removed += batchSize;

    if (batchSize < TRIM_BATCH_LIMIT) break;
  }

  if (removed > 0) {
    const latestSnap = await convRef
      .orderByChild('ts')
      .limitToLast(1)
      .once('value');

    let latestMsg = null;
    latestSnap.forEach(child => {
      latestMsg = child.val() || null;
    });

    await updateConversationMetadata(db, userKey, latestMsg);
  }

  return removed;
}

exports.trimRealtimeConversations = onSchedule(
  {
    schedule: '*/10 * * * *',
    timeZone: 'America/Argentina/Buenos_Aires',
    retryCount: 0,
    maxInstances: 1
  },
  async () => {
    const rtdb = admin.database();
    const db = admin.firestore();
    const cutoffTs = Date.now() - TRIM_RETENTION_MS;

    const userKeys = new Set();

    try {
      const metaSnap = await db.collection('conversaciones').get();
      metaSnap.forEach(doc => userKeys.add(doc.id));
    } catch (err) {
      logger.error('No se pudo leer metadata de conversaciones', err);
    }

    try {
      const aliasSnap = await rtdb.ref('chat/aliases').once('value');
      aliasSnap.forEach(child => userKeys.add(child.key));
    } catch (err) {
      logger.error('No se pudo leer aliases', err);
    }

    const summary = {
      scannedUsers: userKeys.size,
      trimmedMessages: 0,
      affectedUsers: []
    };

    for (const userKey of userKeys) {
      if (!userKey) continue;
      try {
        const removed = await trimConversation(rtdb, db, userKey, cutoffTs);
        if (removed > 0) {
          summary.trimmedMessages += removed;
          summary.affectedUsers.push({ userKey, removed });
        }
      } catch (err) {
        logger.error(`Error limpiando conversación ${userKey}`, err);
      }
    }

    if (summary.trimmedMessages > 0) {
      logger.info('Limpieza periódica completada', summary);
    }
  }
);

exports.adminClearChats = onCall({ cors: true }, async (request) => {
  const token = request?.data?.token;
  if (token !== ADMIN_CLEAR_TOKEN) {
    throw new HttpsError('permission-denied', 'Token inválido');
  }

  const rtdb = admin.database();
  const db = admin.firestore();

  let deletedRTDB = 0;
  const convSnap = await rtdb.ref('chat/conversaciones').once('value');
  const conversations = convSnap.val() || {};
  const userKeys = Object.keys(conversations);

  for (const userKey of userKeys) {
    const msgs = conversations[userKey] || {};
    deletedRTDB += Object.keys(msgs).length;

    try {
      await rtdb.ref(`chat/conversaciones/${userKey}`).remove();
    } catch (err) {
      logger.error(`No se pudo borrar conversación ${userKey} en RTDB`, err);
    }

    try {
      await db.collection('conversaciones').doc(userKey).set({
        lastMessageType: null,
        lastMessagePreview: '',
        lastMessageTs: null,
        lastSender: null,
        lastSenderRole: null,
        hasHistory: false,
        updatedAt: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });
    } catch (err) {
      logger.error(`No se pudo resetear metadata para ${userKey}`, err);
    }
  }

  let deletedFS = 0;
  try {
    deletedFS = await deleteCollectionBatched('mensajes', 300);
  } catch (err) {
    logger.error('No se pudieron borrar mensajes en Firestore', err);
  }

  return { deletedRTDB, deletedFS };
});

exports.dailyCleanup = onSchedule(
  {
    schedule: '0 4 * * *', // todos los dias 04:00
    timeZone: 'America/Argentina/Buenos_Aires',
    retryCount: 0,
    maxInstances: 1
  },
  async () => {
    const rtdb = admin.database();
    const db = admin.firestore();
    try {
      await rtdb.ref('chat/conversaciones').remove();
      logger.info('RTDB: mensajes eliminados de conversaciones');
    } catch (err) {
      logger.error('Error limpiando RTDB', err);
    }
    try {
      const delMensajes = await deleteCollectionBatched('mensajes', 500);
      const resetCount = await resetConversationsMetadata(db);
      logger.info(`Base de datos: borrados ${delMensajes} mensajes, metadata reseteada en ${resetCount} conversaciones`);
    } catch (err) {
      logger.error('Error limpiando Base de datos', err);
    }
  }
);
