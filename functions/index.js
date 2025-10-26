const { onSchedule } = require('firebase-functions/v2/scheduler');
const logger = require('firebase-functions/logger');
const { initializeApp } = require('firebase-admin/app');
const admin = require('firebase-admin');

initializeApp();

const TRIM_RETENTION_MS = 1000 * 60 * 60; // 1 hora
const TRIM_BATCH_LIMIT = 200;

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
      await rtdb.ref('chat/aliases').remove();
      logger.info('RTDB: conversaciones y aliases eliminados');
    } catch (err) {
      logger.error('Error limpiando RTDB', err);
    }
    try {
      const delMensajes = await deleteCollectionBatched('mensajes', 500);
      const delConversaciones = await deleteCollectionBatched('conversaciones', 500);
      logger.info(`Base de datos: borrados ${delMensajes} mensajes, ${delConversaciones} conversaciones`);
    } catch (err) {
      logger.error('Error limpiando Base de datos', err);
    }
  }
);
