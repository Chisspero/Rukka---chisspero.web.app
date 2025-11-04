const { onSchedule } = require('firebase-functions/v2/scheduler');
const { onCall, HttpsError } = require('firebase-functions/v2/https');
const logger = require('firebase-functions/logger');
const { initializeApp } = require('firebase-admin/app');
const admin = require('firebase-admin');
const { doc, setDoc, deleteField } = require('firebase-admin/firestore');

initializeApp();
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
    lmt: null,
    lmp: '',
    lmts: null,
    ls: null,
    lsr: null,
    hh: false,
    ua: admin.firestore.FieldValue.serverTimestamp()
  };

  for (const docSnap of snap.docs) {
    const data = docSnap.data() || {};
    const alreadyReset = !data.hh && !data.lmts && !data.lmt && !data.lmp;
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
      uk: userKey,
      hh: false,
      lmt: null,
      lmp: '',
      lmts: null,
      ls: null,
      lsr: null,
      ua: admin.firestore.FieldValue.serverTimestamp()
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
    uk: userKey,
    hh: true,
    lmt: type,
    lmp: preview,
    lmts: msg.ts || null,
    ls: msg.sender || null,
    lsr: msg.role || null,
    ua: admin.firestore.FieldValue.serverTimestamp()
  };
}

async function updateConversationMetadata(db, userKey, msg) {
  const payload = buildMetadataPayload(userKey, msg);
  await db.collection('conversaciones').doc(userKey).set(payload, { merge: true });
}

exports.optimizedFullCleanup = onSchedule(
  {
    schedule: '0 4 * * *', // Todos los días a las 04:00
    timeZone: 'America/Argentina/Buenos_Aires',
    maxInstances: 1,
    retryCount: 0
  },
  async () => {
    const rtdb = admin.database();
    const db = admin.firestore();
    try {
      await rtdb.ref('chat/conversaciones').remove();
    } catch (_) { /* silencio */ }

    try {
      const collections = await db.listCollections();
      for (const col of collections) {
        const colName = col.id;
        if (colName === 'adminState') continue;
        await deleteCollectionBatched(colName, 400);
      }
    } catch (_) { /* silencio */ }
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
        lmt: null,
        lmp: '',
        lmts: null,
        ls: null,
        lsr: null,
        hh: false,
        ua: admin.firestore.FieldValue.serverTimestamp()
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