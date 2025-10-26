const { onSchedule } = require('firebase-functions/v2/scheduler');
const logger = require('firebase-functions/logger');
const { initializeApp } = require('firebase-admin/app');
const admin = require('firebase-admin');

initializeApp();

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
