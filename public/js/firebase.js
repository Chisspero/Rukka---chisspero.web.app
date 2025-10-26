import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";
const firebaseConfig = {
  apiKey: "AIzaSyB3Fjr_lu4mRECh_QCbF1thuVfTYkf42eQ",
  authDomain: "chisspero.firebaseapp.com",
  projectId: "chisspero",
  storageBucket: "chisspero.firebasestorage.app",
  messagingSenderId: "178897843769",
  appId: "1:178897843769:web:4ec8db86e6dd045b16ffe8",
  measurementId: "G-7H53CKR4T6"
};

const app = initializeApp(firebaseConfig);
const analytics = getAnalytics(app);