

const firestoreService = require('firestore-export-import');
const serviceAccount = require('./serviceAccountKey.json');

const databaseURL = "https://stocklab-8255c.firebaseio.com"

capital_social = './b3/output_b3/capital_social.json'
dados_cia = './b3/output_b3/dados_cia.json'
dre = './cvm/output_cvm/dre.json'

firestoreService.initializeApp(serviceAccount, databaseURL);

console.log('Uploading...')

firestoreService.restore(dre).catch( error =>  console.log(error.message));

//firestoreService.restore(dados_cia).catch( error =>  console.log(error.message));