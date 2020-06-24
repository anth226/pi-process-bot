const fs = require("fs");
const admin = require('firebase-admin');
const json2csv = require('json2csv');


// init firebase
const serviceAccount = require("../../tower-93be8-firebase-adminsdk-o954n-87d13d583d.json");
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://tower-93be8.firebaseio.com",
});

// firebase db
const db = admin.firestore();

async function exportFirestoreUsers() {

     return db.collection('users')
     .get() 
     .then( async (querySnapshot) => {
        
        const users = [];

        querySnapshot.forEach(doc => {

             if ( doc.data().email) {
               users.push( doc.data() )
             }

        });

        const csv = await json2csv.parse(users, {fields :['email','firstName', 'lastName']} );
        fs.writeFileSync("users.csv", csv);

        
     })
    .catch(err => console.log(err) )
}


(async () => {
 
  await exportFirestoreUsers();
  //
})();
