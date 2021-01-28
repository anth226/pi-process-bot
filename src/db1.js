const { Client } = require("pg");
const types = require("pg").types;

let db;

const TIMESTAMP_OID = 1114;
types.setTypeParser(TIMESTAMP_OID, function (value) {
  // Example value string: "2018-10-04 12:30:21.199"
  return value && new Date(value + "+00");
});

function connectDatabase() {
  if (!db) {
    const client = new Client({
      database: process.env.DATABASE_NAME_TRACKDATA,
      host: process.env.DATABASE_HOST,
      port: process.env.DATABASE_PORT,
      user: process.env.DATABASE_USER,
      password: process.env.DATABASE_PASSWORD
    });

    client.connect();

    db = async (sql, cb) => (await client.query(sql, cb)).rows;
  }
  return db;
}

export default connectDatabase();
