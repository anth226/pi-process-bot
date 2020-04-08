const { Client } = require("pg");

let db;

function connectDatabase() {
  if (!db) {
    const client = new Client({
      database: process.env.DATABASE_NAME,
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
