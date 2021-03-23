import {getEnv} from "./env";

const { Client } = require("pg");

let db;

import "dotenv/config";

function connectDatabase() {
  if (!db) {
    const client = new Client({
      database: getEnv("DATABASE_NAME"),
      host: getEnv("DATABASE_HOST"),
      port: getEnv("DATABASE_PORT"),
      user: getEnv("DATABASE_USER"),
      password: getEnv("DATABASE_PASSWORD"),
    });

    client.connect();

    db = async (sql, cb) => (await client.query(sql, cb)).rows;
  }
  return db;
}

export default connectDatabase();
