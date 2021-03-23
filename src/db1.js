import {getEnv} from "./env";

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
      database: getEnv("DATABASE_NAME_TRACKDATA"),
      host: getEnv("DATABASE_HOST"),
      port: getEnv("DATABASE_PORT"),
      user: getEnv("DATABASE_USER"),
      password: getEnv("DATABASE_PASSWORD")
    });

    client.connect();

    db = async (sql, cb) => (await client.query(sql, cb)).rows;
  }
  return db;
}

export default connectDatabase();
