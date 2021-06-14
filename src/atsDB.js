import {getEnv} from "./env";

const { Client } = require("pg");
const types = require("pg").types;

let db;

const TIMESTAMP_OID = 1114;
types.setTypeParser(TIMESTAMP_OID, function (value) {
    return value && new Date(value + "+00");
});

function connectDatabase() {
    if (!db) {
        const client = new Client({
            database: getEnv("DATABASE_NAME"),
            host: getEnv("AWS_POSTGRES_DB_ATS_HOST"),
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