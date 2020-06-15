import axios from "axios";

const chalk = require("chalk");

import db from "../db";

async function lookup(identifier) {
  let url = `${process.env.INTRINIO_BASE_PATH}/companies/${identifier}?api_key=${process.env.INTRINIO_API_KEY}`;

  let data = {};
  try {
    data = await axios.get(url);
  } catch {}
  return data;
}

export async function lookupCompany(identifier) {
  let result = await lookup(identifier);

  let { status, data } = result;
  if (status && status === 200) {
    console.log(data.ticker);

    result = await db(`
        SELECT *
        FROM companies
        WHERE ticker = '${identifier}'
    `);

    if (result && result.length == 0) {
      let query = {
        text:
          "INSERT INTO companies (json, ticker, updated_at) VALUES ( $1, $2, now() ) RETURNING *",
        values: [data, identifier],
      };
      result = await db(query);
    }
  } else {
    console.log(chalk.bgRed("error"), identifier);
  }
}
