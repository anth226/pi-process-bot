const fs = require("fs");
const path = require("path");
const chalk = require("chalk");

const AWS = require("aws-sdk");
require("dotenv").config();

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const dir = "/pages";

import * as institutions from "../../src/controllers/institutions";
import { getInstitutionalHoldings } from "../controllers/intrinio/get_institutional_holdings";

import db from "../../src/db";

function init() {
  console.log(chalk.bgGreen("init"));

  return fs
    .readdirSync(__dirname + dir)
    .filter((name) => path.extname(name) === ".json")
    .map((name) => require(path.join(__dirname, dir, name)));
}

const uploadToS3 = async (cik, index, data) => {
  let params = {
    Bucket: process.env.BUCKET_INTRINIO_ZAKS,
    Key: `holdings/${cik}/${index}.json`,
    Body: JSON.stringify(data),
    ContentType: "application/json",
    ACL: "public-read",
  };

  const response = await s3.upload(params).promise();

  console.log(chalk.bgYellow("s3 =>"), response);
};

const cacheTicker = async (id, ticker) => {
  let result = await db(`
    DELETE
    FROM billionaire_holdings
    WHERE billionaire_id = '${id}'
    AND ticker = '${ticker}'
  `);

  result = await db(`
    SELECT *
    FROM billionaire_holdings
    WHERE ticker = '${ticker}'
    AND billionaire_id = '${id}'
  `);

  if (result && result.length == 0) {
    console.log(chalk.bgYellow("caching: "), id, ticker);
    let query = {
      text:
        "INSERT INTO billionaire_holdings (billionaire_id, ticker) VALUES ( $1, $2 ) RETURNING *",
      values: [id, ticker],
    };

    result = await db(query);
  } else {
    console.log(chalk.bgGreen("cached: "), id, ticker);
  }
};

export async function seedInstitutions() {
  let pages = init();

  for (let i = 0; i < pages.length; i += 1) {
    let page = pages[i];
    let rows = page["rows"];

    for (let j = 0; j < rows.length; j += 1) {
      let row = rows[j];
      console.log(row["filer_permalink"], row["filer_cik"], row["name"][0]);

      let result = await institutions.getInstitutionByCIK(row["filer_cik"]);

      console.log(result);

      if (result.length == 0) {
        let query = {
          text:
            "INSERT INTO institutions (name, cik, json) VALUES ( $1, $2, $3) RETURNING *",
          values: [row["name"][0], row["filer_cik"], row],
        };

        await db(query);
      }
    }
  }
}
export async function fetchHoldings(cik) {
  let response = await getInstitutionalHoldings(cik);

  let next_page = null;
  if (response) {
    next_page = response["next_page"];

    console.log(next_page);

    let index = 0;
    await uploadToS3(cik, index, response);

    while (next_page) {
      index += 1;

      response = await getInstitutionalHoldings(cik, next_page);
      next_page = response["next_page"];
      console.log(response["holdings"][0]);
      console.log(next_page);

      await uploadToS3(cik, index, response);
    }

    let query = {
      text:
        "UPDATE institutions SET holdings_page_count=($1), holdings_updated_at=($2) WHERE cik=($3) RETURNING *",
      values: [index + 1, new Date(), cik],
    };

    await db(query);
  }
}
export async function fetchHoldings_Billionaire(cik, billionaireId) {
  let response = await getInstitutionalHoldings(cik);

  let next_page = null;

  let holdings = [];

  if (response) {
    next_page = response["next_page"];

    console.log(next_page);

    let index = 0;

    await uploadToS3(cik, index, response);
    holdings = response["holdings"];
    console.log(holdings.length);

    for (let n = 0; n < holdings.length; n += 1) {
      await cacheTicker(billionaireId, holdings[n]["company"]["ticker"]);
    }

    while (next_page) {
      index += 1;

      response = await getInstitutionalHoldings(cik, next_page);
      next_page = response["next_page"];
      // console.log(response["holdings"][0]);
      console.log(next_page);

      await uploadToS3(cik, index, response);
      holdings = response["holdings"];
      console.log(holdings.length);

      for (let n = 0; n < holdings.length; n += 1) {
        await cacheTicker(billionaireId, holdings[n]["company"]["ticker"]);
      }
    }

    let query = {
      text:
        "UPDATE institutions SET holdings_page_count=($1), holdings_updated_at=($2) WHERE cik=($3) RETURNING *",
      values: [index + 1, new Date(), cik],
    };

    await db(query);
  }
}

// {
//   company: { ticker: 'SU', name: 'SUNCOR ENERGY', exchange: 'NYSE' },
//   owner: { name: 'Soros Fund Management LLC', cik: '0001029160' },
//   as_of_date: '2019-12-31',
//   shares_held: '0.0',
//   shares_held_percent: '0.0',
//   shares_change: '0.0',
//   shares_change_percent: '100.0',
//   market_value: '0.0',
//   market_value_change: '0.0',
//   last_sec_filing_type: null,
//   last_sec_filing_date: null,
//   last_sec_filing_shares: null,
//   historical_holdings: [
//     { as_of_date: '2019-09-30', shares_held: '0.0' },
//     { as_of_date: '2019-06-30', shares_held: '200000.0' }
//   ]
// }
