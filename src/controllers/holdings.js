import "dotenv/config";

import db from "../db";

const chalk = require("chalk");

const AWS = require("aws-sdk");

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

import * as titans from "./titans";

import { getInstitutionalHoldings } from "../controllers/intrinio/get_institutional_holdings";

const uploadToS3 = async (key, data) => {
  let params = {
    Bucket: process.env.BUCKET_INTRINIO_ZAKS,
    Key: key,
    Body: JSON.stringify(data),
    ContentType: "application/json",
    ACL: "public-read",
  };

  const response = await s3.upload(params).promise();

  console.log(chalk.bgYellow("s3 =>"), response);

  return response;
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

export async function fetchHoldings_Billionaire(
  cik,
  billionaireId,
  batchId = null
) {
  let next_page = null;
  let index = 0;
  let holdings = [];

  let buffer = [];

  do {
    let response = await getInstitutionalHoldings(cik, next_page);
    next_page = response["next_page"];

    let key = `holdings/${cik}/${index}.json`;
    await uploadToS3(key, response);
    holdings = response["holdings"];

    buffer = buffer.concat(holdings);

    console.log(holdings.length);

    for (let n = 0; n < holdings.length; n += 1) {
      await cacheTicker(billionaireId, holdings[n]["company"]["ticker"]);
    }
    index += 1;
    console.log(chalk.bgGreen("next_page =>"), next_page);
  } while (next_page);

  let query = {
    text:
      "UPDATE institutions SET holdings_page_count=($1), holdings_updated_at=($2) WHERE cik=($3) RETURNING *",
    values: [index + 1, new Date(), cik],
  };

  await db(query);

  if (buffer.length > 0) {
    // Cache all data
    let key = `holdings/historical/${cik}/${Number(new Date())}.json`;
    let response = await uploadToS3(key, buffer);
    let query = {
      text:
        "INSERT INTO holdings (cik, batch_id, data_url, created_at ) VALUES ( $1, $2, $3, now() ) RETURNING *",
      values: [cik, batchId, response["Location"]],
    };
    await db(query);

    console.log(chalk.bgGreen("batch complete."), cik, batchId, buffer.length);
  }
}

export async function cacheHoldings_Titans() {
  let result = await db(`
    SELECT *
    FROM institutions
    ORDER BY name ASC
    LIMIT 1
  `);

  // console.log(result);

  // find batch_id
  result = await db(`
    SELECT *
    FROM holdings
    ORDER BY batch_id DESC
    LIMIT 1
  `);

  let batchId = 0;
  if (result.length > 0) {
    batchId = result[i]["batch_id"];
  }

  //
  result = await titans.getTitans({});

  console.log(result);

  if (result.length > 0) {
    for (let i = 0; i < result.length; i += 1) {
      let cik = result[i]["cik"];
      let id = result[i]["id"];

      if (cik) {
        console.log(cik);
        // await whalewisdom.fetchHoldings(cik);
        await fetchHoldings_Billionaire(cik, id, batchId);
      }
    }
  }
}
