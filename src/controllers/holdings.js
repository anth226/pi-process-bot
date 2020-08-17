import "dotenv/config";

import axios from "axios";

import db from "../db";

const chalk = require("chalk");

const AWS = require("aws-sdk");

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

import * as titans from "./titans";

import * as queue from "../queue";
// import * as queue from "../queue2";

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
  batchId = null,
  cache = true
) {
  let next_page = null;
  let index = 0;
  let holdings = [];

  let buffer = [];

  let key = null;

  do {
    let response = await getInstitutionalHoldings(cik, next_page);
    next_page = response["next_page"];

    if (cache) {
      key = `holdings/${cik}/${index}.json`;
      await uploadToS3(key, response);
    }

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
    if (cache) {
      key = `holdings/historical/${cik}/${Number(new Date())}.json`;
      let response = await uploadToS3(key, buffer);

      let query = {
        text:
          "INSERT INTO holdings (cik, batch_id, data_url, created_at ) VALUES ( $1, $2, $3, now() ) RETURNING *",
        values: [cik, batchId, response["Location"]],
      };
      await db(query);
      console.log("holdings_historical => cached");
    }

    console.log(
      chalk.bgGreen("batch complete."),
      cik,
      batchId,
      cache,
      buffer.length
    );
  }
}

export async function cacheHoldings_Billionaires() {
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
    batchId = Number(result[0]["batch_id"]) + 1;
  }

  //
  result = await titans.getBillionairesCiks({ size: 1000 });

  let records = result;

  let buffer = [];

  if (records.length > 0) {
    for (let i = 0; i < records.length; i += 1) {
      let ciks = records[i].ciks;
      let id = records[i].id;

      if (ciks && ciks.length > 0) {
        for (let j = 0; j < ciks.length; j += 1) {
          let cik = ciks[j];
          if (cik.cik != "0000000000" && cik.is_primary == true) {
            console.log(cik.cik);

            queue.publish_ProcessHoldings(
              cik.cik,
              id,
              batchId,
              !buffer.includes(cik.cik)
            );

            if (buffer.includes(cik.cik)) {
              buffer.push(cik.cik);
            }
          }
        }
      }
      /* This is case to include old way of grabbing ciks from the
          billionaires table just in case we have both kinds of data
          can be taken out if we do full transition
      
      else {
        let cik = records[i].cik;
        let id = records[i].id;

        if (cik) {
          console.log(cik);
  
          queue.publish_ProcessHoldings(cik, id, batchId, !buffer.includes(cik));
  
          if (buffer.includes(cik)) {
            buffer.push(cik);
          }
        }
      }
      */
    }
  }
}

export async function fetchAll(cik) {
  let result = await db(`
    SELECT *
    FROM holdings
    WHERE cik='${cik}'
    ORDER BY batch_id DESC
    LIMIT 1
  `);

  // console.log(result);

  if (result.length > 0) {
    let { data_url } = result[0];
    try {
      let result = await axios.get(data_url, {
        crossdomain: true,
        withCredentials: false,
        headers: {
          "Content-Type": "application/json",
          // "Access-Control-Allow-Origin": "*"
        },
      });

      let holdings = result.data;
      return holdings;
    } catch {}
  }

  return [];
}

export async function getAllMaxBatch() {
  return await db(`
  SELECT *
  FROM holdings h
  INNER JOIN (
      SELECT cik, MAX(batch_id) AS MaxBatch
      FROM holdings
      GROUP BY cik
  ) hb ON h.cik = hb.cik AND h.batch_id = hb.MaxBatch
  `);
}
