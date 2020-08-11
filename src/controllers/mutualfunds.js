import "dotenv/config";
import axios from "axios";
import db from "../db";

import * as titans from "./titans";

import * as queue from "../queue";

/*
import axios from "axios";

import db from "../db";

const chalk = require("chalk");

const AWS = require("aws-sdk");

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});








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

*/

export async function getMutualFunds() {
  let funds = [];

  try {
    let url = `https://fds1.cannonvalleyresearch.com/api/v1/report/dailySummary.json?apiKey=${process.env.CANNON_API_KEY}`;
    const result = await axios.get(url);
    funds = result.data;
  } catch (e) {
    console.error(e);
  }
  return funds;
}

export async function updateDB_MutualFunds() {
  let result = await getMutualFunds();

  let records = result;

  if (records.length > 0) {
    for (let i = 0; i < records.length; i += 1) {
      let cik = records[i].cik;
      let json = JSON.stringify(records[i]);
      let ticker = records[i].ticker;

      if (cik > 0) {
        await queue.publish_ProcessMutualFunds(cik, json, ticker);
      }
    }
  }
}

export async function insertMutualFund(cik, json, ticker) {
  if (!cik || !json || !ticker) {
    return;
  }

  let query = {
    text:
      "INSERT INTO mutual_funds (id, json, updated_at, ticker ) VALUES ( $1, $2, now(), $3 ) RETURNING *",
    values: [cik, json, ticker],
  };
  await db(query);
}
