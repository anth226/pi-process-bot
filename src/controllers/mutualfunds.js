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
      let json = JSON.stringify(records[i]);
      let ticker = records[i].ticker;

      if (json && ticker) {
        await queue.publish_ProcessMutualFunds(json, ticker);
      }
    }
  }
}

//Useless comment

export async function insertMutualFund(json, ticker) {
  if (!json || !ticker) {
    return;
  }

  console.log("\ninsert func");
  console.log(ticker);
  console.log(json + "\n");

  // let query = {
  //   text: "SELECT * FROM mutual_funds WHERE ticker = $1",
  //   values: [ticker],
  // };
  // let result = await db(query);

  // console.log(result);

  // if (result.length > 0) {
  //   let query = {
  //     text:
  //       "UPDATE mutual_funds (json, updated_at) VALUES ( $1, now() ) WHERE ticker = $2",
  //     values: [json, ticker],
  //   };
  //   await db(query);
  // } else {
  //   let query = {
  //     text:
  //       "INSERT INTO mutual_funds (json, updated_at, ticker ) VALUES ( $1, now(), $2 ) RETURNING *",
  //     values: [json, ticker],
  //   };
  //   await db(query);
  // }
}
