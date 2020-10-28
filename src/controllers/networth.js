import "dotenv/config";

import * as titans from "./titans";

import * as queue from "../queue";
// import * as queue from "../queue2";

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
    Bucket: process.env.AWS_BUCKET_INTRINIO_ZAKS,
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

export async function updateNetWorth_Billionaires() {
  let result = await titans.getBillionaires({ size: 5000 });

  let records = result;

  if (records.length > 0) {
    for (let i = 0; i < records.length; i += 1) {
      let id = records[i].id;
      if (id > 0) {
        await queue.publish_ProcessNetWorth(id);
      }
    }
  }
}
