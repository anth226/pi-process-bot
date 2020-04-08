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

module.exports = {
  seedInstitutions: async () => {
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
  },
  fetchHoldings: async (cik) => {
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
  },
};
