const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");

const puppeteer = require("puppeteer");
const chalk = require("chalk");

var request = require("request");

import db from "../db";

import * as zip from "./zip";

const AWS = require("aws-sdk");
require("dotenv").config();

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

(async () => {
  await zip.zipPerformances_Billionaires();
  // await fetch_Photo();
})();

async function import_Billionaires() {
  fs.createReadStream(path.join(__dirname, "billionaires.csv"))
    .pipe(csv())
    .on("data", async (row) => {
      if (
        row["Name"] != "" &&
        row["CIK"] != "" &&
        row["Personal Net Worth"] != "" &&
        row["Description"] != ""
      ) {
        console.log(row);
        console.log(
          row["Name"],
          row["CIK"],
          row["Personal Net Worth"],
          row["Description"]
        );

        let result = await db(`
          SELECT *
          FROM billionaires
          WHERE cik = '${row["CIK"]}'
        `);

        if (result) {
          let number = row["Personal Net Worth"];
          number = number.replace(/\D/g, "");

          if (result.length > 0) {
            let billionaire = result[0];

            let query = {
              text:
                "UPDATE billionaires SET name=($1), cik=($2), net_worth=($3), description=($4), institution_name=($5)  WHERE id=($6) RETURNING *",
              values: [
                row["Name"],
                row["CIK"],
                parseInt(number),
                row["Description"],
                row["Fund"],
                billionaire["id"],
              ],
            };

            result = await db(query);
          } else {
            let query = {
              text:
                "INSERT INTO billionaires (name, cik, net_worth, description, institution_name) VALUES ( $1, $2, $3, $4, $5 ) RETURNING *",
              values: [
                row["Name"],
                row["CIK"],
                parseInt(number),
                row["Description"],
                row["Fund"],
              ],
            };

            result = await db(query);
          }

          console.log(result);
        }
      }
    })
    .on("end", () => {
      console.log("CSV file successfully processed");
    });
}

function put_from_url(url, bucket, key, callback) {
  request(
    {
      url: url,
      encoding: null,
    },
    function (err, res, body) {
      if (err) return callback(err, res);

      s3.putObject(
        {
          ACL: "public-read",
          Bucket: bucket,
          Key: key,
          ContentType: res.headers["content-type"],
          ContentLength: res.headers["content-length"],
          Body: body, // buffer
        },
        callback
      );
    }
  );
}

async function fetch_Photo() {
  const browser = await puppeteer.launch({
    args: ["--no-sandbox"],
    headless: true,
  });
  const page = await browser.newPage();

  let rows = [];

  fs.createReadStream(path.join(__dirname, "billionaires.csv"))
    .pipe(csv())
    .on("data", async (row) => {
      // console.log(row);
      if (row["Photo"].startsWith("https://www.forbes.com")) {
        rows.push(row);
      }
    })
    .on("end", async () => {
      console.log("CSV file successfully processed");

      for (let i = 0; i < rows.length; i += 1) {
        console.log(rows[i]["Photo"]);

        let cik = rows[i]["CIK"];
        cik = cik.replace(/\s/g, "");

        let result = await db(`
          SELECT *
          FROM billionaires
          WHERE cik = '${cik}'
        `);

        if (result) {
          if (result.length > 0) {
            if (!result[0].photo_url) {
              await page.goto(rows[i]["Photo"]);

              // await page.waitForNavigation();

              // const screenshotPath = "/tmp/headless-test-result.png";
              // await page.screenshot({ path: screenshotPath });

              const srcAttribute = await page.$eval(
                ".profile-photo__img",
                (e) => e.src
              );
              console.log(srcAttribute);

              let path = `photos/${cik}.jpg`;

              put_from_url(
                srcAttribute,
                process.env.AWS_BUCKET_RI,
                path,
                function (err, res) {
                  if (err) throw err;

                  console.log("Uploaded data successfully!");
                }
              );

              let billionaire = result[0];
              let query = {
                text:
                  "UPDATE billionaires SET photo_url=($1) WHERE id=($2) RETURNING *",
                values: [
                  `https://ri-terminal.s3.amazonaws.com/photos/${cik}.jpg`,
                  billionaire["id"],
                ],
              };

              result = await db(query);
            }
          }
        }

        await page.waitFor(3000);
      }

      await browser.close();

      console.log(chalk.bgGreen("Done!"));
    });

  // console.log(chalk.bgGreen("start_puppeteer"));
}

// const createCsvWriter = require("csv-writer").createObjectCsvWriter;

// const csvWriter = createCsvWriter({
//   path: "report.csv",
//   header: [
//     { id: "name", title: "name" },
//     { id: "cik", title: "cik" }
//   ]
// });

// let portfolios = {
//   portfolios: []
// };

// let records = [];

// fs.readFile(`../src/components/titans/data/portfolios.json`, (err, data) => {
//   if (err) throw err;
//   portfolios = JSON.parse(data);

//   console.log(portfolios["portfolios"].length);

//   portfolios["portfolios"].forEach((portfolio, index) => {
//     records.push({
//       name: portfolio["name"],
//       cik: [portfolio["filer_cik"]]
//     });
//   });

//   csvWriter
//     .writeRecords(records)
//     .then(() => console.log("The CSV file was written successfully"));
// });
