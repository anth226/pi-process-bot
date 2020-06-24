const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");

const puppeteer = require("puppeteer");
const chalk = require("chalk");

var request = require("request");

import db from "../db";

import * as zip from "./zip";
import * as holdings from "../controllers/holdings";
import * as performances from "../controllers/performances";
import * as companies from "../controllers/companies";
import * as titans from "../controllers/titans";
import * as queue from "../queue";

const AWS = require("aws-sdk");
require("dotenv").config();

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

// ALTER SEQUENCE billionaires_id_seq RESTART WITH 1;
// DELETE FROM billionaires;

(async () => {
  //
  // await import_Billionaires();
  // await fetch_Billionaire_Photos();
  //
  // await zip.zipPerformances_Billionaires();
  //
  // await holdings.cacheHoldings_Titans();
  //
  // let cik = "0001067983";
  // await performances.calculatePerformance_Billionaire(cik);
  // await companies.cacheCompanies_Portfolio(cik);
  // await titans.generateSummary(cik);
  //
  // //
  // let result = await titans.getBillionaires({});
  // for (let i = 0; i < result.length; i += 1) {
  //   let cik = result[i]["cik"];
  //   if (cik) {
  //     await titans.generateSummary(cik);
  //     console.log();
  //   }
  // }
  // //

  let ticker = "AAPL";
  await queue.publish_ProcessSecurityPrices(ticker);
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
          WHERE name = '${row["Name"]}'
        `);

        if (result) {
          let number = row["Personal Net Worth"];
          number = number.replace(/\D/g, "");

          let cik = row["CIK"];
          cik = cik.replace(/\s/g, "");

          if (result.length > 0) {
            let billionaire = result[0];

            let query = {
              text:
                "UPDATE billionaires SET name=($1), cik=($2), net_worth=($3), description=($4), institution_name=($5), photo_source=($6) WHERE id=($7) RETURNING *",
              values: [
                row["Name"],
                cik,
                parseInt(number),
                row["Description"],
                row["Fund"],
                row["Photo"],
                billionaire["id"],
              ],
            };

            result = await db(query);
          } else {
            let query = {
              text:
                "INSERT INTO billionaires (name, cik, net_worth, description, institution_name, photo_source) VALUES ( $1, $2, $3, $4, $5, $6 ) RETURNING *",
              values: [
                row["Name"],
                cik,
                parseInt(number),
                row["Description"],
                row["Fund"],
                row["Photo"],
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

async function fetch_Billionaire_Photos() {
  const browser = await puppeteer.launch({
    args: ["--no-sandbox"],
    headless: true,
  });
  const page = await browser.newPage();

  let result = await db(`
    SELECT *
    FROM billionaires
  `);

  if (result && result.length > 0) {
    for (let i = 0; i < result.length; i += 1) {
      let { photo_source, id } = result[i];

      if (photo_source && photo_source.startsWith("https://www.forbes.com")) {
        await page.goto(photo_source);

        // await page.waitForNavigation();

        // const screenshotPath = "/tmp/headless-test-result.png";
        // await page.screenshot({ path: screenshotPath });

        //
        // FORBES PHOTOS ONLY
        const srcAttribute = await page.$eval(
          ".profile-photo__img",
          (e) => e.src
        );
        console.log(srcAttribute);

        let path = `photos/${id}.jpg`;

        put_from_url(srcAttribute, process.env.AWS_BUCKET_RI, path, function (
          err,
          res
        ) {
          if (err) throw err;

          console.log(chalk.bgGreen("uploaded"), photo_source, id);
        });
        // FORBES PHOTOS ONLY
        //

        let query = {
          text:
            "UPDATE billionaires SET photo_url=($1) WHERE id=($2) RETURNING *",
          values: [`https://ri-terminal.s3.amazonaws.com/photos/${id}.jpg`, id],
        };

        await db(query);
      }

      await page.waitFor(3000);
    }

    await browser.close();
  }
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

async function fetch_Photos() {
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
}
