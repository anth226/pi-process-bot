import axios from "axios";

const chalk = require("chalk");

import * as titans from "./titans";
import * as queue from "../queue";

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

export async function getPortfolios() {
  return {
    url: "https://ri-terminal.s3.amazonaws.com/portfolios.json",
  };
}

function getHistoricalData(cik, frequency, next_page = null) {
  let url = `${process.env.INTRINIO_BASE_PATH}/historical_data/${cik}/marketcap?frequency=${frequency}&api_key=${process.env.INTRINIO_API_KEY}`;

  if (next_page) {
    url = `${process.env.INTRINIO_BASE_PATH}/historical_data/${cik}/marketcap?frequency=${frequency}&next_page=${next_page}&api_key=${process.env.INTRINIO_API_KEY}`;
  }

  let data = axios
    .get(url)
    .then(function (res) {
      return res.data;
    })
    .catch(function (err) {
      console.log("error", cik);
      return {};
    });

  return data;
}

export async function calculatePerformance_Billionaire(
  cik,
  billionaireId,
  batchId = null,
  cache = true
) {
  let frequencies = [
    // "daily",
    // "weekly",
    // "monthly",
    "quarterly",
    "yearly",
  ];

  let marketcaps = {};

  for (let i = 0; i < frequencies.length; i += 1) {
    let frequency = frequencies[i];

    let next_page = null;
    let historical_data = [];
    let buffer = {};

    do {
      let response = await getHistoricalData(cik, frequency, next_page);
      next_page = response["next_page"];

      buffer = response["historical_data"];
      historical_data = historical_data.concat(buffer);

      console.log(chalk.bgGreen("next_page =>"), next_page);
    } while (next_page);

    marketcaps[frequency] = historical_data;
  }

  console.log("marketcaps", marketcaps);

  let key = `marketcaps/${cik}.json`;
  let response = await uploadToS3(key, marketcaps);

  console.log(response["Location"]);

  // let next_page = null;
  // let index = 0;
  // let holdings = [];
  // let buffer = [];
  // let key = null;
  // do {
  //   let response = await getInstitutionalHoldings(cik, next_page);
  //   next_page = response["next_page"];
  //   if (cache) {
  //     key = `holdings/${cik}/${index}.json`;
  //     await uploadToS3(key, response);
  //   }
  //   holdings = response["holdings"];
  //   buffer = buffer.concat(holdings);
  //   console.log(holdings.length);
  //   for (let n = 0; n < holdings.length; n += 1) {
  //     await cacheTicker(billionaireId, holdings[n]["company"]["ticker"]);
  //   }
  //   index += 1;
  //   console.log(chalk.bgGreen("next_page =>"), next_page);
  // } while (next_page);

  // let query = {
  //   text:
  //     "UPDATE institutions SET holdings_page_count=($1), holdings_updated_at=($2) WHERE cik=($3) RETURNING *",
  //   values: [index + 1, new Date(), cik],
  // };
  // await db(query);
  // if (buffer.length > 0) {
  //   // Cache all data
  //   if (cache) {
  //     key = `holdings/historical/${cik}/${Number(new Date())}.json`;
  //     let response = await uploadToS3(key, buffer);
  //     let query = {
  //       text:
  //         "INSERT INTO holdings (cik, batch_id, data_url, created_at ) VALUES ( $1, $2, $3, now() ) RETURNING *",
  //       values: [cik, batchId, response["Location"]],
  //     };
  //     await db(query);
  //     console.log("holdings_historical => cached");
  //   }
  //   console.log(
  //     chalk.bgGreen("batch complete."),
  //     cik,
  //     batchId,
  //     cache,
  //     buffer.length
  //   );
  // }
}

export async function cachePerformances_Billionaires() {
  let result = await titans.getBillionairesCiks({ size: 1000 });

  let records = result;

  let buffer = [];
  let batchId = 0;

  if (records.length > 0) {
    for (let i = 0; i < records.length; i += 1) {
      let ciks = records[i].ciks;
      let id = records[i].id;
      
      if (ciks && ciks.length > 0) {
        for (let j = 0; j < ciks.length; j += 1){
          let cik = ciks[j];
          if (cik.cik != "0000000000" && cik.is_primary == true){
            console.log(cik.cik);

            queue.publish_ProcessPerformances(cik.cik, id, batchId, !buffer.includes(cik.cik));

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
  
          queue.publish_ProcessPerformances(
            cik,
            id,
            batchId,
            !buffer.includes(cik)
          );  
          
          if (buffer.includes(cik)) {
            buffer.push(cik);
          }
        }
      }
      */
    }
  }
}

export async function generateSummaries_Billionaires() {
  let result = await titans.getBillionairesCiks({ size: 1000 });

  let records = result;

  if (records.length > 0) {
    for (let i = 0; i < records.length; i += 1) {
      let ciks = records[i].ciks;
      
      if (ciks && ciks.length > 0) {
        for (let j = 0; j < ciks.length; j += 1){
          let cik = ciks[j];
          if (cik.cik != "0000000000" && cik.is_primary == true){
            console.log(cik.cik);

            await queue.publish_ProcessSummaries(cik.cik);
            
          }
        }
      }
    }
  }
}
