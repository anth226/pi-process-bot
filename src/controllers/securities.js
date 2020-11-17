import db from "../db";
import axios from "axios";
import * as queue from "../queue";

import * as companies from "./companies";

export async function fillSecurities() {
  //get and fill companies
  let result = await db(`
            SELECT *
            FROM companies
        `);

  console.log("COMPANIES");
  for (let i in result) {
    let type = "common_stock";
    let ticker = result[i].ticker;
    let cik = result[i].cik;
    await queue.publish_ProcessMetrics_Securities(ticker, type, cik);
    console.log(ticker);
  }

  //get and fill mutual funds
  result = await db(`
          SELECT *
          FROM mutual_funds
      `);

  console.log("MUTUAL FUNDS");
  for (let i in result) {
    let type = "mutual_fund";
    let ticker = result[i].ticker;
    let cik = result[i].json.cik;
    await queue.publish_ProcessMetrics_Securities(ticker, type, cik);
    console.log(ticker);
  }

  //get and fill etfs
  result = await db(`
        SELECT *
        FROM etfs
    `);

  console.log("ETFS");
  for (let i in result) {
    let type = "etf";
    let ticker = result[i].ticker;
    await queue.publish_ProcessMetrics_Securities(ticker, type, "");
    console.log(ticker);
  }
  console.log("DONE");
}

export async function insertSecurity(metrics, ticker, type, cik) {
  if (!type || !ticker) {
    return;
  }

  let query = {
    text: "SELECT * FROM securities WHERE ticker = $1",
    values: [ticker],
  };
  let result = await db(query);

  if (result.length > 0) {
    let query = {
      text: "UPDATE securities SET json_metrics = $1 WHERE ticker = $2",
      values: [metrics, ticker],
    };
    await db(query);
  } else {
    let query = {
      text:
        "INSERT INTO securities (json_metrics, ticker, type, cik ) VALUES ( $1, $2, $3, $4 ) RETURNING *",
      values: [metrics, ticker, type, cik],
    };
    await db(query);
  }
}

export async function getMetrics(ticker) {
  let dataPoints = [
    "52_week_high",
    "52_week_low",
    "marketcap",
    "debttoequity",
    "pricetobook",
    "pricetoearnings",
    "netincome",
    "roic",
    "average_daily_volume",
  ];
  let metrics = {};
  for (let i in dataPoints) {
    let url = `${process.env.INTRINIO_BASE_PATH}/securities/${ticker}/data_point/${dataPoints[i]}/number?api_key=${process.env.INTRINIO_API_KEY}`;
    let data = {};
    try {
      data = await axios.get(url);
    } catch {}
    if (data && data.data) {
      metrics[dataPoints[i]] = data.data;
    }
  }

  return metrics;
}
