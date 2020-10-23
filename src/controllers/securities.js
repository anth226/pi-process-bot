import db from "../db";
import * as queue from "../queue";

import * as companies from "./companies";

export async function fillSecurities() {
  //get and fill companies
  let result = await db(`
            SELECT *
            FROM companies
        `);

  for (let i in result) {
    let type = "common_stock";
    let ticker = result[i].ticker;
    let cik = result[i].cik;
    let metrics = await companies.getCompanyMetrics(ticker);
    await insertSecurity(metrics, ticker, type, cik);
    console.log(ticker);
  }

  //get and fill mutual funds
  result = await db(`
          SELECT *
          FROM mutual_funds
      `);

  for (let i in result) {
    let type = "mutual_fund";
    let ticker = result[i].ticker;
    let cik = result[i].json.cik;
    let metrics = await companies.getCompanyMetrics(ticker);
    await insertSecurity(metrics, ticker, type, cik);
    console.log(ticker);
  }

  //get and fill etfs
  result = await db(`
        SELECT *
        FROM etfs
    `);

  for (let i in result) {
    let type = "etf";
    let ticker = result[i].ticker;
    let metrics = await companies.getCompanyMetrics(ticker);
    await insertSecurity(metrics, ticker, type, null);
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
