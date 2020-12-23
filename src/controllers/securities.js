import db from "../db";
import axios from "axios";
import * as queue from "../queue";
import intrinioSDK from "intrinio-sdk";

import * as companies from "./companies";
import * as getSecurityData from "./intrinio/get_security_data";

// init intrinio
intrinioSDK.ApiClient.instance.authentications["ApiKeyAuth"].apiKey =
  process.env.INTRINIO_API_KEY;

intrinioSDK.ApiClient.instance.basePath = `${process.env.INTRINIO_BASE_PATH}`;

const companyAPI = new intrinioSDK.CompanyApi();
const securityAPI = new intrinioSDK.SecurityApi();
const indexAPI = new intrinioSDK.IndexApi();

export async function getSecurityByTicker(ticker) {
  let result = await db(`
        SELECT *
        FROM securities
        WHERE ticker = '${ticker}'
    `);

  if (result && result.length > 0) {
    return result[0];
  }
}

export async function getSecurities() {
  let result = await db(`
        SELECT *
        FROM securities
    `);

  if (result && result.length > 0) {
    return result;
  }
}

export async function fillPerformancesSecurities() {
  //get and fill companies
  let result = await db(`
            SELECT *
            FROM companies
        `);

  console.log("COMPANIES");
  for (let i in result) {
    let type = "common_stock";
    let ticker = result[i].ticker;
    let cik = result[i].cik ? result[i].cik : "?";
    let name = result[i].json.name;
    await queue.publish_ProcessPerformances_Securities(ticker, type, cik, name);
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
    let cik = result[i].cik ? result[i].cik : "?";
    let name = result[i].json.name;
    await queue.publish_ProcessPerformances_Securities(ticker, type, cik, name);
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
    let name = result[i].json.name;
    await queue.publish_ProcessPerformances_Securities(ticker, type, "?", name);
    console.log(ticker);
  }
  console.log("DONE");
}

export async function insertPerformanceSecurity(
  performance,
  ticker,
  type,
  cik,
  name
) {
  if (!type || !ticker) {
    return;
  }

  let query = {
    text: "SELECT * FROM securities WHERE ticker = $1",
    values: [ticker],
  };
  let result = await db(query);

  if (result.length > 0) {
    if (name) {
      let query = {
        text:
          "UPDATE securities SET json_calculations = $1, name = $3 WHERE ticker = $2",
        values: [performance, ticker, name],
      };
      await db(query);
    }
  } else {
    let query = {
      text:
        "INSERT INTO securities (json_metrics, ticker, type, cik, name ) VALUES ( $1, $2, $3, $4, $5 ) RETURNING *",
      values: [performance, ticker, type, cik, name],
    };
    await db(query);
  }
}

export async function getClosestPriceDate(date, dailyData) {
  for (let i in dailyData) {
    let apiDate = dailyData[i].date.toString();
    let pricedate = apiDate.slice(0, 10);
    if (pricedate <= date && dailyData[i].value) {
      return dailyData[i];
    }
  }
}

export async function getSecurityPerformance(ticker) {
  let data = await getSecurityData.getChartData(securityAPI, ticker);
  console.log(data);
  if (!data || !data.daily || data.daily.length < 1) {
    return;
  }
  let dailyData = data.daily;

  let today = new Date();
  let est = new Date(today);
  est.setHours(est.getHours() - 5);
  let week = new Date(est);
  week.setDate(est.getDate() - 7);
  week = week.toISOString().slice(0, 10);
  let twoweek = new Date(est);
  twoweek.setDate(est.getDate() - 14);
  twoweek = twoweek.toISOString().slice(0, 10);
  let month = new Date(est);
  month.setDate(est.getDate() - 30);
  month = month.toISOString().slice(0, 10);
  let threemonth = new Date(est);
  threemonth.setDate(est.getDate() - 90);
  threemonth = threemonth.toISOString().slice(0, 10);
  est = est.toISOString().slice(0, 10);

  let todayPrice = await getClosestPriceDate(est, dailyData);
  let weekPrice = await getClosestPriceDate(week, dailyData);
  let twoweekPrice = await getClosestPriceDate(twoweek, dailyData);
  let monthPrice = await getClosestPriceDate(month, dailyData);
  let threemonthPrice = await getClosestPriceDate(threemonth, dailyData);

  if (
    todayPrice &&
    weekPrice &&
    twoweekPrice &&
    monthPrice &&
    threemonthPrice
  ) {
    let perf = {
      price_percent_change_7_days:
        (todayPrice.value / weekPrice.value - 1) * 100,
      price_percent_change_14_days:
        (todayPrice.value / twoweekPrice.value - 1) * 100,
      price_percent_change_30_days:
        (todayPrice.value / monthPrice.value - 1) * 100,
      price_percent_change_3_months:
        (todayPrice.value / threemonthPrice.value - 1) * 100,
      values: {
        today: todayPrice,
        week: weekPrice,
        twoweek: twoweekPrice,
        month: monthPrice,
        threemonth: threemonthPrice,
      },
    };
    return perf;
  }
  return null;
}
