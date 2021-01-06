import db from "../db";
import axios from "axios";
import * as queue from "../queue";
import intrinioSDK from "intrinio-sdk";

import * as companies from "./companies";
import * as institutions from "./institutions";
import * as quodd from "./quodd";
import * as widgets from "./widgets";
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

export async function getSecuritiesByTickers(tickers) {
  let result = await db(`
        SELECT *
        FROM securities
        WHERE ticker in (${tickers})
    `);

  if (result && result.length > 0) {
    return result;
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
    let ticker = result[i].ticker;
    await queue.publish_ProcessPerformances_Securities(ticker);
    console.log(ticker);
  }

  //get and fill mutual funds
  result = await db(`
          SELECT *
          FROM mutual_funds
      `);

  console.log("MUTUAL FUNDS");
  for (let i in result) {
    let ticker = result[i].ticker;
    await queue.publish_ProcessPerformances_Securities(ticker);
    console.log(ticker);
  }

  //get and fill etfs
  result = await db(`
        SELECT *
        FROM etfs
    `);

  console.log("ETFS");
  for (let i in result) {
    let ticker = result[i].ticker;
    await queue.publish_ProcessPerformances_Securities(ticker);
    console.log(ticker);
  }
  console.log("DONE");
}

export async function insertPerformanceSecurity(
  ticker,
  perf_7_days,
  perf_14_days,
  perf_30_days,
  perf_3_months,
  perf_1_year,
  perf_values
) {
  if (!ticker) {
    return;
  }

  let query = {
    text: "SELECT * FROM securities WHERE ticker = $1",
    values: [ticker],
  };
  let result = await db(query);

  if (result.length > 0) {
    let query = {
      text:
        "UPDATE securities SET price_percent_change_7_days = $2, price_percent_change_14_days = $3, price_percent_change_30_days = $4, price_percent_change_3_months = $5, price_percent_change_1_year = $6, perf_values = $7  WHERE ticker = $1",
      values: [
        ticker,
        perf_7_days,
        perf_14_days,
        perf_30_days,
        perf_3_months,
        perf_1_year,
        perf_values,
      ],
    };
    await db(query);
  }
  // else {
  //   let query = {
  //     text:
  //       "INSERT INTO securities (json_metrics, ticker, type, cik, name ) VALUES ( $1, $2, $3, $4, $5 ) RETURNING *",
  //     values: [performance, ticker, type, cik, name],
  //   };
  //   await db(query);
  // }
}

export async function fillHoldingsCountSecurities() {
  let holdingsArr = [];
  let holdingsCount = await institutions.getInstitutionHoldingsCount();
  holdingsCount.forEach(async (value, key) => {
    holdingsArr.push({
      ticker: key,
      count: value,
    });
  });
  if (holdingsArr.length > 0) {
    for (let i in holdingsArr) {
      let ticker = holdingsArr[i].ticker;
      let count = holdingsArr[i].count;
      await insertHoldingsCountSecurity(ticker, count);
    }
  }
}

export async function insertHoldingsCountSecurity(ticker, count) {
  if (!ticker || !count) {
    return;
  }

  let query = {
    text: "SELECT * FROM securities WHERE ticker = $1",
    values: [ticker],
  };
  let result = await db(query);

  if (result.length > 0) {
    let query = {
      text:
        "UPDATE securities SET institutional_holdings_count = $2 WHERE ticker = $1",
      values: [ticker, count],
    };
    await db(query);
  }
  // else {
  //   let query = {
  //     text:
  //       "INSERT INTO securities (json_metrics, ticker, type, cik, name ) VALUES ( $1, $2, $3, $4, $5 ) RETURNING *",
  //     values: [performance, ticker, type, cik, name],
  //   };
  //   await db(query);
  // }
}

export async function getClosestPriceDate(date, dailyData) {
  for (let i in dailyData) {
    let priceDate;
    let apiDate = dailyData[i].date;
    if (typeof apiDate === "string" || apiDate instanceof String) {
      priceDate = apiDate.slice(0, 10);
    } else {
      let strDate = apiDate.toISOString();
      priceDate = strDate.slice(0, 10);
    }
    if (priceDate <= date && dailyData[i].value) {
      return dailyData[i];
    }
  }
}

export async function getSecurityPerformance(ticker) {
  let data = await getSecurityData.getChartData(securityAPI, ticker);
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
  let year = new Date(est);
  year.setDate(est.getDate() - 365);
  year = year.toISOString().slice(0, 10);
  let estTimestamp = est.toISOString();
  est = est.toISOString().slice(0, 10);

  let tPrice = await widgets.getPrice(ticker);

  let todayPrice = await getClosestPriceDate(est, dailyData);
  let weekPrice = await getClosestPriceDate(week, dailyData);
  let twoweekPrice = await getClosestPriceDate(twoweek, dailyData);
  let monthPrice = await getClosestPriceDate(month, dailyData);
  let threemonthPrice = await getClosestPriceDate(threemonth, dailyData);
  let yearPrice = await getClosestPriceDate(year, dailyData);

  if (
    todayPrice &&
    weekPrice &&
    twoweekPrice &&
    monthPrice &&
    threemonthPrice
  ) {
    let latest = yearPrice ? yearPrice : data.daily.pop();
    let earliest;

    if (tPrice) {
      earliest = {
        date: estTimestamp,
        value: tPrice,
      };
    } else {
      earliest = todayPrice;
    }

    let perf = {
      price_percent_change_7_days: (earliest.value / weekPrice.value - 1) * 100,
      price_percent_change_14_days:
        (earliest.value / twoweekPrice.value - 1) * 100,
      price_percent_change_30_days:
        (earliest.value / monthPrice.value - 1) * 100,
      price_percent_change_3_months:
        (earliest.value / threemonthPrice.value - 1) * 100,
      price_percent_change_1_year: (earliest.value / latest.value - 1) * 100,
      values: {
        today: earliest,
        week: weekPrice,
        twoweek: twoweekPrice,
        month: monthPrice,
        threemonth: threemonthPrice,
        year: latest,
      },
    };
    // add to redis
    await quodd.setPerfCache(ticker, perf);
    return perf;
  }
  return null;
}
