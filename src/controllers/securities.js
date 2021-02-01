import db from "../db";
import axios from "axios";
import * as queue from "../queue";
import intrinioSDK from "intrinio-sdk";

import * as companies from "./companies";
import * as institutions from "./institutions";
import * as quodd from "./quodd";
import * as widgets from "./widgets";
import * as getSecurityData from "./intrinio/get_security_data";
import moment from "moment";
import MTZ from "moment-timezone";

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
  } else {
    console.log("Could not find security for ticker: ", ticker);
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
  let current = date;
  let forward = current;
  let reverse = current;
  let checks = 0;

  let itemIndex = dailyData.findIndex((dataPoint) => {
    let dpDate = dataPoint.date;

    if (typeof dpDate === "string" || dpDate instanceof String) {
      dpDate = dpDate.slice(0, 10);
    } else {
      let strDate = dpDate.toISOString();
      dpDate = strDate.slice(0, 10);
    }

    return dpDate === current;
  });

  while (itemIndex < 0) {
    if (checks >= 0 && checks < 7) {
      forward = moment(forward).add(1, "days").format("YYYY-MM-DD");
      current = forward;
    } else {
      reverse = moment(reverse).subtract(1, "days").format("YYYY-MM-DD");
      current = reverse;
    }

    itemIndex = dailyData.findIndex((dataPoint) => {
      let dpDate = dataPoint.date;

      if (typeof dpDate === "string" || dpDate instanceof String) {
        dpDate = dpDate.slice(0, 10);
      } else {
        let strDate = dpDate.toISOString();
        dpDate = strDate.slice(0, 10);
      }

      return dpDate === current;
    });

    checks += 1;
    if (checks === 14) {
      checks = 0;
    }
  }

  return dailyData[itemIndex];
}

export async function getSecurityPerformance(ticker) {
  let data = await getSecurityData.getChartData(securityAPI, ticker);

  if (!data || !data.daily || data.daily.length < 1) {
    return;
  }

  let dailyData = data.daily;

  let est = moment.tz("America/New_York").format("YYYY-MM-DD");

  let week = moment
    .tz("America/New_York")
    .subtract(7, "days")
    .format("YYYY-MM-DD");

  let twoweek = moment
    .tz("America/New_York")
    .subtract(14, "days")
    .format("YYYY-MM-DD");

  let month = moment
    .tz("America/New_York")
    .subtract(1, "months")
    .format("YYYY-MM-DD");

  let threemonth = moment
    .tz("America/New_York")
    .subtract(3, "months")
    .format("YYYY-MM-DD");

  let year = moment
    .tz("America/New_York")
    .subtract(1, "years")
    .format("YYYY-MM-DD");

  // console.log("est", est);
  // console.log("week", week);
  // console.log("twoweek", twoweek);
  // console.log("month", month);
  // console.log("threemonth", threemonth);
  // console.log("year", year);

  let estTimestamp = moment.tz("America/New_York").format("YYYY-MM-DD");

  let intrinioResponse = await getSecurityData.getSecurityLastPrice(ticker);

  let todayPrice = await getClosestPriceDate(est, dailyData);
  let weekPrice = await getClosestPriceDate(week, dailyData);
  let twoweekPrice = await getClosestPriceDate(twoweek, dailyData);
  let monthPrice = await getClosestPriceDate(month, dailyData);
  let threemonthPrice = await getClosestPriceDate(threemonth, dailyData);
  let yearPrice = await getClosestPriceDate(year, dailyData);

  // console.log("todayPrice", todayPrice);
  // console.log("weekPrice", weekPrice);
  // console.log("twoweekPrice", twoweekPrice);
  // console.log("monthPrice", monthPrice);
  // console.log("threemonthPrice", threemonthPrice);
  // console.log("yearPrice", yearPrice);

  if (
    todayPrice &&
    weekPrice &&
    twoweekPrice &&
    monthPrice &&
    threemonthPrice &&
    intrinioResponse
  ) {
    let earliest;
    let latest = yearPrice ? yearPrice : data.daily.pop();
    let open_price = quodd.getOpenPrice(ticker) || intrinioResponse.open_price;

    if (open_price) {
      earliest = {
        date: estTimestamp,
        value: open_price,
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
