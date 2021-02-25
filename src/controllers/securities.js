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
import asyncRedis from "async-redis";
import redis from "redis";

import { CACHED_SECURITY } from "../redis";

// init intrinio
intrinioSDK.ApiClient.instance.authentications["ApiKeyAuth"].apiKey =
  process.env.INTRINIO_API_KEY;

intrinioSDK.ApiClient.instance.basePath = `${process.env.INTRINIO_BASE_PATH}`;

let sharedCache;
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
  perf_today,
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
      text: `
        UPDATE securities 
        SET price_percent_change_7_days = $2, 
        price_percent_change_14_days = $3, 
        price_percent_change_30_days = $4, 
        price_percent_change_3_months = $5, 
        price_percent_change_1_year = $6, 
        perf_values = $7,  
        today_performance = $8
        WHERE ticker = $1
      `,
      values: [
        ticker,
        perf_7_days,
        perf_14_days,
        perf_30_days,
        perf_3_months,
        perf_1_year,
        perf_values,
        (Math.round(perf_today * 100) / 100)
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

    return (dpDate === current && dataPoint.value);
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

      return (dpDate === current && dataPoint.value);
    });

    checks += 1;
    if (checks === 14) {
      checks = 0;
    }
  }

  return dailyData[itemIndex];
}

export async function getSecurityPerformance(ticker) {
  console.log("----------------Start Performance----------------");
  console.log("Ticker: ", ticker);

  let data = await getSecurityData.getChartData(securityAPI, ticker);

  if (!data || !data.daily || data.daily.length < 1) {
    return;
  }

  let dailyData = data.daily;

  if (dailyData.length === 1 && !dailyData[0].value) {
    return;
  }

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

  let estTimestamp = moment.tz("America/New_York").format("YYYY-MM-DD");

  let intrinioResponse = await getSecurityData.getSecurityLastPrice(ticker);

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
    threemonthPrice &&
    intrinioResponse
  ) {
    let earliest;
    let latest = yearPrice ? yearPrice : data.daily.pop();

    console.log("Fetch the open price");

    let cachedOpen = await quodd.getOpenPrice(ticker);
    let lastPrice = await quodd.getLastPrice(ticker);

    if (cachedOpen) {
      cachedOpen = cachedOpen / 100;
    }
    
    console.log("cached open: ", cachedOpen);

    let open_price = cachedOpen || intrinioResponse.open_price;

    console.log("open_price: ", open_price);

    if (open_price) {
      earliest = {
        date: estTimestamp,
        value: open_price,
      };
    } else {
      earliest = todayPrice;
    }

    var todayperf = null;
    if (earliest.value) {
      todayperf = ((lastPrice.last_price || earliest.value) / earliest.value - 1) * 100;
    }

    // console.log("today: ", todayperf);
    // console.log("week: ", (earliest.value / weekPrice.value - 1) * 100);
    // console.log("2week: ", (earliest.value / twoweekPrice.value - 1) * 100);
    // console.log("1month: ", (earliest.value / monthPrice.value - 1) * 100);
    // console.log("3month: ", (earliest.value / threemonthPrice.value - 1) * 100);
    // console.log("1year: ", (earliest.value / latest.value - 1) * 100);

    let perf = {
      price_percent_change_today: todayperf,
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
  console.log("Failed to getSecurityPerformance for ticker: ", ticker);
  console.log("----------------End Performance----------------");
  return null;
}

export async function processNewTicker(ticker) {
  const sharedCache = connectSharedCache();
  const alreadyProcessed = await securityExists({
    sharedCache,
    ticker
  });

  if (alreadyProcessed) {
    console.log(`Security ${ticker} already exists`);

    return true;
  }

  const securityDetails = await fetchSecurityDetails(ticker);

  if (!securityDetails.type) {
    console.log(`unknown-type-detected-${ticker}`);

    return false;
  }

  await createNewSecurity(securityDetails);

  try {
    switch (securityDetails.type) {
      case 'common_stock': 
        await createCompany(securityDetails);
        break;
      case 'etf': 
        await createETF(securityDetails);
        break;
      case 'mutual_fund': 
        await createMutualFund(securityDetails);
        break;
      default: 
        console.log(`Stock type ${securityDetails.type} not supported`);
        break;
    }
  } catch (e) {
    console.log('Failed to create type specific record', e);
  }

  await markTickerAsProcessed({
    sharedCache, 
    ticker
  });

  console.log(`new-ticker-${ticker}-added`);

  return true;
}

const connectSharedCache = () => {
  let credentials = {
    host: process.env.REDIS_HOST_SHARED_CACHE,
    port: process.env.REDIS_PORT_SHARED_CACHE,
  };

  if (!sharedCache) {
    const client = redis.createClient(credentials);

    client.on("error", function (error) {
      //   reportError(error);
    });

    sharedCache = asyncRedis.decorate(client);
  }

  return sharedCache;
};

const securityExists = async ({sharedCache, ticker}) => {
  let exists = await sharedCache.get(`${CACHED_SECURITY}-e${ticker}`);

  if (exists) {
    return true;
  }

  return false;
}

const markTickerAsProcessed = ({sharedCache, ticker}) => {
  return sharedCache.set(`${CACHED_SECURITY}-e${ticker}`, true);
}

const fetchSecurityDetails = async (ticker) => {
  const codesMap = {
    EQS: 'common_stock',
    ETF: 'etf',
    CEF: 'mutual_fund',
  };

  const intrinioResponse = await axios.get(
    `${process.env.INTRINIO_BASE_PATH}/securities/${ticker}?api_key=${process.env.INTRINIO_API_KEY}`
  );

  const securityDetails = intrinioResponse.data;

  return {
    ticker,
    type: codesMap[securityDetails.code],
    cik: securityDetails.cik || null,
    name: securityDetails.name || null
  };
}

const createNewSecurity = (security) => {
  return db({
    text:
      "INSERT INTO securities (ticker, type, cik, name) VALUES ( $1, $2, $3, $4 ) RETURNING *",
    values: [security.ticker, security.type, security.cik, security.name],
  });
}

const createCompany = async (security) => {
  let companyDetails = null;
  let logoDetails = {};

  try {
    const intrinioResponse = await axios.get(
      `${process.env.INTRINIO_BASE_PATH}/companies/${security.ticker}?api_key=${process.env.INTRINIO_API_KEY}`
    );

    companyDetails = intrinioResponse.data;

    try {
      const brandFetchResponse = await axios.post(process.env.BRAND_FETCH_BASE_PATH, {
        domain: companyDetails.company_url
      }, {
        headers: {
          'x-api-key': process.env.BRAND_FETCH_API_KEY,
          'Content-Type': 'application/json'
        }
      });
  
      logoDetails = brandFetchResponse.data;
    } catch (e) {
      console.log(`Logo not found ${security.ticker}`);
    }
  } catch (e) {
    throw new Error(`Company Details Not Found`);
  }

  const logo = (
    (logoDetails.response && logoDetails.response.logo && logoDetails.response.logo.image) ||
    (logoDetails.response && logoDetails.response.icon && logoDetails.response.icon.image)
  );

  const logoSource = logo ? 'brandfetch' : null;

  console.log(logo);

  return db({
    text:
      `INSERT INTO companies 
        (json, ticker, updated_at, json_metrics, json_calculations, cik, json_clearbit, json_brandfetch, logo_url, logo_source) 
        VALUES ( $1, $2, now(), null, null, $3, null, $4, $5, $6 ) 
        RETURNING *
      `,
    values: [companyDetails, security.ticker, security.cik, logoDetails, (logo || null), logoSource],
  });
}

const createETF = async (security) => {
  let etfDetails = null;
  
  try {
    const intrinioResponse = await axios.get(
      `${process.env.INTRINIO_BASE_PATH}/etfs/${security.ticker}?api_key=${process.env.INTRINIO_API_KEY}`
    );

    etfDetails = intrinioResponse.data;
  } catch (e) {
    throw new Error(`ETF Details Not Found`);
  }

  return db({
    text:
      `INSERT INTO companies 
        (json, ticker, updated_at, json_stats, json_analytics) 
        VALUES ( $1, $2, now(), null, null ) 
        RETURNING *
      `,
    values: [etfDetails, security.ticker],
  });
}

const createMutualFund = async (security) => {
  return db({
    text:
      `INSERT INTO mutual_funds 
        (json, updated_at, ticker, json_summary, json_performance) 
        VALUES ( null, now(), $1, null, null ) 
        RETURNING *
      `,
    values: [security.ticker],
  });
}
