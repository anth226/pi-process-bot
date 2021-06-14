import axios from "axios";
import cheerio from "cheerio";

import db from "../db";

import * as queue from "../queue";
import intrinioSDK from "intrinio-sdk";
import * as institutions from "./institutions";
import * as quodd from "./quodd";
import * as getSecurityData from "./intrinio/get_security_data";
import moment from "moment";

import { getEnv } from "../env";
import {
  CACHED_SECURITY,
  CACHED_DAY,
  CACHED_NOW,
  CACHED_THEN,
  CACHED_PERF,
  connectPriceCache,
} from '../redis';
import { log } from "winston";

// init intrinio
intrinioSDK.ApiClient.instance.authentications["ApiKeyAuth"].apiKey = getEnv("INTRINIO_API_KEY");

intrinioSDK.ApiClient.instance.basePath = getEnv("INTRINIO_BASE_PATH");

const securityAPI = new intrinioSDK.SecurityApi();

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

export async function getHighestPerformingSecurities() {
  let result = await db(`
      SELECT *
      FROM securities
      WHERE delisted = FALSE
      AND today_performance > 0
      ORDER BY today_performance DESC
    `);

  if (result && result.length > 0) {
    return result;
  }
}

export async function getStrongBuys() {
  let result = await db(`
        SELECT *
        FROM strong_buys
    `);

  if (result && result.length > 0) {
    return result;
  }
}

export async function setStrongBuys(ticks) {
  //let buys = await widgets.getStrongBuys(ticks);

  if (ticks && ticks.length > 0) {
    for (let t in ticks) {
      let ticker = ticks[t];
      let security = await getSecurityByTicker(ticker);
      if (security) {
        let query = {
          text:
            "UPDATE strong_buys SET ticker = $2, security_id = $3 WHERE position = $1",
          values: [t, ticker, security.id],
        };
        await db(query);
      }
    }
  }
}

export async function fillPerformancesSecurities() {
  //get and fill securities
  let result = await db(`
    SELECT ticker
    FROM securities
    WHERE delisted = FALSE
  `);

  for (let i in result) {
    let ticker = result[i].ticker;
    await queue.publish_ProcessPerformances_Securities(ticker);
    console.log(ticker);
  }

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

export async function getDaily(ticker) {
  let data = await getSecurityData.getChartData(securityAPI, ticker);

  return data
}

export async function getClosestPriceDate(date, dailyData) {
  let differenceBefore, differenceAfter, differenceBeforeIndex;

  date = date + 'T00:00:00.000Z'; // Match format of intrinio dates

  // data from intrinio is DESC date
  for (let i = 0; i < dailyData.length; i++) {
    if (!dailyData[i].value) {
      continue;
    }
    if (moment(dailyData[i]?.date).isSameOrBefore(date)) {
      differenceBefore = moment(date).diff(
        moment(dailyData[i]?.date),
        "days"
      );
      if (dailyData[i - 1] && dailyData[i - 1].value) {
        differenceAfter = moment(date).diff(
          moment(dailyData[i - 1]?.date),
          "days"
        );
      }
      differenceBeforeIndex = i;
      break;
    }
  }

  let index = differenceBeforeIndex;

  if ((differenceAfter || differenceAfter === 0) && differenceBefore) {
    if (Math.abs(differenceAfter) < differenceBefore) {
      index = differenceBeforeIndex - 1
    }
  }

  if (index || index === 0) {
    return dailyData[index];
  }

  return dailyData[dailyData.length - 1]
}

export async function getSecurityPerformance(ticker) {
  console.log("----------------Start Performance----------------");
  console.log("Ticker: ", ticker);

  let data = await getSecurityData.getChartData(securityAPI, ticker);

  if (!data || !data.daily || data.daily.length < 1) {
    console.log("No daily data");
    console.log("----------------END Performance----------------");
    return;
  }

  let dailyData = data.daily;

  if (dailyData.length === 1 && !dailyData[0].value) {
    console.log("Null first indicie");
    console.log("----------------END Performance----------------");
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

  let todayPrice = await getClosestPriceDate(est, dailyData);
  let weekPrice = await getClosestPriceDate(week, dailyData);
  let twoweekPrice = await getClosestPriceDate(twoweek, dailyData);
  let monthPrice = await getClosestPriceDate(month, dailyData);
  let threemonthPrice = await getClosestPriceDate(threemonth, dailyData);
  let yearPrice = await getClosestPriceDate(year, dailyData);

  let earliest;

  console.log("Fetch the open price");

  let open_price = await quodd.getOpenPrice(ticker);
  let lastPrice = await quodd.getLastPrice(ticker);

  console.log("cached open: ", open_price);

  if (!open_price) {
    let intrinioResponse = await getSecurityData.getSecurityLastPrice(ticker);
    if (intrinioResponse) {
      open_price = intrinioResponse.open_price;
    }
  }

  console.log("open_price: ", open_price);
  console.log("lastPrice: ", lastPrice);

  // console.log("today: ", todayPrice);
  // console.log("week: ", weekPrice);
  // console.log("two week: ", twoweekPrice);
  // console.log("month: ", monthPrice);
  // console.log("three month: ", threemonthPrice);
  // console.log("year: ", yearPrice);

  if (open_price) {
    console.log(estTimestamp);
    console.log(open_price);

    earliest = {
      date: estTimestamp,
      value: open_price,
    };
  } else {
    earliest = todayPrice;
  }

  var todayperf = null;
  if (earliest?.value && lastPrice?.last_price) {
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
    price_percent_change_1_year: (earliest.value / yearPrice.value - 1) * 100,
    values: {
      today: earliest,
      week: weekPrice,
      twoweek: twoweekPrice,
      month: monthPrice,
      threemonth: threemonthPrice,
      year: yearPrice,
    },
  };
  // add to redis

  await quodd.setPerfCache(ticker, perf);
  console.log("----------------End Performance----------------");
  return perf;
}

export async function updateSecuritiesDelistStatus() {
  try {
    console.log(`Starting Delist Check!!!`);

    const securities = (await db(`
      SELECT ticker, delisted
      FROM securities
    `)).reduce((securities, security) => {
      securities[security.ticker] = security.delisted;

      return securities;
    }, {});

    const sharedCache = connectPriceCache();
    const todaysDate = moment().format('YYYY-MM-DD');
    const keys = await sharedCache.keys(`${CACHED_SECURITY}*`);
    let activatedSecurities = [];
    let delistedSecurities = [];

    console.log(`${keys.length} securities to be checked for delist status`);

    for await (let cachedKey of keys) {
      const ticker = cachedKey.split(':').pop().substring(1);
      const lastSpinDate = await sharedCache.get(cachedKey);
      let delisted = false;

      if (lastSpinDate && lastSpinDate !== todaysDate) {
        delisted = true;
      }

      if (securities[ticker] === delisted) {
        continue;
      }

      if (delisted) {

        try {
          await Promise.all([
            sharedCache.del(`${CACHED_DAY}${ticker}`),
            sharedCache.del(`${CACHED_NOW}${ticker}`),
            sharedCache.del(`${CACHED_THEN}${ticker}`),
            sharedCache.del(`${CACHED_PERF}${ticker}`),
          ]);
        } finally {
          delistedSecurities.push(ticker);
        }
      } else {
        activatedSecurities.push(ticker)
      }
    }

    if (delistedSecurities.length) {
      const delistedSecuritiesChunks = getArrayChunks(delistedSecurities, 200);

      for await (let delistedSecuritiesChunk of delistedSecuritiesChunks) {
        await Promise.all([
          db(`
            UPDATE securities
            SET delisted = true WHERE ticker IN ('${delistedSecuritiesChunk.join("','")}')
          `),
          db(`
            DELETE FROM portfolio_histories
            WHERE ticker IN ('${delistedSecuritiesChunk.join("','")}')
          `)
        ])
      }
    }

    if (activatedSecurities.length) {
      const activatedSecuritiesChunks = getArrayChunks(activatedSecurities, 200);

      for await (let activatedSecuritiesChunk of activatedSecuritiesChunks) {
        await db(`
          UPDATE securities
          SET delisted = false WHERE ticker IN ('${activatedSecuritiesChunk.join("','")}')
        `);
      }
    }

    console.log(`${delistedSecurities.length} securities delisted!!!`);
    console.log(`${activatedSecurities.length} securities activated!!!`);
    console.log(`Delist status update completed!!!`);
  } catch (e) {
    console.log(e);
  }
}

export async function processNewTicker(ticker) {
  const securityDetails = await fetchSecurityDetails(ticker);

  if (!securityDetails.type) {
    console.log(`unknown-type-detected-${ticker}`);

    return false;
  }

  const created = await createNewSecurity(securityDetails);

  if (!created) {
    console.log(`Security entry for ${ticker} already exists`);

    return false;
  }

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

  const priceCache = connectPriceCache();
  const date = moment().format('YYYY-MM-DD');

  priceCache.set(`${CACHED_SECURITY}${ticker}`, date);
  console.log(`new-ticker-${ticker}-added`);

  return true;
}

const fetchSecurityDetails = async (ticker) => {
  const codesMap = {
    EQS: 'common_stock',
    ETF: 'etf',
    CEF: 'mutual_fund',
  };

  let response = {
    ticker,
    type: 'common_stock',
    cik: null,
    name: ''
  };

  try {
    const intrinioResponse = await axios.get(
      `${getEnv("INTRINIO_BASE_PATH")}/securities/${ticker}?api_key=${getEnv("INTRINIO_API_KEY")}`
    );

    const securityDetails = intrinioResponse.data;

    response.type = codesMap[securityDetails.code] || 'common_stock';
    response.cik = securityDetails.cik || null;
    response.name = securityDetails.name || '';
  } catch (e) {
    response.name = await getSecurityName(ticker.toLowerCase());
  }

  return response;
}

const getSecurityName = async (ticker) => {

  try {
    const response = await axios.get(`https://www.marketwatch.com/investing/stock/${ticker}`);
    const html = response.data;
    const domReference = cheerio.load(html);

    return (domReference('.company__name').text() || '').trim();
  } catch (e) {
    console.log(e);
    return '';
  }
}

const createNewSecurity = async (security) => {
  let result = await db({
    text: "SELECT * FROM securities WHERE ticker = $1",
    values: [security.ticker],
  });

  if (result && result.length > 0) {
    return false;
  }

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
      `${getEnv("INTRINIO_BASE_PATH")}/companies/${security.ticker}?api_key=${getEnv("INTRINIO_API_KEY")}`
    );

    companyDetails = intrinioResponse.data;

    try {
      const brandFetchResponse = await axios.post(getEnv("BRAND_FETCH_BASE_PATH"), {
        domain: companyDetails.company_url
      }, {
        headers: {
          'x-api-key': getEnv("BRAND_FETCH_API_KEY"),
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

  if (!security.name && companyDetails.name) {
    await db({
      text:
        "UPDATE securities SET name = $1 WHERE ticker = $2",
      values: [companyDetails.name, security.ticker],
    });
  }

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
      `${getEnv("INTRINIO_BASE_PATH")}/etfs/${security.ticker}?api_key=${getEnv("INTRINIO_API_KEY")}`
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
};

const getArrayChunks = (arr, chunkSize) => {
  var chunks = [];

  for (var i = 0, len = arr.length; i < len; i += chunkSize) {
    chunks.push(arr.slice(i, i + chunkSize));
  }

  return chunks;
}


export const fetchCachedSecuritiesFromSharedCache = async (res) => {
  let sharedCache = connectPriceCache();
  const keys = await sharedCache.keys('C_SEC:*');

  return res.send(keys);
}

export const clearCachedSecuritiesFromSharedCache = async (res) => {
  let sharedCache = connectPriceCache();
  const keys = await sharedCache.keys('C_SEC:*');

  for (let i = 0; i < keys.length; i++) {
    await sharedCache.del(keys[i]);
  }

  return res.send(keys);
}

export const syncExistingSecuritiesWithRedis = async (ticker, res) => {
  try {
    let sharedCache = connectPriceCache();

    let securities = await db(`
      SELECT ticker
      FROM securities
      ${ticker && `WHERE ticker = '${ticker}'`}
    `);

    res.write(securities.length.toString());

    for (let i = 0; i < securities.length; i++) {
      res.write(i.toString());
      await sharedCache.set(`C_SEC:${securities[i].ticker}`, 'true');
    }

    res.write('done');
    return 'done';
  } catch (e) {
    res.write('error');
    res.write(e.message);

    return 'error';
  }
};

export const populateSecuritiesNames = async () => {
  try {
    let securities = await db(`
      SELECT ticker
      FROM securities
      WHERE name IS NULL OR name = ''
    `);

    for (let index = 0; index < securities.length; index++) {
      try {
        const ticker = securities[index].ticker;
        const name = await getSecurityName(ticker.toLowerCase());

        if (!name) {
          continue;
        }

        await db({
          text: "UPDATE securities SET name = $1 WHERE ticker = $2",
          values: [name, ticker],
        });
      } catch (e) {
        console.log(e);
      }
    }

    return true;
  } catch (e) {
    console.log(e);
    return false;
  }
}

export const filterSecurityNames = async (data) => {
  let tickers = data.map(s => s.ticker).join(",");

  const securities = await db(`
                SELECT ticker, name FROM securities WHERE ticker = ANY('{${tickers}}')
                `);

  const securityMap = securities.reduce((s, security) => ({ ...s, [security.ticker]: security.name }), {});

  // filter by securities we have
  let results = data.map(s => {
    s.name = securityMap[s.ticker];
    return s;
  }).filter(s => s.name);

  return results;
}
