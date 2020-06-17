import axios from "axios";
import db from "../db";
import * as queue from "../queue";

import * as holdings from "./holdings";

import { orderBy } from "lodash";

export async function getTitans({ sort = [], page = 0, size = 100, ...query }) {
  return await db(`
    SELECT *
    FROM billionaires
    ORDER BY id ASC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}

export async function getBillionaires({
  sort = [],
  page = 0,
  size = 250,
  ...query
}) {
  return await db(`
    SELECT *
    FROM billionaires
    ORDER BY id ASC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}

export async function cacheCompanies_Portfolio(cik) {
  let result = await db(`
    SELECT *
    FROM holdings
    WHERE cik = '${cik}'
    ORDER BY batch_id DESC
    LIMIT 1
  `);

  if (result.length > 0) {
    let { data_url } = result[0];
    try {
      let result = await axios.get(data_url, {
        crossdomain: true,
        withCredentials: false,
        headers: {
          "Content-Type": "application/json",
          // "Access-Control-Allow-Origin": "*"
        },
      });

      let holdings = result.data;

      holdings.forEach((holding) => {
        let {
          company: { ticker },
        } = holding;
        console.log(ticker);
        queue.publish_ProcessCompanyLookup(ticker);
      });
    } catch {}
  }
}

export async function generateSummary(cik) {
  console.log(cik);

  let data = await holdings.fetchAll(cik);

  // Calculate fund performances

  // Calculate sectors
  await evaluateSectorCompositions(data);

  // Evaluate Top Stock
  await evaluateTopStocks(data);

  // Calculate portfolio performance
  await evaluateFundPerformace(cik);
}

const evaluateTopStocks = async (data) => {
  let sorted = orderBy(data, ["shares_held_percent"], ["desc"]);

  console.log(sorted.length);
};

const evaluateFundPerformace = async (cik) => {
  let data_url = `https://intrinio-zaks.s3.amazonaws.com/marketcaps/${cik}.json`;

  try {
    let result = await axios.get(data_url, {
      crossdomain: true,
      withCredentials: false,
      headers: {
        "Content-Type": "application/json",
        // "Access-Control-Allow-Origin": "*"
      },
    });

    let marketcaps = result.data;
    // console.log(marketcaps);

    const findChange = (values, offset) => {
      try {
        let current = {};
        let previous = {};
        let index_current = -1;
        let index_previous = -1;

        for (let i = 0; i < values.length; i += 1) {
          let item = values[i];

          if (item["value"]) {
            index_current = i;
            index_previous = i + offset;
            break;
          }
        }

        current = values[index_current];
        previous = values[index_previous];

        let delta = current["value"] - previous["value"];

        // % increase = Increase ÷ Original Number × 100.

        let percent_change = (delta / current["value"]) * 100;

        return percent_change;
      } catch (e) {
        // console.error(e);
        return null;
      }
    };

    let change = 0;
    let quarterly = marketcaps["quarterly"];

    change = findChange(quarterly, 1);

    console.log("1 quarter", change);
    let yearly = marketcaps["yearly"];

    change = findChange(yearly, 1);
    console.log("1 year", change);

    change = findChange(yearly, 5);
    console.log("5 year", change);
  } catch (e) {
    console.error(e);
  }
};

const evaluateSectorCompositions = async (data) => {
  // console.log(data);

  let tickers = data.map(({ company }) => company["ticker"]);

  // console.log(tickers);

  let query = {
    text: "SELECT * FROM companies WHERE ticker = ANY($1::text[])",
    values: [tickers],
  };

  let result = await db(query);

  // console.log(result);

  const mergeById = (a1, a2) =>
    a1.map((i1) => ({
      ...a2.find((i2) => i2.company.ticker === i1.ticker && i2),
      ...i1,
    }));

  let merged = mergeById(result, data);

  // console.log(merged);

  // //

  let sectors = merged.map(({ json }) => json["sector"]);
  // console.log(sectors);

  let buffer = {};
  let total = 0;

  for (let i = 0; i < merged.length; i += 1) {
    // if (counts.hasOwnProperty(key)) {
    //   counts[key] = (counts[key] / total) * 100;
    // }
    let sector = merged[i]["json"]["sector"];
    let market_value = merged[i]["market_value"];

    buffer[`${sector}`] = buffer[`${sector}`]
      ? buffer[`${sector}`] + market_value
      : market_value;

    total += market_value;
  }

  // console.log(buffer);
  // console.log(total);

  for (let key in buffer) {
    if (buffer.hasOwnProperty(key)) {
      buffer[key] = (buffer[key] / total) * 100;
      // buffer[key] = buffer[key].toFixed(2);
    }
  }

  console.log(buffer);

  return buffer;
};
