import axios from "axios";
import cheerio from "cheerio";

const chalk = require("chalk");

import db from "../db";

import * as queue from "../queue";
import * as titans from "./titans";
// import * as queue from "../queue2";

async function lookup(identifier) {
  let url = `${process.env.INTRINIO_BASE_PATH}/companies/${identifier}?api_key=${process.env.INTRINIO_API_KEY}`;

  let data = {};
  try {
    data = await axios.get(url);
  } catch {}
  return data;
}

export async function lookupCompany(identifier) {
  let result = await lookup(identifier);

  let { status, data } = result;
  if (status && status === 200) {
    console.log(data.ticker);

    result = await db(`
        SELECT *
        FROM companies
        WHERE ticker = '${identifier}'
    `);

    if (result && result.length == 0) {
      let query = {
        text:
          "INSERT INTO companies (json, ticker, updated_at) VALUES ( $1, $2, now() ) RETURNING *",
        values: [data, identifier],
      };
      result = await db(query);
    }
  } else {
    console.log(chalk.bgRed("error"), identifier);
  }
}

export async function updateJson_InsiderCompanies() {
  const response = await axios.get(`${process.env.PROD_API_URL}/all-insider`);
  if (response.status === 200 && response.data.length > 0) {
    let data = response.data;
    for (let item of data) {
      let ticker = item[0];
      queue.publish_ProcessCompanyLookup(ticker);
    }
  }
}

export async function getCompanies() {
  let result = await db(`
        SELECT *
        FROM companies
    `);

  if (result && result.length > 0) {
    return result;
  }
}

export async function getCompanyByTicker(ticker) {
  let result = await db(`
        SELECT *
        FROM companies
        WHERE ticker = '${ticker}'
    `);

  if (result && result.length > 0) {
    return result[0];
  }
}

export async function getCompanyByCIK(cik) {
  let result = await db(`
        SELECT *
        FROM companies
        WHERE cik = '${cik}'
    `);

  if (result && result.length > 0) {
    return result[0];
  }
}

const companyPage = "https://finviz.com/quote.ashx?t=";

const fetchDataCompany = async (ticker) => {
  try {
    const result = await axios.get(companyPage + ticker);

    return cheerio.load(result.data);
  } catch (error) {
    return null;
  }
};

export async function getCompanyMetrics(ticker) {
  ticker = ticker.replace(".", "-");

  let data = [];
  try {
    const $ = await fetchDataCompany(ticker);

    let res = {};

    $("table.snapshot-table2 tr.table-dark-row").each(function (idx, element) {
      let keys = [];
      let vals = [];

      $(element)
        .find("td")
        .each(function (i, e) {
          if (i % 2 == 0) {
            keys.push($(e).text());
          } else {
            vals.push($(e).text());
          }
        });

      for (let i = 0; i < keys.length; i++) {
        res[keys[i]] = vals[i];
      }
    });

    delete res["Price"];
    delete res["Employees"];

    //console.log(res);
    return res;
  } catch (error) {
    return null;
  }
}

export async function updateMetrics_Companies() {
  let companies = await getCompanies();
  for (let c in companies) {
    let ticker = companies[c].ticker;
    let cik = companies[c].cik;
    let metrics = await getCompanyMetrics(ticker);
    metrics = JSON.stringify(metrics);

    //update json_metrics
    if (metrics && ticker) {
      await queue.publish_ProcessMetricsCompanies(ticker, metrics);
    }

    //update json_calculations
    if (ticker && cik) {
      let perf = await titans.calculateFallbackPerformance_Billionaire(cik);
      if (perf.performance_quarter) {
        let query = {
          text:
            "UPDATE companies SET json_calculations=($1) WHERE cik=($2) RETURNING *",
          values: [perf, cik.cik],
        };
        await db(query);
      }
    }
  }
}

export async function insertMetricsCompany(ticker, metrics) {
  if (!ticker || !metrics) {
    return;
  }

  let query = {
    text:
      "UPDATE companies SET json_metrics = $2, updated_at = now() WHERE ticker = $1",
    values: [ticker, metrics],
  };
  await db(query);
}
