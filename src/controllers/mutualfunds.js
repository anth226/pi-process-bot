import "dotenv/config";
import axios from "axios";
import db from "../db";

import intrinioSDK from "intrinio-sdk";

import * as titans from "./titans";
import * as getSecurityData from "./intrinio/get_security_data";

import * as queue from "../queue";
// import * as queue from "../queue2";

// init intrinio
intrinioSDK.ApiClient.instance.authentications["ApiKeyAuth"].apiKey =
  process.env.INTRINIO_API_KEY;

intrinioSDK.ApiClient.instance.basePath = `${process.env.INTRINIO_BASE_PATH}`;

const companyAPI = new intrinioSDK.CompanyApi();
const securityAPI = new intrinioSDK.SecurityApi();
const indexAPI = new intrinioSDK.IndexApi();

export async function getMutualFundByTicker(ticker) {
  let result = await db(`
        SELECT *
        FROM mutual_funds
        WHERE ticker = '${ticker}'
    `);

  if (result && result.length > 0) {
    return result[0];
  }
}

export async function getJsonMutualFunds() {
  let funds = [];

  try {
    let url = `https://fds1.cannonvalleyresearch.com/api/v1/report/dailySummary.json?apiKey=${process.env.CANNON_API_KEY}`;
    const result = await axios.get(url);
    funds = result.data;
  } catch (e) {
    console.error(e);
  }
  return funds;
}

export async function getJsonSumMutualFund(fundId) {
  let fundSums = [];
  let fundSum;

  try {
    let url = `https://fds1.cannonvalleyresearch.com/api/v1/portSummary/${fundId}/?apiKey=${process.env.CANNON_API_KEY}`;
    const result = await axios.get(url);
    fundSums = result.data;
    fundSum = fundSums[fundSums.length - 1];
  } catch (e) {
    console.error(e);
  }
  return fundSum;
}

export async function getJsonPerformanceMutualFund(ticker) {
  let data = await getSecurityData.getChartData(securityAPI, ticker);
  if (data.daily[0].value && data.daily[29].value && data.weekly[52].value) {
    let perf = {
      price_percent_change_30_days:
        1 - data.daily[0].value / data.daily[29].value,
      price_percent_change_1_year:
        1 - data.daily[0].value / data.weekly[52].value,
    };
    return perf;
  }
}

export async function updateJson_MutualFunds() {
  let result = await getJsonMutualFunds();

  let records = result;

  if (records.length > 0) {
    for (let i = 0; i < records.length; i += 1) {
      let json = JSON.stringify(records[i]);
      let ticker = records[i].ticker;
      let fundId = records[i].fundId;
      if (fundId && json && ticker) {
        await queue.publish_ProcessJsonMutualFunds(json, fundId, ticker);
      }
    }
  }
}

export async function insertJsonMutualFund(key, json, ticker) {
  if (!key || !json || !ticker) {
    return;
  }

  let query = {
    text: "SELECT * FROM mutual_funds WHERE ticker = $1",
    values: [ticker],
  };
  let result = await db(query);

  if (result.length > 0) {
    let query = {
      text: `UPDATE mutual_funds SET ${key} = $1, updated_at = now() WHERE ticker = $2`,
      values: [json, ticker],
    };
    await db(query);
  } else {
    let query = {
      text: `INSERT INTO mutual_funds (${key}, updated_at, ticker ) VALUES ( $1, now(), $2 ) RETURNING *`,
      values: [json, ticker],
    };
    await db(query);
  }
}
