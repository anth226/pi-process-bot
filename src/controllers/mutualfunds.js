import "dotenv/config";
import axios from "axios";
import db from "../db";

import * as titans from "./titans";

import * as queue from "../queue";
// import * as queue from "../queue2";

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

export async function insertJsonMutualFund(json, jsonSum, ticker) {
  if (!json || !jsonSum || !ticker) {
    return;
  }

  let query = {
    text: "SELECT * FROM mutual_funds WHERE ticker = $1",
    values: [ticker],
  };
  let result = await db(query);

  if (result.length > 0) {
    let query = {
      text:
        "UPDATE mutual_funds SET json = $1, json_summary = $2, updated_at = now() WHERE ticker = $3",
      values: [json, jsonSum, ticker],
    };
    await db(query);
  } else {
    let query = {
      text:
        "INSERT INTO mutual_funds (json, json_summary, updated_at, ticker ) VALUES ( $1, $2, now(), $3 ) RETURNING *",
      values: [json, jsonSum, ticker],
    };
    await db(query);
  }
}
