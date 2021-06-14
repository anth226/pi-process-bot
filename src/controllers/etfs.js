import "dotenv/config";
import axios from "axios";
import db from "../db";
import {getEnv} from "../env";

import * as queue from "../queue";
// import * as queue from "../queue2";

export async function getETFByTicker(ticker) {
  let result = await db(`
        SELECT *
        FROM etfs
        WHERE ticker = '${ticker}'
    `);

  if (result && result.length > 0) {
    return result[0];
  }
}

export async function getDBETFs() {
  let result = await db(`
          SELECT *
          FROM etfs
      `);

  if (result && result.length > 0) {
    return result;
  }
}

export async function getAllETFs() {
  let next_page = null;
  let etfs = [];
  let buffer = {};
  do {
    let response = await getETFs(next_page);
    next_page = response["next_page"];

    buffer = response["etfs"];
    etfs = etfs.concat(buffer);
  } while (next_page);
  return etfs;
}

export async function getETFs(next_page = null) {
  let url = `${getEnv("INTRINIO_BASE_PATH")}/etfs?api_key=${getEnv("INTRINIO_API_KEY")}`;

  if (next_page) {
    url = `${getEnv("INTRINIO_BASE_PATH")}/etfs?next_page=${next_page}&api_key=${getEnv("INTRINIO_API_KEY")}`;
  }

  let data = axios
    .get(url)
    .then(function (res) {
      return res.data;
    })
    .catch(function (err) {
      console.log("error");
      return {};
    });

  return data;
}

export async function getJsonETF(ticker) {
  let data;
  try {
    let url = `${getEnv("INTRINIO_BASE_PATH")}/etfs/${ticker}?api_key=${getEnv("INTRINIO_API_KEY")}`;
    const result = await axios.get(url);
    data = result.data;
  } catch (e) {
    console.error(e);
  }
  return data;
}

export async function getStatsETF(ticker) {
  let data;
  try {
    let url = `${getEnv("INTRINIO_BASE_PATH")}/etfs/${ticker}/stats?api_key=${getEnv("INTRINIO_API_KEY")}`;
    const result = await axios.get(url);
    data = result.data;
  } catch (e) {
    console.error(e);
  }
  return data;
}

export async function getAnalyticsETF(ticker) {
  let data;
  try {
    let url = `${getEnv("INTRINIO_BASE_PATH")}/etfs/${ticker}/analytics?api_key=${getEnv("INTRINIO_API_KEY")}`;
    const result = await axios.get(url);
    data = result.data;
  } catch (e) {
    console.error(e);
  }
  return data;
}

export async function updateJson_ETFs() {
  let result = await getAllETFs();

  let records = result;

  if (records.length > 0) {
    for (let i = 0; i < records.length; i += 1) {
      let ticker = records[i].ticker;
      if (ticker) {
        await queue.publish_ProcessJsonETFs(ticker);
      }
    }
  }
}

export async function insertJsonETF(json, stats, analytics, ticker) {
  if (!json || !ticker) {
    return;
  }

  let query = {
    text: "SELECT * FROM etfs WHERE ticker = $1",
    values: [ticker],
  };
  let result = await db(query);

  if (result.length > 0) {
    let query = {
      text:
        "UPDATE etfs SET json = $1, updated_at = now(), json_stats = $2, json_analytics = $3 WHERE ticker = $4",
      values: [json, stats, analytics, ticker],
    };
    await db(query);
  } else {
    let query = {
      text:
        "INSERT INTO etfs (json, updated_at, json_stats, json_analytics, ticker ) VALUES ( $1, now(), $2, $3, $4 ) RETURNING *",
      values: [json, stats, analytics, ticker],
    };
    await db(query);
  }
}
