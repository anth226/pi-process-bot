import db from "../db";
import axios from "axios";
import cheerio from "cheerio";
import intrinioSDK from "intrinio-sdk";
import * as getSecurityData from "./intrinio/get_security_data";
import * as queue from "../queue";
import * as companies from "./companies";
import * as institutions from "./institutions";
import * as mutualfunds from "./mutualfunds";
import * as etfs from "./etfs";
import * as securities from "./securities";
import {getEnv} from "../env";

// init intrinio
intrinioSDK.ApiClient.instance.authentications["ApiKeyAuth"].apiKey =
  getEnv("INTRINIO_API_KEY");

intrinioSDK.ApiClient.instance.basePath = getEnv("INTRINIO_BASE_PATH");

const companyAPI = new intrinioSDK.CompanyApi();
const securityAPI = new intrinioSDK.SecurityApi();
const indexAPI = new intrinioSDK.IndexApi();

export async function getDailyEarnings() {
  try {
    let today = new Date();
    let est = new Date(today);
    est.setHours(est.getHours() - 5);
    est.toISOString().slice(0, 10);

    let url = `${getEnv("INTRINIO_BASE_PATH")}/zacks/eps_surprises?start_date=${est}&end_date=${est}&api_key=${getEnv("INTRINIO_API_KEY")}`;

    let response = await axios.get(url);
    let data = response.data;
    let eps_surprises = data.eps_surprises;

    if (data && eps_surprises.length > 0) {
      return eps_surprises;
    }
  } catch (e) {
    console.error(e);
  }
}

export async function getEarningsReports() {
  let result = await db(`
      SELECT *
      FROM earnings_reports
    `);

  return result;
}

export async function getFutureEarningsDates() {
  let today = new Date(); //.toISOString().slice(0, 10);
  let est = new Date(today);
  est.setHours(est.getHours() - 5);
  let yesterday = new Date(est);
  yesterday.setDate(yesterday.getDate() - 1);

  let url = `${getEnv("INTRINIO_BASE_PATH")}/securities/screen?order_column=next_earnings_date&order_direction=asc&page_size=10000&api_key=${getEnv("INTRINIO_API_KEY")}`;
  const body = {
    operator: "AND",
    clauses: [
      {
        field: "next_earnings_date",
        operator: "gt",
        value: yesterday,
      },
      {
        field: "next_earnings_time_of_day",
        operator: "gt",
        value: "0",
      },
      {
        field: "next_earnings_year",
        operator: "gt",
        value: "0",
      },
      {
        field: "next_earnings_quarter",
        operator: "gt",
        value: "0",
      },
    ],
  };

  let res = axios
    .post(url, body)
    .then(function (data) {
      //console.log(data);
      return data;
    })
    .catch(function (err) {
      console.log(err);
      return err;
    });

  return res.then((data) => data.data);
}

export async function getPastEarningsDates() {
  let today = new Date(); //.toISOString().slice(0, 10);
  let est = new Date(today);
  est.setHours(est.getHours() - 5);

  let url = `${getEnv("INTRINIO_BASE_PATH")}/securities/screen?order_column=next_earnings_date&order_direction=desc&page_size=10000&api_key=${getEnv("INTRINIO_API_KEY")}`;
  const body = {
    operator: "AND",
    clauses: [
      {
        field: "next_earnings_date",
        operator: "lt",
        value: est,
      },
      {
        field: "next_earnings_time_of_day",
        operator: "gt",
        value: "0",
      },
    ],
  };

  let res = axios
    .post(url, body)
    .then(function (data) {
      //console.log(data);
      return data;
    })
    .catch(function (err) {
      console.log(err);
      return err;
    });

  return res.then((data) => data.data);
}

export async function fillEarnings() {
  let data = await getFutureEarningsDates();

  for (let i in data) {
    let ticker = data[i].security.ticker;
    let name = data[i].security.name;
    let earningsDate = data[i].data[0].text_value;
    let time_of_day = data[i].data[1].text_value;
    let fiscal_quarter = data[i].data[3].text_value;

    let fiscalYear = data[i].data[2].number_value;
    let fiscal_year = fiscalYear.toString();

    queue.publish_ProcessEarningsDate_Securities(
      ticker,
      name,
      earningsDate,
      time_of_day,
      fiscal_year,
      fiscal_quarter
    );
  }
}

export async function updateEarnings() {
  let data = await getDailyEarnings();
  let today = new Date();
  let est = new Date(today);
  est.setHours(est.getHours() - 5);
  est.toISOString().slice(0, 10);

  for (let i in data) {
    let suprise_percentage;
    let actual_reported_date = data[i].actual_reported_date;
    let eps_actual = data[i].eps_actual;
    let eps_percent_diff = data[i].eps_percent_diff;
    let ticker = data[i].security.ticker;

    let query = {
      text:
        "SELECT * FROM earnings_reports WHERE ticker = $1 AND earnings_date = $2",
      values: [ticker, est],
    };
    let result = await db(query);

    if (result.length > 0) {
      if (!eps_percent_diff && eps_actual && result[0].eps_estimate) {
        let percentage = (eps_actual / result[0].eps_estimate - 1) * 100;
        suprise_percentage = percentage.toFixed(2);
      } else {
        suprise_percentage = eps_percent_diff;
      }

      let query = {
        text:
          "UPDATE earnings_reports SET actual_reported_date = $2, eps_actual = $3, suprise_percentage = $4 WHERE ticker = $1 AND earnings_date = $5",
        values: [
          ticker,
          actual_reported_date,
          eps_actual,
          suprise_percentage,
          est,
        ],
      };
      await db(query);
      console.log(ticker + " actual earnings reported");
    } else {
      //   let query = {
      //     text:
      //       "INSERT INTO earnings_reports (ticker, type, fiscal_year, fiscal_quarter, actual_reported_date, eps_actual, suprise_percentage) VALUES ( $1, $2, $3, $4, $5, $6, $7 )",
      //     values: [
      //       ticker,
      //       fiscal_year,
      //       fiscal_quarter,
      //       actual_reported_date,
      //       eps_actual,
      //       eps_percent_diff,
      //     ],
      //   };
      //   await db(query);
      //   console.log(ticker + "actual earnings added and reported");
    }
  }
}

export async function insertEarnings(
  ticker,
  name,
  earnings_date,
  time_of_day,
  eps_estimate,
  ranking,
  logo_url,
  type,
  fiscal_year,
  fiscal_quarter
) {
  if (!earnings_date || !ticker) {
    return;
  }

  let query = {
    text:
      "SELECT * FROM earnings_reports WHERE ticker = $1 AND earnings_date = $2",
    values: [ticker, earnings_date],
  };
  let result = await db(query);

  if (result.length > 0) {
    // let query = {
    //   text:
    //     "UPDATE earnings_reports SET earnings_date = $2, time_of_day = $3, eps_estimate = $4, ranking = $5, logo_url = $6, name = $7, type = $8, fiscal_year = $9, fiscal_quarter = $10 WHERE ticker = $1 AND eps_actual IS NULL", // AND eps_actual IS NULL",
    //   values: [
    //     ticker,
    //     earnings_date,
    //     time_of_day,
    //     eps_estimate,
    //     ranking,
    //     logo_url,
    //     name,
    //     type,
    //     fiscal_year,
    //     fiscal_quarter,
    //   ],
    // };
    // await db(query);
  } else {
    let query = {
      text:
        "INSERT INTO earnings_reports (ticker, earnings_date, time_of_day, eps_estimate, ranking, logo_url, name, type, fiscal_year, fiscal_quarter ) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 ) RETURNING *",
      values: [
        ticker,
        earnings_date,
        time_of_day,
        eps_estimate,
        ranking,
        logo_url,
        name,
        type,
        fiscal_year,
        fiscal_quarter,
      ],
    };
    await db(query);
  }
}
