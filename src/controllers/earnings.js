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

// init intrinio
intrinioSDK.ApiClient.instance.authentications["ApiKeyAuth"].apiKey =
  process.env.INTRINIO_API_KEY;

intrinioSDK.ApiClient.instance.basePath = `${process.env.INTRINIO_BASE_PATH}`;

const companyAPI = new intrinioSDK.CompanyApi();
const securityAPI = new intrinioSDK.SecurityApi();
const indexAPI = new intrinioSDK.IndexApi();

export async function getIntrinioEarnings() {
  try {
    let today = new Date(); //.toISOString().slice(0, 10);

    let url = `${process.env.INTRINIO_BASE_PATH}/zacks/eps_surprises?start_date=${today}&end_date=${today}&api_key=${process.env.INTRINIO_API_KEY}`;

    let response = await axios.get(url);
    let data = response.data;
    let eps_surprises = data.eps_surprises;

    if (data && eps_surprises.length > 0) {
      for (let i in eps_surprises) {
        let type;
        let time_of_day;
        let ranking;
        let logo_url;
        let name;
        let fiscal_year = eps_surprises[i].fiscal_year;
        let fiscal_quarter = eps_surprises[i].fiscal_quarter;
        let earnings_date = eps_surprises[i].actual_reported_date;
        let eps_actual = eps_surprises[i].eps_actual;
        let eps_mean_estimate = eps_surprises[i].eps_mean_estimate;
        let eps_percent_diff = eps_surprises[i].eps_percent_diff;
        let ticker = eps_surprises[i].security.ticker;
        let apiName = eps_surprises[i].security.name;

        let security = await securities.getSecurityByTicker(ticker);

        if (security) {
          type = security.type;
        }

        //time of day

        //ranking

        //logo url

        //our name

        // console.log(
        //   "ticker",
        //   ticker,
        //   "name",
        //   name,
        //   "fiscal_year",
        //   fiscal_year,
        //   "fiscal_quarter",
        //   fiscal_quarter,
        //   "earnings_date",
        //   earnings_date,
        //   "eps_actual",
        //   eps_actual,
        //   "eps_mean_estimate",
        //   eps_mean_estimate,
        //   "eps_percent_diff",
        //   eps_percent_diff,
        //   "\n"
        // );
      }
    }
  } catch (e) {
    console.error(e);
  }
}

// export async function getEarningsReports() {
//   //fix!
//   let result = await db(`
//       SELECT widget_instances.*, widget_data.*, widgets.*
//       FROM widget_instances
//     `);

//   return result;
// }

//let yesterday = new Date(today);

//yesterday.setDate(yesterday.getDate() - 1);

//const url = `${process.env.INTRINIO_BASE_PATH}/zacks/eps_surprises?start_date=${today}&end_date=${today}&api_key=${process.env.INTRINIO_API_KEY}`;
// const body = {
//   operator: "AND",
//   clauses: [
//     {
//       field: "next_earnings_date",
//       operator: "gt",
//       value: yesterday,
//     },
//     {
//       field: "next_earnings_time_of_day",
//       operator: "gt",
//       value: "0",
//     },
//   ],
// };

// let res = axios
//   .post(url, body)
//   .then(function (data) {
//     console.log(data);
//     return data;
//   })
//   .catch(function (err) {
//     console.log(err);
//     return err;
//   });

//let data = await res.then((data) => data.data);

export async function getFutureEarningsDates() {
  let today = new Date(); //.toISOString().slice(0, 10);
  let yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);

  let url = `${process.env.INTRINIO_BASE_PATH}/securities/screen?order_column=next_earnings_date&order_direction=asc&page_size=10000&api_key=${process.env.INTRINIO_API_KEY}`;
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
    let earningsDate = data[i].data[0].text_value;
    let time_of_day = data[i].data[1].text_value;

    queue.publish_ProcessEarningsDate_Securities(
      ticker,
      earningsDate,
      time_of_day
    );
  }
}

export async function insertEarnings(
  ticker,
  earnings_date,
  time_of_day,
  eps_estimate,
  ranking,
  logo_url
) {
  if (!earnings_date || !ticker) {
    return;
  }

  let query = {
    text:
      "SELECT * FROM earnings_reports WHERE ticker = $1 AND eps_actual IS NULL",
    values: [ticker],
  };
  let result = await db(query);

  if (result.length > 0) {
    let query = {
      text:
        "UPDATE earnings_reports SET earnings_date = $2, time_of_day = $3, eps_estimate = $4, ranking = $5, logo_url = $6 WHERE ticker = $1 AND eps_actual IS NULL",
      values: [
        ticker,
        earnings_date,
        time_of_day,
        eps_estimate,
        ranking,
        logo_url,
      ],
    };
    await db(query);
  } else {
    let query = {
      text:
        "INSERT INTO earnings_reports (ticker, earnings_date, time_of_day, eps_estimate, ranking, logo_url ) VALUES ( $1, $2, $3, $4, $5, $6 ) RETURNING *",
      values: [
        ticker,
        earnings_date,
        time_of_day,
        eps_estimate,
        ranking,
        logo_url,
      ],
    };
    await db(query);
  }
}
