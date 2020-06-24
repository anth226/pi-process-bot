import axios from "axios";
import db from "../db";
import * as queue from "../queue";
const chalk = require("chalk");

// export async function cachePrices_Portfolio(cik) {
//   let result = await db(`
//       SELECT *
//       FROM holdings
//       WHERE cik = '${cik}'
//       ORDER BY batch_id DESC
//       LIMIT 1
//     `);

//   if (result.length > 0) {
//     let { data_url } = result[0];
//     try {
//       let result = await axios.get(data_url, {
//         crossdomain: true,
//         withCredentials: false,
//         headers: {
//           "Content-Type": "application/json",
//           // "Access-Control-Allow-Origin": "*"
//         },
//       });

//       let holdings = result.data;

//       holdings.forEach((holding) => {
//         let {
//           company: { ticker },
//         } = holding;
//         console.log(ticker);
//         queue.publish_ProcessSecurityPrices(ticker);
//       });
//     } catch {}
//   }
// }

async function fetch(identifier, frequency) {
  let url = `${process.env.INTRINIO_BASE_PATH}/securities/${identifier}/historical_data/adj_close_price?frequency=${frequency}&api_key=${process.env.INTRINIO_API_KEY}`;

  let next_page = null;
  let historical_data = [];
  let buffer = {};

  try {
    do {
      let response = await axios.get(url);
      //   console.log(response);

      let { data } = response;

      next_page = data["next_page"];

      buffer = data["historical_data"];
      historical_data = historical_data.concat(buffer);

      console.log(chalk.bgGreen("next_page =>"), next_page);

      if (next_page) {
        url = `${process.env.INTRINIO_BASE_PATH}/securities/${identifier}/historical_data/adj_close_price?frequency=${frequency}&next_page=${next_page}&api_key=${process.env.INTRINIO_API_KEY}`;
      }
    } while (next_page);

    return historical_data;
  } catch (e) {
    console.error(e);
    return [];
  }
}

export async function fetchTape(identifier) {
  let frequency = "quarterly";
  let prices = await fetch(identifier, frequency);

  console.log(prices);

  let data = {
    prices,
  };

  let result = await db(`
    SELECT *
    FROM prices
    WHERE ticker='${identifier}'
  `);

  if (result.length > 0) {
    let query = {
      text:
        "UPDATE prices SET json=($1), updated_at=now() WHERE ticker=($2) RETURNING *",
      values: [data, identifier],
    };

    result = await db(query);
  } else {
    let query = {
      text:
        "INSERT INTO prices ( json, ticker, created_at, updated_at, frequency ) VALUES ( $1, $2, now(), now(), $3 ) RETURNING *",
      values: [data, identifier, frequency],
    };

    result = await db(query);
  }
}
