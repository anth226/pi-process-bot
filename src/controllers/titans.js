import axios from "axios";
import db from "../db";
import * as queue from "../queue";

import * as holdings from "./holdings";

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
  // Calculate performances
  // Calculate sectors

  let data = await holdings.fetchAll(cik);

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

  console.log(merged);

  // //

  // let sectors = result.map(({ json }) => json["sector"]);

  // console.log(sectors);

  // let counts = {};
  // let total = 0;

  // for (var i = 0; i < sectors.length; i++) {
  //   let sector = sectors[i];
  //   counts[`${sector}`] = counts[`${sector}`] ? counts[`${sector}`] + 1 : 1;
  //   total += 1;
  // }

  // // console.log(counts);

  // for (let key in counts) {
  //   if (counts.hasOwnProperty(key)) {
  //     counts[key] = (counts[key] / total) * 100;
  //   }
  // }

  // console.log(counts);

  // //

  // calculate composition by market value
}
