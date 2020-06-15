import axios from "axios";
import db from "../db";
import * as queue from "../queue";

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
