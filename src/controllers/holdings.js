import "dotenv/config";

import * as whalewisdom from "../whalewisdom/whalewisdom";

import db from "../db";

import * as titans from "./titans";

export async function cacheHoldings_Titans() {
  let result = await db(`
    SELECT *
    FROM institutions
    ORDER BY name ASC
    LIMIT 1
  `);

  // console.log(result);

  result = await titans.getTitans({});

  console.log(result);

  if (result.length > 0) {
    for (let i = 0; i < result.length; i += 1) {
      let cik = result[i]["cik"];
      let id = result[i]["id"];

      if (cik) {
        console.log(cik);
        // await whalewisdom.fetchHoldings(cik);
        await whalewisdom.fetchHoldings_Billionaire(cik, id);
      }
    }
  }
}
