import "dotenv/config";
const whalewisdom = require("./whalewisdom");

import db from "../../src/db";

import * as titans from "../../src/controllers/titans";

(async () => {
  // await whalewisdom.seedInstitutions();

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
      if (cik) {
        console.log(cik);
        await whalewisdom.fetchHoldings(cik);
      }
    }
  }

  // 0001067983;
})();
