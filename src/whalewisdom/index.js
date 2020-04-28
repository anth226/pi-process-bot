import "dotenv/config";

import * as whalewisdom from "./whalewisdom";

import db from "../../src/db";

import * as titans from "../../src/controllers/titans";

(async () => {
  // await whalewisdom.seedInstitutions();

  // let result = await db(`
  //   SELECT *
  //   FROM institutions
  //   ORDER BY name ASC
  //   LIMIT 1
  // `);

  // // console.log(result);

  // result = await titans.getTitans({});

  // console.log(result);

  // if (result.length > 0) {
  //   for (let i = 0; i < result.length; i += 1) {
  //     let cik = result[i]["cik"];
  //     if (cik) {
  //       console.log(cik);
  //       await whalewisdom.fetchHoldings(cik);
  //     }
  //   }
  // }

  let cik = "0001029160";

  let result = await db(`
    SELECT *
    FROM billionaires
    WHERE cik = '${cik}'
  `);

  if (result && result.length > 0) {
    await whalewisdom.fetchHoldings_Billionaire(cik, result[0]["id"]);
  }
  // 0001067983;
})();
