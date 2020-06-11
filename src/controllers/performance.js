// https://ri-terminal.s3.amazonaws.com/portfolios.json

import * as titans from "./titans";
import * as queue from "../queue";

export async function getPortfolios() {
  return {
    url: "https://ri-terminal.s3.amazonaws.com/portfolios.json",
  };
}

export async function calculatePerformance_Billionaire(
  cik,
  billionaireId,
  batchId = null,
  cache = true
) {
  // https://api-v2.intrinio.com/historical_data/AAPL/marketcap?frequency=monthly&api_key=OjljMjViZjQzNWU4NGExZWZlZTFmNTY4ZDU5ZmI5ZDI0
  //
  // let next_page = null;
  // let index = 0;
  // let holdings = [];
  // let buffer = [];
  // let key = null;
  // do {
  //   let response = await getInstitutionalHoldings(cik, next_page);
  //   next_page = response["next_page"];
  //   if (cache) {
  //     key = `holdings/${cik}/${index}.json`;
  //     await uploadToS3(key, response);
  //   }
  //   holdings = response["holdings"];
  //   buffer = buffer.concat(holdings);
  //   console.log(holdings.length);
  //   for (let n = 0; n < holdings.length; n += 1) {
  //     await cacheTicker(billionaireId, holdings[n]["company"]["ticker"]);
  //   }
  //   index += 1;
  //   console.log(chalk.bgGreen("next_page =>"), next_page);
  // } while (next_page);
  // let query = {
  //   text:
  //     "UPDATE institutions SET holdings_page_count=($1), holdings_updated_at=($2) WHERE cik=($3) RETURNING *",
  //   values: [index + 1, new Date(), cik],
  // };
  // await db(query);
  // if (buffer.length > 0) {
  //   // Cache all data
  //   if (cache) {
  //     key = `holdings/historical/${cik}/${Number(new Date())}.json`;
  //     let response = await uploadToS3(key, buffer);
  //     let query = {
  //       text:
  //         "INSERT INTO holdings (cik, batch_id, data_url, created_at ) VALUES ( $1, $2, $3, now() ) RETURNING *",
  //       values: [cik, batchId, response["Location"]],
  //     };
  //     await db(query);
  //     console.log("holdings_historical => cached");
  //   }
  //   console.log(
  //     chalk.bgGreen("batch complete."),
  //     cik,
  //     batchId,
  //     cache,
  //     buffer.length
  //   );
  // }
}

export async function cachePerformances_Billionaires() {
  let result = await titans.getBillionaires({ size: 1000 });

  let records = result;

  let buffer = [];
  let batchId = 0;

  if (records.length > 0) {
    for (let i = 0; i < records.length; i += 1) {
      let cik = records[i]["cik"];
      let id = records[i]["id"];

      if (cik) {
        console.log(cik);

        queue.publish_ProcessPerformances(
          cik,
          id,
          batchId,
          !buffer.includes(cik)
        );

        if (buffer.includes(cik)) {
          buffer.push(cik);
        }
      }
    }
  }
}
