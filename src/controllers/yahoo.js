import axios from "axios";
import cheerio from "cheerio";

import db from "../db";
const siteUrl = "https://finance.yahoo.com/quote/%5EGSPC/history/";

(async () => {
  //   await getIndexData();
  await sync();
})();

export async function fetchIndexData() {
  const result = await axios.get(siteUrl);
  return cheerio.load(result.data);
}

export async function getIndexData() {
  let data = [];

  const $ = await fetchIndexData();

  let table = $("[data-test=historical-prices]");

  table.find("tr").each((idx, element) => {
    let row = [];

    $(element)
      .find("span")
      .each((idx, span) => {
        let data = $(span).html();

        row.push(data);
      });

    data.push(row);

    row = [];
  });

  return data;
}

export async function sync() {
  let data = await getIndexData();

  for (let i = 1; i < data.length - 1; i += 1) {
    let row = data[i];
    let date = new Date(row[0].replace(/\s\s+/g, " "));

    date = row[0].replace(/\s\s+/g, " ");
    console.log(date);

    let result = await db(`
        SELECT *
        FROM indices_candles_daily
        WHERE timestamp = '${date}'
    `);

    if (result && result.length > 0) {
      console.log("skip");
      break;
    } else {
      console.log(row);

      let open = parseFloat(row[1].replace(/,/g, "")) * 100;
      let high = parseFloat(row[2].replace(/,/g, "")) * 100;
      let low = parseFloat(row[3].replace(/,/g, "")) * 100;
      let close = parseFloat(row[4].replace(/,/g, "")) * 100;
      let adjusted = parseFloat(row[5].replace(/,/g, "")) * 100;
      let volume = parseFloat(row[6].replace(/,/g, ""));

      let query = {
        text:
          "INSERT INTO indices_candles_daily (index, timestamp, open, high, low, close, adj_close, volume) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8 ) RETURNING *",
        values: ["INDEXSP", date, open, high, low, close, adjusted, volume],
      };

      await db(query);
    }
  }
}
