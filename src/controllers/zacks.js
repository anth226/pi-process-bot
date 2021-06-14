import parser from 'fast-xml-parser';
import axios from "axios";
import cheerio from "cheerio";
import * as queue from "../queue";

import db from "../db";
import { getEnv } from "../env";

export const fetchZackFeeds = async () => {
  const { data } = await axios.get(getEnv("ZACK_FEED"));

  if (data) {
    let parsedData = parser.parse(data);
    if (parsedData && parsedData.root && parsedData.root.entry) {
      let zackData = parsedData.root.entry;

      for await (let zack of zackData) {
        console.log("processing ticker: ", zack);

        let query = {
          text: "SELECT * FROM zacks_rank WHERE ticker = $1",
          values: [zack.ticker],
        };
        const result = await db(query);

        let rank = parseInt(zack.zacksrank);

        if (rank && rank > 0) {
          if (result.length > 0) {
            query = {
              text:
                "UPDATE zacks_rank SET rank = $1, updated_at = NOW() WHERE ticker = $2",
              values: [rank, zack.ticker],
            };
            await db(query);
            console.log("Update rank: ", zack.ticker);
          } else {
            let sector = await scrapeZackCategory(zack.ticker);
            query = {
              text:
                "INSERT INTO zacks_rank (ticker, rank, sector) VALUES ( $1, $2, $3 )",
              values: [zack.ticker, rank, sector],
            };
            await db(query);
            console.log("Insert rank: ", zack.ticker);
          }
        }
      }
    }
  }
};


export const setZackCategories = async () => {
  let result = await db(`
    SELECT *
    FROM zacks_rank
  `);

  for (let security of result) {
    queue.publish_ProcessZacksCategories(String(security.id), security.ticker);
  }
};

export async function scrapeZackCategory(ticker) {
  try {
    const response = await axios.get(
      `https://www.zacks.com/stock/quote/${ticker}`
    );

    const $ = cheerio.load(response.data);

    let data = [];

    $("table.abut_top").each(function (idx, element) {
      let find = $(element).text();
      data.push(find);
    });

    return data[0].substring(26).trim();
  } catch (error) {
    return null;
  }
}

export const updateZackCategories = async (id, sector) => {
  let query = {
    text:
      "UPDATE zacks_rank SET sector = $2 WHERE id = $1",
    values: [id, sector],
  };
  await db(query);
  console.log("zacks cat updated");
};