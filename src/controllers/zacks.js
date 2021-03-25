import parser from 'fast-xml-parser';
import axios from "axios";

import db from "../db";
import {getEnv} from "../env";

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
            query = {
              text:
                "INSERT INTO zacks_rank (ticker, rank) VALUES ( $1, $2 )",
              values: [zack.ticker, rank],
            };
            await db(query);
            console.log("Insert rank: ", zack.ticker);
          }
        }
      }
    }
  }
};