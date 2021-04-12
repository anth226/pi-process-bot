import axios from "axios";

import db from "../db";
import {getEnv} from "../env";

export const fetchCryptoNews = async () => {
  const token = getEnv("CRYPTO_NEWS_API_KEY");
  const url = "https://cryptonews-api.com/api/v1?tickers=BTC,ETH,XRP&items=50&token="+token;
  const { data } = await axios.get(url);

  if (data && data.data) {
    let news = data.data;
    for await (let n of news) {
      let tickers = n.tickers;
      for (let ticker of tickers) {
        let query = {
          text: "INSERT INTO crypto_news (ticker, news_url, image_url, title, description, source_name, timestamp, sentiment, type) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9 ) ON CONFLICT DO NOTHING",
          values: [ticker, n.news_url, n.image_url, n.title, n.text, n.source_name, n.date, n.sentiment, n.type],
        };
        await db(query);
      }
    }
  }
};