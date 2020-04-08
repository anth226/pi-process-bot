import axios from "axios";

export function analystSnapshot(ticker) {
  let lastPrice = axios
    .get(
      `https://api-v2.intrinio.com/securities/${ticker.toUpperCase()}/zacks/analyst_ratings/snapshot?source=iex&api_key=${
        process.env.INTRINIO_API_KEY
      }`
    )
    .then(function(res) {
      return res.data;
    })
    .catch(function(err) {
      console.log(err);
      return {};
    });

  return lastPrice;
}
