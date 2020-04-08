import axios from "axios";

export function getETFHoldings(ticker) {
  let holdings = axios
    .get(
      `https://api-v2.intrinio.com/zacks/etf_holdings?etf_ticker=${ticker.toUpperCase()}&api_key=${
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

  return holdings;
}

//https://api-v2.intrinio.com/zacks/etf_holdings
