import axios from "axios";
import {getEnv} from "../../env";

export function getETFHoldings(ticker) {
  let holdings = axios
    .get(
      `${
        getEnv("INTRINIO_BASE_PATH")
      }/zacks/etf_holdings?etf_ticker=${ticker.toUpperCase()}&api_key=${
        getEnv("INTRINIO_API_KEY")
      }`
    )
    .then(function (res) {
      return res.data;
    })
    .catch(function (err) {
      console.log(err);
      return {};
    });

  return holdings;
}

//https://api-v2.intrinio.com/zacks/etf_holdings
