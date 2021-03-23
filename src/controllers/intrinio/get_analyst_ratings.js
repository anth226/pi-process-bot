import axios from "axios";
import {getEnv} from "../../env";

export function analystSnapshot(ticker) {
  let lastPrice = axios
    .get(
      `${
        getEnv("INTRINIO_BASE_PATH")
      }/securities/${ticker.toUpperCase()}/zacks/analyst_ratings/snapshot?source=iex&api_key=${
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

  return lastPrice;
}
