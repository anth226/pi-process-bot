import axios from "axios";
import {getEnv} from "../../env";

export function getCikHoldingsFromIntrinio(cik, next_page = null, date) {
  let url = `${getEnv("INTRINIO_BASE_PATH")}/owners/${cik}/institutional_holdings?page_size=10000&api_key=${getEnv("INTRINIO_API_KEY")}`

  if (next_page) {
    url = `${getEnv("INTRINIO_BASE_PATH")}/owners/${cik}/institutional_holdings?page_size=10000&next_page=${next_page}&api_key=${getEnv("INTRINIO_API_KEY")}`
  }

  if (date) {
    url = url + `&as_of_date=${date}`
    console.log("url: ", url);
  }

  let data = axios
    .get(url)
    .then(function (res) {
      return res.data;
    })
    .catch(function (err) {
      console.log(err);
      return {};
    });

  return data;
}
