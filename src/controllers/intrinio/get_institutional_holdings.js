import axios from "axios";

// https://api-v2.intrinio.com/zacks/institutional_holdings?owner_cik=0001584087&page_size=10000&api_key=
// https://api-v2.intrinio.com/zacks/institutional_holdings?owner_cik=0001584087&next_page=UlBBWXwzMDYxNzMxMDI&api_key=

export function getInstitutionalHoldings(cik, next_page = null) {
  let url = `https://api-v2.intrinio.com/zacks/institutional_holdings?owner_cik=${cik}&api_key=${process.env.INTRINIO_API_KEY}`;

  if (next_page) {
    url = `https://api-v2.intrinio.com/zacks/institutional_holdings?owner_cik=${cik}&next_page=${next_page}&api_key=${process.env.INTRINIO_API_KEY}`;
  }

  let data = axios
    .get(url)
    .then(function(res) {
      return res.data;
    })
    .catch(function(err) {
      console.log(err);
      return {};
    });

  return data;
}
