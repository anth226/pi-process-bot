import intrinioSDK from "intrinio-sdk";
import axios from "axios";

export async function screen(intrinioApi, params) {
  const url = `https://api-v2.intrinio.com/securities/screen?order_column=marketcap&order_direction=asc&api_key=${process.env.INTRINIO_API_KEY}`;
  // const body = {
  //   "operator": "AND",
  //   "clauses": [
  //     //
  //     // {
  //     //   "field": "pricetoearnings",
  //     //   "operator": "lt",
  //     //   "value": "3"
  //     // },
  //     {
  //       "field": "sector",
  //       "operator": "eq",
  //       "value": "Consumer Goods"
  //     },
  //     {
  //       "field": "marketcap",
  //       "operator": "gt",
  //       "value": "100000000"
  //     },
  //   ]
  // }

  let res = axios
    .post(url, params)
    .then(function(data) {
      return data;
    })
    .catch(function(err) {
      return err;
    });

  return res.then(data => data.data);
}

/*
export async function highestYields(intrinioApi) {

  const url = `https://api-v2.intrinio.com/securities/screen?order_column=trailing_dividend_yield&order_direction=desc&page_size=50&api_key=${process.env.INTRINIO_API_KEY}`;
  const body = {
  "operator": "AND",
  "clauses": [
      {
        "field": "marketcap",
        "operator": "gt",
        "value": "1"
      },
      {
        "field": "trailing_dividend_yield",
        "operator": "gt",
        "value": "0"
      },
    ]
  }

  let res = axios.post(url, body)
    .then(function(data) {
      console.log(data);
      return data;
    }).catch(function(err) {
      console.log(err);
      return err;
    });

  return res.then((data) => data.data)
}*/

export async function similarCompanies(ticker, intrinioApi) {
  // get sector & industry category
  const res = await Promise.all(
    [
      intrinioApi.getSecurityDataPointText(ticker, "sector"),
      intrinioApi.getSecurityDataPointText(ticker, "industry_category")
    ].map(p => p.catch(e => e))
  );
  if (res[0] instanceof Error || res[1] instanceof Error) {
    return [];
  }

  const sector = res[0].replace(/['"]+/g, "");
  const industryCategory = res[1].replace(/['"]+/g, "").replace("\\u0026", "&");

  const url = `https://api-v2.intrinio.com/securities/screen?order_column=marketcap&order_direction=desc&page_size=50&api_key=${process.env.INTRINIO_API_KEY}`;
  const body = {
    operator: "AND",
    clauses: [
      {
        field: "marketcap",
        operator: "gt",
        value: "0"
      },
      {
        field: "sector",
        operator: "eq",
        value: sector
      },
      {
        field: "industry_category",
        operator: "eq",
        value: industryCategory
      }
    ]
  };

  let result = axios
    .post(url, body)
    .then(function(data) {
      return data.data.filter(obj => obj.security.ticker != ticker);
    })
    .catch(function(err) {
      return err;
    });

  return result.then(data => data);
}
