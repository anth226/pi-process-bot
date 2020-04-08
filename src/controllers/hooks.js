// https://ri-terminal.s3.amazonaws.com/portfolios.json

import * as performance from "./performance";
import * as titans from "./titans";
import axios from "axios";

const AWS = require("aws-sdk");
require("dotenv").config();

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});

import { find, map, sortBy } from "lodash";

export async function zipPerformances_Billionaires() {
  console.time("zipPerformances_Billionaires");

  // Billionaires
  let billionaires = await titans.getBillionaires({});

  // Performances Url
  let response = await performance.getPortfolios();

  let { url } = response;

  let investors = billionaires.map(billionaire => {
    return {
      id: billionaire.id,
      billionaire
    };
  });

  // Performances Data
  response = await axios.get(url, {
    crossdomain: true,
    withCredentials: false,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*"
    }
  });

  let performances = [];

  if (response.status === 200) {
    performances = response.data;
  }

  //   console.log(performances);

  const mergedWithPerformance = map(investors, function(investor) {
    let cik = investor.hasOwnProperty("billionaire")
      ? investor.billionaire.cik
      : null;

    let performance = find(performances.portfolios, { filer_cik: cik });

    return {
      ...investor,
      performance,
      weight: investor.hasOwnProperty("billionaire") ? 1 : 0
    };
  });

  let sorted = sortBy(mergedWithPerformance, ["weight", "net_worth"]);

  console.timeEnd("zipPerformances_Billionaires");

  let path = `performance/billionaires.json`;
  let params = {
    Bucket: process.env.AWS_BUCKET_RI,
    Key: path
  };

  params = {
    ...params,
    Body: JSON.stringify(sorted),
    ContentType: "application/json"
  };

  response = await s3.upload(params).promise();

  console.log(response);

  return sorted;
}
