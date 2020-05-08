// https://ri-terminal.s3.amazonaws.com/portfolios.json

import * as performance from "../controllers/performance";
import * as titans from "../controllers/titans";
import axios from "axios";

const AWS = require("aws-sdk");
require("dotenv").config();

const chalk = require("chalk");

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

import { find, map, sortBy } from "lodash";

export async function zipPerformances_Billionaires() {
  console.time("zipPerformances_Billionaires");

  // Billionaires
  let billionaires = await titans.getBillionaires({});

  // Performances Url
  let response = await performance.getPortfolios();

  let { url } = response;

  let investors = billionaires.map((billionaire) => {
    console.log(chalk.bgYellow("billionaire.id"), billionaire.id);
    return {
      id: billionaire.id,
      billionaire,
    };
  });

  // Performances Data
  response = await axios.get(url, {
    crossdomain: true,
    withCredentials: false,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
  });

  let performances = [];

  if (response.status === 200) {
    performances = response.data;
  }

  //   console.log(performances);

  const mergedWithPerformance = map(investors, function (investor) {
    let cik = investor.hasOwnProperty("billionaire")
      ? investor.billionaire.cik
      : null;

    cik = cik ? cik.replace(/\s/g, "") : null;

    let performance = find(performances.portfolios, {
      filer_cik: cik,
    });

    if (performance) {
      console.log(chalk.bgGreen("success"), investor.id);

      return {
        ...investor,
        performance,
        weight: investor.hasOwnProperty("billionaire") ? 1 : 0,
      };
    } else {
      console.log(chalk.bgRed("fail"), investor.id, cik);

      return {
        ...investor,
        performance: {},
        weight: investor.hasOwnProperty("billionaire") ? 1 : 0,
      };
    }
  });

  let sorted = sortBy(mergedWithPerformance, ["weight", "net_worth"]);

  let filtered = sorted.filter((billionaire) => {
    return billionaire != null;
  });

  console.timeEnd("zipPerformances_Billionaires");

  let path = `results/billionaires.json`;
  let params = {
    Bucket: process.env.AWS_BUCKET_RI,
    Key: path,
    ACL: "public-read",
  };

  params = {
    ...params,
    Body: JSON.stringify(filtered),
    ContentType: "application/json",
  };

  response = await s3.upload(params).promise();

  //   console.log(sorted);

  console.log(response);

  return sorted;
}
