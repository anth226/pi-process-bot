import "dotenv/config";
import express from "express";
import admin from "firebase-admin";
import cookieParser from "cookie-parser";
import bodyParser from "body-parser";

import * as companies from "./controllers/companies";
import * as securities from "./controllers/securities";
import * as earnings from "./controllers/earnings";
import * as titans from "./controllers/titans";
import * as holdings from "./controllers/holdings";
import * as performances from "./controllers/performances";
import * as institutions from "./controllers/institutions";
import * as networth from "./controllers/networth";
import * as widgets from "./controllers/widgets";
import * as mutualfunds from "./controllers/mutualfunds";
import * as etfs from "./controllers/etfs";
import * as pages from "./controllers/pages";
import * as nlp from "./controllers/nlp";
import * as userPortfolios from "./controllers/userportfolios";
import * as ncds from "./controllers/ncds";

import * as yahoo from "./controllers/yahoo";

import * as queue from "./queue";
//import * as queue2 from "./queue2";
import redis from "./redis";

var bugsnag = require("@bugsnag/js");
var bugsnagExpress = require("@bugsnag/plugin-express");

var bugsnagClient = bugsnag({
  apiKey: process.env.BUGSNAG_KEY,
  otherOption: process.env.RELEASE_STAGE,
});

bugsnagClient.use(bugsnagExpress);

var middleware = bugsnagClient.getPlugin("express");
/*
~~~~~~Configuration Stuff~~~~~~
*/
// debug
var rawBodySaver = function (req, res, buf, encoding) {
  if (buf && buf.length) {
    req.rawBody = buf.toString(encoding || "utf8");
  }
};

// set up middlewares
const app = express();
app.use(cookieParser());
//app.use(express.json());
app.use(bodyParser.json({ verify: rawBodySaver }));
app.use(bodyParser.urlencoded({ verify: rawBodySaver, extended: true }));
app.use(bodyParser.raw({ verify: rawBodySaver, type: "*/*" }));
//for frontend
app.use(express.static("dist"));

app.use(middleware.requestHandler);
app.use(middleware.errorHandler);

/*
~~~~~~Middlewares~~~~~~
*/

function checkAuth(req, res, next) {
  if (
    req.cookies.access_token &&
    req.cookies.access_token.split(" ")[0] === "Bearer"
  ) {
    // Handle token presented as a Bearer token in the Authorization header
    const session = req.cookies.access_token.split(" ")[1];
    admin
      .auth()
      .verifySessionCookie(session, true)
      .then((decodedClaims) => {
        req.terminal_app = { claims: decodedClaims };
        next();
      })
      .catch((error) => {
        // Session is unavailable or invalid. Force user to login.
        res.status(403).send("Unauthorized");
      });
  } else {
    // Bearer cookie doesnt exist
    res.status(403).send("Unauthorized");
  }
}

/*
~~~~~~Routes~~~~~~
*/

//Request URL: https://terminal.retirementinsider.com/success?session_id=cs_test_rVIlOBqZ6XvLsDFCCI8MVveNLuFCpJUqsH1vKfIFWLQSl9nPcILCUM85

// index
app.get("/", async (req, res) => {
  res.render("/public/index");
});

// redis flush
app.get("/redis/flush", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }

  await redis.flushall("ASYNC");
  res.send("ok");
});

/*      SQS routes      */

/* Billionaires */

// /cache_holdings_titans?token=XXX
app.get("/cache_holdings_titans", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await holdings.cacheHoldings_Billionaires();
  res.send("ok");
});

// /cache_performances_titans?token=XXX
app.get("/cache_performances_titans", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await performances.cachePerformances_Billionaires();
  res.send("ok");
});

// /generate_summaries_titans?token=XXX
app.get("/generate_summaries_titans", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await performances.generateSummaries_Billionaires();
  res.send("ok");
});

// /update_networth_titans?token=XXX
app.get("/update_networth_titans", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await networth.updateNetWorth_Billionaires();
  res.send("ok");
});

// /process_snapshots_titans?token=XXX
app.get("/process_snapshots_titans", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await titans.processTitansSnapshots();
  res.send("ok");
});

// /billionaires/:id/generate_summary?token=XXX
app.get("/billionaires/:id/generate_summary", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await titans.processHoldingsPerformanceAndSummary(req.params.id);
  res.send("ok");
});

/* Securities */

//    FETCH BOT
// /update_metrics_securities?token=XXX
// app.get("/update_metrics_securities", async (req, res) => {
//   if (process.env.DISABLE_CRON == "true") {
//     res.send("disabled");
//     return;
//   }
//   let { query } = req;
//   if (query.token != "XXX") {
//     res.send("fail");
//     return;
//   }
//   await securities.fillSecurities();
//   res.send("ok");
// });
//    FETCH BOT

// /update_performances_securities?token=XXX
app.get("/update_performances_securities", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await securities.fillPerformancesSecurities();
  res.send("ok");
});

// /fill_earnings_securities?token=XXX
app.get("/fill_earnings_securities", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await earnings.fillEarnings();
  res.send("ok");
});

// /fill_holdings_count_securities?token=XXX
app.get("/fill_holdings_count_securities", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  res.send("no");
  //seperate it out to just do one at a time
  // await securities.fillHoldingsCountSecurities();
  // setTimeout(function () {
  //   res.send("ok");
  // }, 30000);
});

/* Earnings */

// /update_eps_earnings?token=XXX
app.get("/update_eps_earnings", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await earnings.updateEarnings();

  let id = await widgets.getWidgetTypeId("SecuritiesEarningsCalendar");
  await widgets.processInput(id[0].id);
  res.send("ok");
});

/* Mutual Funds */

// /update_json_mutualfunds?token=XXX
app.get("/update_json_mutualfunds", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await mutualfunds.updateJson_MutualFunds();
  res.send("ok");
});

/* Companies */

// /update_metrics_companies?token=XXX
app.get("/update_metrics_companies", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await companies.updateJson_InsiderCompanies();
  await companies.updateMetrics_Companies();
  res.send("ok");
});

/* Widgets */

// /update_global_widgets?token=XXX
app.get("/update_global_widgets", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await widgets.updateGlobal();
  res.send("ok");
});

// /update_local_widgets?token=XXX
app.get("/update_local_widgets", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await widgets.updateLocal();
  res.send("ok");
});

app.get("/widgets/:id/process_input", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await widgets.processInput(req.params.id);
  res.send("ok");
});

/* User Portfolios */

// /update_user_portfolios?token=XXX
app.get("/update_user_portfolios", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await userPortfolios.fillUsersPortPerfs();
  res.send("ok");
});

// /user_portfolios/:id/update?token=XXX
app.get("/user_portfolios/:id/update", async (req, res) => {
  console.log("here");
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await userPortfolios.fillUserPortPerf(req.params.id);
  res.send("ok");
});

/* ETFs */

// /update_etfs?token=XXX
app.get("/update_etfs", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await etfs.updateJson_ETFs();
  res.send("ok");
});

/* Institutions */

// /fetch_institutional_holdings?token=XXX
app.get("/fetch_institutional_holdings", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await institutions.fetchHoldings();
  res.send("ok");
});

// /evaluate_top_10_institutions?token=XXX
app.get("/evaluate_top_10_institutions", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await institutions.evaluateTop10();
  res.send("ok");
});

// /evaluate_allocations_institutions?token=XXX
app.get("/evaluate_allocations_institutions", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await institutions.evaluateAllocations();
  res.send("ok");
});

// /calculate_performances_institutions?token=XXX
app.get("/calculate_performances_institutions", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  //await institutions.calculatePerformances();
  res.send("ok");
});

app.get("/process_snapshots_institutions", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await institutions.processInstitutionsSnapshots();
  res.send("ok");
});

/* S&P History */

// /create_indices_candles_daily?token=XXX
app.get("/create_indices_candles_daily", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await yahoo.sync();
  res.send("ok");
});

/* PAGES */

// /generate_pages_portfolios?token=XXX
app.get("/generate_pages_portfolios", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await pages.parseHoldings_Portfolios();
  res.send("ok");
});

// /generate_pages_titans?token=XXX
app.get("/generate_pages_titans", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }

  res.send("ok");
});

/* NLP */

// /categorize_securities?token=XXX
app.get("/categorize_securities", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await nlp.categorizeTickers();
  res.send("ok");
});

/* NCDS */
app.get("/ncds_consolidate", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await ncds.consolidate();
  res.send("ok");
});

/*      DB routes      */

app.get("/bot/institutions/", async (req, res) => {
  let data = await institutions.getInstitutionsUpdated({ size: 5000 });
  if (data.length > 0) {
    res.send({ data });
  }
});

app.get("/bot/billionaires/", async (req, res) => {
  let data = await titans.getBillionairesCiksAndNotes({ size: 5000 });
  if (data.length > 0) {
    res.send({ data });
  }
});

app.get("/bot/holdings/", async (req, res) => {
  let data = await holdings.getAllMaxBatch();
  if (data.length > 0) {
    res.send({ data });
  }
});

app.get("/bot/widgets/", async (req, res) => {
  let data = await widgets.getLocalWidgets();
  if (data.length > 0) {
    res.send({ data });
  }
});

// Start Server
app.listen(process.env.PORT || 8080, () => {
  console.log(`listening on ${process.env.PORT || 8080}`);

  //training
  nlp.trainClassifier();

  queue.consumer_1.start();
  queue.consumer_2.start();
  queue.consumer_3.start();
  queue.consumer_4.start();
  queue.consumer_5.start();
  queue.consumer_6.start();
  queue.consumer_7.start();
  queue.consumer_8.start();
  queue.consumer_9.start();
  queue.consumer_10.start();
  queue.consumer_11.start();
  queue.consumer_12.start();
  queue.consumer_13.start();
  queue.consumer_14.start();
  queue.consumer_15.start();
  queue.consumer_16.start();
  queue.consumer_17.start();
  queue.consumer_18.start();
  queue.consumer_19.start();
  queue.consumer_20.start();
  queue.consumer_21.start();
  if (process.env.RELEASE_STAGE == "production") {
    queue.newTickersConsumer.start();
  }
});
