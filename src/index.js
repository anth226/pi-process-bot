import "dotenv/config";
import express from "express";
import admin from "firebase-admin";
import cookieParser from "cookie-parser";
import bodyParser from "body-parser";

import * as companies from "./controllers/companies";
import * as titans from "./controllers/titans";
import * as holdings from "./controllers/holdings";
import * as performances from "./controllers/performances";
import * as institutions from "./controllers/institutions";
import * as networth from "./controllers/networth";
import * as widgets from "./controllers/widgets";
import * as mutualfunds from "./controllers/mutualfunds";
import * as etfs from "./controllers/etfs";
import * as nlp from "./controllers/nlp";

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
  }

  await redis.flushall("ASYNC");
  res.send("ok");
});

/*      SQS routes      */

/* Billionaires */

// /cache_holdings_titans?token=XXX
app.get("/cache_holdings_titans", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await holdings.cacheHoldings_Billionaires();
  res.send("ok");
});

// /cache_performances_titans?token=XXX
app.get("/cache_performances_titans", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await performances.cachePerformances_Billionaires();
  res.send("ok");
});

// /generate_summaries_titans?token=XXX
app.get("/generate_summaries_titans", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await performances.generateSummaries_Billionaires();
  res.send("ok");
});

// /update_networth_titans?token=XXX
app.get("/update_networth_titans", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await networth.updateNetWorth_Billionaires();
  res.send("ok");
});

// /billionaires/:id/generate_summary?token=XXX
app.get("/billionaires/:id/generate_summary", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await titans.processHoldingsPerformanceAndSummary(req.params.id);
  res.send("ok");
});

/* Mutual Funds */

// /update_json_mutualfunds?token=XXX
app.get("/update_json_mutualfunds", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await mutualfunds.updateJson_MutualFunds();
  res.send("ok");
});

/* Companies */

// /update_metrics_companies?token=XXX
app.get("/update_metrics_companies", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await companies.updateJson_InsiderCompanies();
  await companies.updateMetrics_Companies();
  res.send("ok");
});

/* Widgets */

// /update_global_widgets?token=XXX
app.get("/update_global_widgets", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await widgets.updateGlobal();
  res.send("ok");
});

// /update_local_widgets?token=XXX
app.get("/update_local_widgets", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await widgets.updateLocal();
  res.send("ok");
});

app.get("/widgets/:id/process_input", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await widgets.processInput(req.params.id);
  res.send("ok");
});

/* ETFs */

// /update_etfs?token=XXX
app.get("/update_etfs", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await etfs.updateJson_ETFs();
  res.send("ok");
});

/* ETFs */

// /categorize_securities?token=XXX
app.get("/categorize_securities", async (req, res) => {
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
  }
  await nlp.categorizeTickers();
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
});
