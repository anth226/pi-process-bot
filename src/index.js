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
import * as zacks from "./controllers/zacks"

import * as yahoo from "./controllers/yahoo";

import * as trades from "./controllers/trades";
import * as alerts from "./controllers/alerts";

import * as queue from "./queue";
//import * as queue2 from "./queue2";
import redis, {
  syncRedisData
} from "./redis";
import {getEnv} from "./env";

var bugsnag = require("@bugsnag/js");
var bugsnagExpress = require("@bugsnag/plugin-express");

var bugsnagClient = bugsnag({
  apiKey: getEnv("BUGSNAG_KEY"),
  otherOption: getEnv("RELEASE_STAGE"),
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

var cronJob = require('cron').CronJob;
const client = require('twilio')(
  process.env.TWILIO_ACCOUNT_SID,
  process.env.TWILIO_AUTH_TOKEN
);
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

// redis flush
app.get("/redis/sync-shared-data", async (req, res) => {
  if (getEnv("RELEASE_STAGE") == "production") {
    res.send("fail");
    return;
  }

  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }

  await syncRedisData();
  
  res.send("ok");
});

/*      SQS routes      */

/* Billionaires */

// /cache_holdings_titans?token=XXX
app.get("/cache_holdings_titans", async (req, res) => {
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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

app.get("/get_env", async (req, res) => {
  let { query } = req;

  if (query.env && query.env.length > 0) {
    //console.log(query.env);
  } else {
    res.send("failed, no env");
    return;
  }

  let env = query.env;
  let result = getEnv(env);

  res.send({result: result});
});

// /update_networth_titans?token=XXX
app.get("/update_networth_titans", async (req, res) => {
  if (getEnv("DISABLE_CRON") == "true") {
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

// TODO: Remove titans snapshots sqs, lambda, and functions
// deprecated
// /process_snapshots_titans?token=XXX
app.get("/process_snapshots_titans", async (req, res) => {
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
//   if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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

app.get("/process_snapshots_ciks", async (req, res) => {
  if (getEnv("DISABLE_CRON") == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await institutions.processCiks("snapshots");
  res.send("ok");
});

app.get("/process_top10_and_allocations_ciks", async (req, res) => {
  if (getEnv("DISABLE_CRON") == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await institutions.processCiks("top10andallocations");
  res.send("ok");
});

/* S&P History */

// /create_indices_candles_daily?token=XXX
app.get("/create_indices_candles_daily", async (req, res) => {
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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
  if (getEnv("DISABLE_CRON") == "true") {
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

// Fetch all Ciks
app.get("/fetch_ciks", async (req, res) => {
  if (getEnv("DISABLE_CRON") == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await institutions.fetchAllCiks();
  res.send("ok");
});

// Alerts

// app.use("/alerts", checkAuth);
app.post("/alerts", async (req, res) => {
  const result = await alerts.createAlert(
    req.body.name,
    req.body.message,
    req.body.isDaily
  );
  res.send(result);
});

//app.use("/alerts/:id", checkAuth);
app.get("/alerts", async (req, res) => {
  const result = await alerts.getAlerts(req);
  res.send(result);
});

//app.use("/alerts/:id", checkAuth);
app.get("/alerts/:id", async (req, res) => {
  const result = await alerts.getAlert(req.params.id);
  res.send(result);
});

//app.use("/alerts/:id/users", checkAuth);
app.get("/alerts/:id/users", async (req, res) => {
  const result = await alerts.getAlertUsers(req.params.id);
  res.send(result);
});

//app.use("/alerts/:id/activate", checkAuth);
app.get("/alerts/:id/activate", async (req, res) => {
  const result = await alerts.activateAlert(req.params.id);
  res.send(result);
});

//app.use("/alerts/:id/deactivate", checkAuth);
app.get("/alerts/:id/deactivate", async (req, res) => {
  const result = await alerts.deactivateAlert(req.params.id);
  res.send(result);
});

 app.use("/alerts/:id/addUser", checkAuth);
app.get("/alerts/:id/addUser", async (req, res) => {
  const result = await alerts.addAlertUser(
    req.terminal_app.claims.uid,
    req.params.id,
    req.body.phone
  );
  res.send(result);
});

app.use("/alerts/:id/subscribe", checkAuth);
app.get("/alerts/:id/subscribe", async (req, res) => {
  const result = await alerts.subscribeAlert(
    req.body.phone,
    req.params.id
  );
  res.send(result);
});

app.use("/alerts/:id/unsubscribe", checkAuth);
app.get("/alerts/:id/unsubscribe", async (req, res) => {
  const result = await alerts.unsubscribeAlert(
    req.body.phone,
    req.params.id
  );
  res.send(result);
});

//app.use("/daily_alerts", checkAuth);
app.get("/daily_alerts", async (req, res) => {
  const result = await alerts.getDailyAlerts();
  res.send(result);
});

// end point to use to get the daily ark trade, processing the last 30 days of those trades into ark_portfolio, sending out daily SMS notif
app.get("/daily_arkTrades", async (req, res) => {
  try {
    let dailyArkTrades = await trades.getTradesFromARK();

    let updatedDailyAlert = await alerts.updateCWDailyAlertMessage();

    let dailyAlerts = await alerts.getDailyAlerts();
    var alertUsers;

    if(dailyAlerts.length > 0) {
      for( var i = 0; i < dailyAlerts.length; i++ ) {
        alertUsers = await alerts.getAlertActiveUsers(dailyAlerts[i].id);
        if(alertUsers.length > 0) {
          for( var x = 0; x < alertUsers.length; x++ ) {
            client.messages
            .create({
              from: process.env.TWILIO_PHONE_NUMBER,
              to: alertUsers[x].user_phone_number,
              body: dailyAlerts[i].message
            })
            .then(() => {
              console.log(JSON.stringify({ success: true }));
            })
            .catch(err => {
              console.log(err);
              console.log(JSON.stringify({ success: false }));
            });
          }
        }
      }
    }
  } catch (error) {
    console.log(error);
  }  
});

// Fetch Zacks Rank
app.get("/fetch_zacks", async (req, res) => {
  if (process.env.DISABLE_CRON == "true") {
    res.send("disabled");
    return;
  }
  let { query } = req;
  if (query.token != "XXX") {
    res.send("fail");
    return;
  }
  await zacks.fetchZackFeeds();
  res.send("success");
});

// Start Server
app.listen(process.env.PORT || 8080, () => {
  console.log(`listening on ${process.env.PORT || 8080}`);

  if (getEnv("DISABLE_CONSUMER") == "true") {
    console.log("consumer disabled");
    return;
  }

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
  queue.consumer_22.start();
  queue.cikConsumer.start();
  if (getEnv("RELEASE_STAGE") == "production") {
    queue.newTickersConsumer.start();
  }
});
// debug