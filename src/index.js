import "dotenv/config";
import express from "express";
import admin from "firebase-admin";
import cookieParser from "cookie-parser";
import bodyParser from "body-parser";

import * as companies from "./controllers/companies";
import * as titans from "./controllers/titans";
import * as holdings from "./controllers/holdings";
import * as performances from "./controllers/performances";

const { Consumer } = require("sqs-consumer");

// AWS_SQS_URL_BILLIONAIRE_HOLDINGS (Individual)
const consumer_1 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_BILLIONAIRE_HOLDINGS,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await holdings.fetchHoldings_Billionaire(
      sqsMessage.cik,
      Number(sqsMessage.id),
      Number(sqsMessage.batchId),
      sqsMessage.cache
    );
  },
});

consumer_1.on("error", (err) => {
  console.error(err.message);
});

consumer_1.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_BILLIONAIRE_PERFORMANCES (Individual)
const consumer_2 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_BILLIONAIRE_PERFORMANCES,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await performances.calculatePerformance_Billionaire(
      sqsMessage.cik,
      Number(sqsMessage.id),
      Number(sqsMessage.batchId),
      sqsMessage.cache
    );

    await titans.cacheCompanies_Portfolio(sqsMessage.cik);
  },
});

consumer_2.on("error", (err) => {
  console.error(err.message);
});

consumer_2.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_COMPANY_LOOKUP (Individual)
const consumer_3 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_COMPANY_LOOKUP,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await companies.lookupCompany(sqsMessage.identifier);
  },
});

consumer_3.on("error", (err) => {
  console.error(err.message);
});

consumer_3.on("processing_error", (err) => {
  console.error(err.message);
});

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
  res.send("hello");
});

app.get("/cache_holdings_titans", async (req, res) => {
  await holdings.cacheHoldings_Billionaires();
  res.send("ok");
});

app.get("/cache_performances_titans", async (req, res) => {
  await performances.cachePerformances_Billionaires();
  res.send("ok");
});

app.listen(process.env.PORT, () => {
  console.log(`listening on ${process.env.PORT}`);

  consumer_1.start();
  consumer_2.start();
  consumer_3.start();
});
