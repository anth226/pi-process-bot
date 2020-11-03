import * as companies from "./controllers/companies";
import * as titans from "./controllers/titans";
import * as holdings from "./controllers/holdings";
import * as institutions from "./controllers/institutions";
import * as performances from "./controllers/performances";
import * as prices from "./controllers/prices";
import * as mutualfunds from "./controllers/mutualfunds";
import * as widgets from "./controllers/widgets";
import * as etfs from "./controllers/etfs";
import * as nlp from "./controllers/nlp";

const { Consumer } = require("sqs-consumer");

const AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });
const sqs = new AWS.SQS({ apiVersion: "2012-11-05" });

export function publish_ProcessHoldings(cik, id, batchId, cache) {
  let queueUrl = process.env.AWS_SQS_URL_BILLIONAIRE_HOLDINGS;

  let data = {
    cik,
    id,
    batchId,
    cache,
  };

  let params = {
    MessageAttributes: {
      cik: {
        DataType: "String",
        StringValue: data.cik,
      },
      id: {
        DataType: "String",
        StringValue: `${data.id}`,
      },
      batchId: {
        DataType: "String",
        StringValue: `${data.batchId}`,
      },
      cache: {
        DataType: "String",
        StringValue: `${data.cache}`,
      },
    },
    MessageBody: JSON.stringify(data),
    // MessageDeduplicationId: req.body["userEmail"],
    // MessageGroupId: "Holdings",
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessPerformances(cik, id, batchId, cache) {
  let queueUrl = process.env.AWS_SQS_URL_BILLIONAIRE_PERFORMANCES;

  let data = {
    cik,
    id,
    batchId,
    cache,
  };

  let params = {
    MessageAttributes: {
      cik: {
        DataType: "String",
        StringValue: data.cik,
      },
      id: {
        DataType: "String",
        StringValue: `${data.id}`,
      },
      batchId: {
        DataType: "String",
        StringValue: `${data.batchId}`,
      },
      cache: {
        DataType: "String",
        StringValue: `${data.cache}`,
      },
    },
    MessageBody: JSON.stringify(data),
    // MessageDeduplicationId: req.body["userEmail"],
    // MessageGroupId: "Holdings",
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessCompanyLookup(identifier) {
  let queueUrl = process.env.AWS_SQS_URL_COMPANY_LOOKUP;

  let data = {
    identifier,
  };

  let params = {
    MessageAttributes: {
      identifier: {
        DataType: "String",
        StringValue: data.identifier,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${identifier}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessSecurityPrices(identifier) {
  let queueUrl = process.env.AWS_SQS_URL_SECURITY_PRICES;

  let data = {
    identifier,
  };

  let params = {
    MessageAttributes: {
      identifier: {
        DataType: "String",
        StringValue: data.identifier,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${identifier}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessSummaries(cik) {
  let queueUrl = process.env.AWS_SQS_URL_BILLIONAIRE_SUMMARIES;

  let data = {
    cik,
  };

  let params = {
    MessageAttributes: {
      cik: {
        DataType: "String",
        StringValue: data.cik,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${cik}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessNetWorth(id) {
  let queueUrl = process.env.AWS_SQS_URL_BILLIONAIRE_NETWORTH;

  let data = {
    id,
  };

  let params = {
    MessageAttributes: {
      id: {
        DataType: "String",
        StringValue: data.id,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${id}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessJsonMutualFunds(json, fundId, ticker) {
  let queueUrl = process.env.AWS_SQS_URL_MUTUAL_FUNDS_DAILY_PRICES;

  let data = {
    json,
    fundId,
    ticker,
  };

  let params = {
    MessageAttributes: {
      json: {
        DataType: "String",
        StringValue: data.json,
      },
      fundId: {
        DataType: "String",
        StringValue: data.fundId,
      },
      ticker: {
        DataType: "String",
        StringValue: data.ticker,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${ticker}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessMetricsCompanies(ticker, metrics) {
  let queueUrl = process.env.AWS_SQS_URL_COMPANIES_METRICS;

  let data = {
    ticker,
    metrics,
  };

  let params = {
    MessageAttributes: {
      ticker: {
        DataType: "String",
        StringValue: data.ticker,
      },
      metrics: {
        DataType: "String",
        StringValue: data.metrics,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${ticker}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_UpdateGlobalDashboard(widgetInstanceId) {
  let queueUrl = process.env.AWS_SQS_URL_GLOBAL_DASHBOARD;

  let data = {
    widgetInstanceId,
  };

  let params = {
    MessageAttributes: {
      widgetInstanceId: {
        DataType: "String",
        StringValue: data.widgetInstanceId,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${widgetInstanceId}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_UpdateLocalDashboards(widgetInstanceId) {
  let queueUrl = process.env.AWS_SQS_URL_LOCAL_DASHBOARDS;

  let data = {
    widgetInstanceId,
  };

  let params = {
    MessageAttributes: {
      widgetInstanceId: {
        DataType: "String",
        StringValue: data.widgetInstanceId,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${widgetInstanceId}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessJsonETFs(ticker) {
  let queueUrl = process.env.AWS_SQS_URL_ETF_DATA_COMPLIER;

  let data = {
    ticker,
  };

  let params = {
    MessageAttributes: {
      ticker: {
        DataType: "String",
        StringValue: data.ticker,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${ticker}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessCategorization(ticker, table) {
  let queueUrl = process.env.AWS_SQS_URL_SECURITY_CATEGORIZATION;

  let data = {
    ticker,
    table,
  };

  let params = {
    MessageAttributes: {
      ticker: {
        DataType: "String",
        StringValue: data.ticker,
      },
      table: {
        DataType: "String",
        StringValue: data.table,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${ticker}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

export function publish_ProcessInstitutionalHoldings(id) {
  let queueUrl = process.env.AWS_SQS_URL_INSTITUTIONAL_HOLDINGS;

  let data = {
    id,
  };

  let params = {
    MessageAttributes: {
      id: {
        DataType: "Number",
        StringValue: data.id,
      },
    },
    MessageBody: JSON.stringify(data),
    MessageDeduplicationId: `${id}-${queueUrl}`,
    MessageGroupId: this.constructor.name,
    QueueUrl: queueUrl,
  };

  // Send the order data to the SQS queue
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("error", err);
    } else {
      console.log("queue success =>", data.MessageId);
    }
  });
}

// AWS_SQS_URL_BILLIONAIRE_HOLDINGS (Individual)
export const consumer_1 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_BILLIONAIRE_HOLDINGS,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await institutions.backfillInstitution_Billionaire(
      sqsMessage.cik,
      Number(sqsMessage.id)
    );

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
export const consumer_2 = Consumer.create({
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
export const consumer_3 = Consumer.create({
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

// AWS_SQS_URL_SECURITY_PRICES (Individual)
export const consumer_4 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_SECURITY_PRICES,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await prices.fetchTape(sqsMessage.identifier);
  },
});

consumer_4.on("error", (err) => {
  console.error(err.message);
});

consumer_4.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_BILLIONAIRE_SUMMARIES
export const consumer_5 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_BILLIONAIRE_SUMMARIES,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await titans.generateSummary(sqsMessage.cik);
  },
});

consumer_5.on("error", (err) => {
  console.error(err.message);
});

consumer_5.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_BILLIONAIRE_NETWORTH
export const consumer_6 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_BILLIONAIRE_NETWORTH,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await titans.updateNetWorth(sqsMessage.id);
  },
});

consumer_6.on("error", (err) => {
  console.error(err.message);
});

consumer_6.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_MUTUAL_FUNDS_DAILY_PRICES
export const consumer_7 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_MUTUAL_FUNDS_DAILY_PRICES,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await mutualfunds.insertJsonMutualFund(
      "json",
      sqsMessage.json,
      sqsMessage.ticker
    );

    let fundSum = await mutualfunds.getJsonSumMutualFund(sqsMessage.fundId);
    let jsonSum = JSON.stringify(fundSum);

    await mutualfunds.insertJsonMutualFund(
      "json_summary",
      jsonSum,
      sqsMessage.ticker
    );

    let performance = await mutualfunds.getJsonPerformanceMutualFund(
      sqsMessage.ticker
    );

    await mutualfunds.insertJsonMutualFund(
      "json_performance",
      performance,
      sqsMessage.ticker
    );
  },
});

consumer_7.on("error", (err) => {
  console.error(err.message);
});

consumer_7.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_COMPANIES_METRICS
export const consumer_8 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_COMPANIES_METRICS,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await companies.insertMetricsCompany(sqsMessage.ticker, sqsMessage.metrics);
  },
});

consumer_8.on("error", (err) => {
  console.error(err.message);
});

consumer_8.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_GLOBAL_DASHBOARD
export const consumer_9 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_GLOBAL_DASHBOARD,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await widgets.processInput(sqsMessage.widgetInstanceId);
  },
});

consumer_9.on("error", (err) => {
  console.error(err.message);
});

consumer_9.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_LOCAL_DASHBOARDS
export const consumer_10 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_LOCAL_DASHBOARDS,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await widgets.processInput(sqsMessage.widgetInstanceId);
  },
});

consumer_10.on("error", (err) => {
  console.error(err.message);
});

consumer_10.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_ETFS_UPDATE
export const consumer_11 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_ETF_DATA_COMPLIER,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    let etfJson = await etfs.getJsonETF(sqsMessage.ticker);
    let etfStats = await etfs.getStatsETF(sqsMessage.ticker);
    let etfAnalytics = await etfs.getAnalyticsETF(sqsMessage.ticker);
    if (etfJson && etfStats && etfAnalytics) {
      let json = JSON.stringify(etfJson);
      let stats = JSON.stringify(etfStats);
      let analytics = JSON.stringify(etfAnalytics);
      if (json && stats && analytics) {
        await etfs.insertJsonETF(json, stats, analytics, sqsMessage.ticker);
      }
    }
  },
});

consumer_11.on("error", (err) => {
  console.error(err.message);
});

consumer_11.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_SECURITY_CATEGORIZATION
export const consumer_12 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_SECURITY_CATEGORIZATION,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await nlp.categorizeTicker(sqsMessage.ticker, sqsMessage.table);
  },
});

consumer_12.on("error", (err) => {
  console.error(err.message);
});

consumer_12.on("processing_error", (err) => {
  console.error(err.message);
});

// AWS_SQS_URL_INSTITUTIONAL_HOLDINGS
export const consumer_13 = Consumer.create({
  queueUrl: process.env.AWS_SQS_URL_INSTITUTIONAL_HOLDINGS,
  handleMessage: async (message) => {
    let sqsMessage = JSON.parse(message.Body);

    console.log(sqsMessage);

    await institutions.processHoldingsForInstitution(sqsMessage.id);
  },
});

consumer_13.on("error", (err) => {
  console.error(err.message);
});

consumer_13.on("processing_error", (err) => {
  console.error(err.message);
});
