import * as companies from "./controllers/companies";
import * as titans from "./controllers/titans";
import * as holdings from "./controllers/holdings";
import * as institutions from "./controllers/institutions";
import * as performances from "./controllers/performances";
import * as prices from "./controllers/prices";
import * as mutualfunds from "./controllers/mutualfunds";

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

export function publish_ProcessMutualFunds(id) {
  let queueUrl = process.env.AWS_SQS_URL_MUTUAL_FUNDS_DAILY_PRICES;

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

export const consumerURLS = [
  process.env.AWS_SQS_URL_BILLIONAIRE_HOLDINGS,
  process.env.AWS_SQS_URL_BILLIONAIRE_PERFORMANCES,
  process.env.AWS_SQS_URL_COMPANY_LOOKUP,
  process.env.AWS_SQS_URL_SECURITY_PRICES,
  process.env.AWS_SQS_URL_BILLIONAIRE_SUMMARIES,
  process.env.AWS_SQS_URL_BILLIONAIRE_NETWORTH,
  process.env.AWS_SQS_URL_MUTUAL_FUNDS_DAILY_PRICES,
];

export const consumerFunctions = [
  // consumer_1
  [
    institutions.backfillInstitution_Billionaire,
    holdings.fetchHoldings_Billionaire,
  ],
  // consumer_2
  [
    performances.calculatePerformance_Billionaire,
    titans.cacheCompanies_Portfolio,
  ],
  // consumer_3
  [companies.lookupCompany],
  // consumer_4
  [prices.fetchTape],
  // consumer_5
  [titans.generateSummary],
  // consumer_6
  [titans.updateNetWorth],
  // consumer_7
  [mutualfunds.insertMutualFund],
];

export async function runConsumerFunctions(conNum, sqsMessage) {
  let consumerFuncs = consumerFunctions[conNum];
  for (let i = 0; i < consumerFuncs.length; i++) {
    //console.log(consumerFuncs[i]);
    consumerFuncs[i](
      sqsMessage.cik,
      Number(sqsMessage.id),
      Number(sqsMessage.batchId),
      sqsMessage.cache,
      sqsMessage.identifier,
      sqsMessage.json,
      sqsMessage.ticker
    );
  }
}

export async function createConsumers() {
  let consumers = [];

  for (let i = 0; i < consumerFunctions.length; i++) {
    consumers[i] = Consumer.create({
      queueUrl: consumerURLS[i],
      handleMessage: async (message) => {
        let sqsMessage = JSON.parse(message.Body);

        console.log(sqsMessage);

        runConsumerFunctions(i, sqsMessage);
      },
    });
    consumers[i].on("error", (err) => {
      console.error(err.message);
    });
    consumers[i].on("processing_error", (err) => {
      console.error(err.message);
    });
  }
  return consumers;
}

export async function runConsumers() {
  let consumers = createConsumers();

  for (let i = 0; i < consumers.length; i++) {
    consumers[i].start();
  }
}
