const AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });
const sqs = new AWS.SQS({ apiVersion: "2012-11-05" });

// export function publish(job) {
//   const params = {
//     MessageBody: JSON.stringify({
//       job,
//       date: new Date().toISOString(),
//     }),
//     QueueUrl: process.env.AWS_SQS_URL,
//     MessageGroupId: "0",
//   };
//   sqs.sendMessage(params, (err, data) => {
//     if (err) {
//       console.log("Error", err);
//     } else {
//       console.log("Successfully added message", data.MessageId);
//     }
//   });
// }

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
