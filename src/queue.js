const AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });
const sqs = new AWS.SQS({ apiVersion: "2012-11-05" });

// import * as holdings from "./controllers/holdings";

export function publish(job) {
  const params = {
    MessageBody: JSON.stringify({
      job,
      date: new Date().toISOString(),
    }),
    QueueUrl: process.env.AWS_SQS_URL,
    MessageGroupId: "0",
  };
  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("Error", err);
    } else {
      console.log("Successfully added message", data.MessageId);
    }
  });
}

// export function receive() {
//   let queueURL = process.env.AWS_SQS_URL;
//   var params = {
//     AttributeNames: ["SentTimestamp"],
//     MaxNumberOfMessages: 10,
//     MessageAttributeNames: ["All"],
//     QueueUrl: queueURL,
//     VisibilityTimeout: 20,
//     WaitTimeSeconds: 0,
//   };

//   sqs.receiveMessage(params, (err, data) => {
//     if (err) {
//       console.log("Receive Error", err);
//     } else if (data.Messages) {
//       var deleteParams = {
//         QueueUrl: queueURL,
//         ReceiptHandle: data.Messages[0].ReceiptHandle,
//       };

//       sqs.deleteMessage(deleteParams, async (err, data) => {
//         if (err) {
//           console.log("Delete Error", err);
//         } else {
//           console.log("Message Deleted", data);
//           await holdings.cacheHoldings_Titans();
//         }
//       });
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
        DataType: "Number",
        StringValue: data.id,
      },
      batchId: {
        DataType: "Number",
        StringValue: data.batchId,
      },
      cache: {
        DataType: "Boolean",
        StringValue: data.cache,
      },
    },
    MessageBody: JSON.stringify(data),
    // MessageDeduplicationId: req.body["userEmail"],
    MessageGroupId: "Holdings",
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
