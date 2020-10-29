import "dotenv/config";
import db from "../db";

const AWS = require("aws-sdk");

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const uploadToS3 = async (bucket, key, data) => {
  let params = {
    Bucket: bucket,
    Key: key,
    Body: JSON.stringify(data),
    ContentType: "application/json",
    ACL: "public-read",
  };

  const response = await s3.upload(params).promise();

  //console.log(chalk.bgYellow("s3 =>"), response);

  return response;
};

export async function generate_Portfolios() {
  let n = 0;
  await uploadToS3(
    process.env.AWS_BUCKET_PAGES_INSTITUTIONS,
    `default/${n}.json`,
    []
  );
}
