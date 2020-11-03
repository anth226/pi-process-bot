import "dotenv/config";
import db from "../db";
import { orderBy } from "lodash";

export async function parseHoldings_Portfolios() {
  let result = await db(`
        SELECT id
        FROM institutions
    `);

  let ids = result.map((record) => record.id);

  let query = {
    text:
      "SELECT id, json_array_length(json_holdings) AS count, institution_id FROM institution_holdings WHERE institution_id = ANY($1::int[])",
    values: [ids],
  };

  result = await db(query);

  let sorted = orderBy(result, ["count"], ["desc"]);

  const hasCount = sorted.filter((x) => x.count !== null);

  const nullCount = sorted.filter((x) => x.count === null);

  ids = hasCount.map((record) => record.institution_id);

  //   query = {
  //     text: `SELECT i.*, i_h.*
  //       FROM institutions AS i
  //       LEFT JOIN institution_holdings AS i_h
  //       ON i.id = i_h.institution_id
  //       WHERE i.id = ANY($1::int[]) AND i.json is not null
  //       ORDER BY array_position($1::bigint[], i.id)`,
  //     values: [ids],
  //   };

  query = {
    text: `SELECT *
      FROM institutions
      WHERE id = ANY($1::int[]) AND json is not null
      ORDER BY array_position($1::bigint[], id)`,
    values: [ids],
  };

  result = await db(query);

  let pages = chunk(result, 96);

  for (let n = 0; n < pages.length; n++) {
    console.log(n);

    try {
      await uploadToS3(
        process.env.AWS_BUCKET_PAGES_INSTITUTIONS,
        `default/${n + 1}.json`,
        pages[n]
      );
    } catch (error) {
      console.log(error);
      console.log(pages[n]);
    }
  }

  query = {
    text: `UPDATE pages_institutions SET updated_at=now(), page_count=($1) WHERE id = 1`,
    values: [pages.length],
  };

  result = await db(query);
}

function chunk(arr, len) {
  var chunks = [],
    i = 0,
    n = arr.length;

  while (i < n) {
    chunks.push(arr.slice(i, (i += len)));
  }

  return chunks;
}

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
