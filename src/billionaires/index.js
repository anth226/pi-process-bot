const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");

import db from "../db";

import * as zip from "./zip";

(async () => {
  await zip.zipPerformances_Billionaires();
})();

async function import_Billionaires() {
  fs.createReadStream(path.join(__dirname, "billionaires.csv"))
    .pipe(csv())
    .on("data", async (row) => {
      if (
        row["Name"] != "" &&
        row["CIK"] != "" &&
        row["Personal Net Worth"] != "" &&
        row["Description"] != ""
      ) {
        console.log(row);
        console.log(
          row["Name"],
          row["CIK"],
          row["Personal Net Worth"],
          row["Description"]
        );

        let result = await db(`
        SELECT *
        FROM billionaires
        WHERE cik = '${row["CIK"]}'
      `);

        if (result) {
          let number = row["Personal Net Worth"];
          number = number.replace(/\D/g, "");

          if (result.length > 0) {
            let billionaire = result[0];

            let query = {
              text:
                "UPDATE billionaires SET name=($1), cik=($2), net_worth=($3), description=($4), institution_name=($5)  WHERE id=($6) RETURNING *",
              values: [
                row["Name"],
                row["CIK"],
                parseInt(number),
                row["Description"],
                row["Fund"],
                billionaire["id"],
              ],
            };

            result = await db(query);
          } else {
            let query = {
              text:
                "INSERT INTO billionaires (name, cik, net_worth, description, institution_name) VALUES ( $1, $2, $3, $4, $5 ) RETURNING *",
              values: [
                row["Name"],
                row["CIK"],
                parseInt(number),
                row["Description"],
                row["Fund"],
              ],
            };

            result = await db(query);
          }

          console.log(result);
        }
      }
    })
    .on("end", () => {
      console.log("CSV file successfully processed");
    });
}

// const createCsvWriter = require("csv-writer").createObjectCsvWriter;

// const csvWriter = createCsvWriter({
//   path: "report.csv",
//   header: [
//     { id: "name", title: "name" },
//     { id: "cik", title: "cik" }
//   ]
// });

// let portfolios = {
//   portfolios: []
// };

// let records = [];

// fs.readFile(`../src/components/titans/data/portfolios.json`, (err, data) => {
//   if (err) throw err;
//   portfolios = JSON.parse(data);

//   console.log(portfolios["portfolios"].length);

//   portfolios["portfolios"].forEach((portfolio, index) => {
//     records.push({
//       name: portfolio["name"],
//       cik: [portfolio["filer_cik"]]
//     });
//   });

//   csvWriter
//     .writeRecords(records)
//     .then(() => console.log("The CSV file was written successfully"));
// });
