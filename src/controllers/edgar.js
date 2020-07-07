import axios from "axios";

import db from "../db";

export async function lookupByName(name) {
  const url = "https://efts.sec.gov/LATEST/search-index";

  const data = { keysTyped: name };

  try {
    const response = await axios.post(url, data, {
      headers: {
        "Content-Type": "application/json",
      },
    });
    return { data: response.data };
  } catch (error) {
    return { data: null };
  }
}

export async function cache(billionaire) {
  let name = billionaire["name"];
  let titan_id = billionaire["id"];

  console.log(titan_id, name);

  let result = await db(`
    SELECT *
    FROM edgar_search_results
    WHERE titan_id = '${titan_id}'
  `);

  if (result) {
    if (result.length == 0) {
      result = await lookupByName(name);

      let query = {
        text:
          "INSERT INTO edgar_search_results (titan_id, query, json, updated_at) VALUES ( $1, $2, $3, now() ) RETURNING *",
        values: [titan_id, name, result],
      };

      await db(query);
    } else {
      result = await lookupByName(name);

      let query = {
        text:
          "UPDATE edgar_search_results SET query=($1), result=($2), updated_at=(now()) WHERE titan_id=($3) RETURNING *",
        values: [name, result, titan_id],
      };

      result = await db(query);
    }
  }
}
