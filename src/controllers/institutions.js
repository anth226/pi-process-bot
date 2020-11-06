import db from "../db";
import * as titans from "./titans";

import * as queue from "../queue";

import { getInstitutionalHoldings } from "../controllers/intrinio/get_institutional_holdings";

import { orderBy, find, sumBy } from "lodash";

export async function getInstitutions({
  sort = [],
  page = 0,
  size = 100,
  ...query
}) {
  return await db(`
    SELECT *
    FROM institutions
    ORDER BY id DESC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}

export const getInstitutionByCIK = async (cik) =>
  db(`
    SELECT *
    FROM institutions
    WHERE cik = '${cik}'
  `);

export async function getInstitutionsUpdated({
  sort = [],
  page = 0,
  size = 100,
  ...query
}) {
  return await db(`
    SELECT *
    FROM institutions
    WHERE updated_at IS NOT NULL
    ORDER BY cik DESC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}

export async function backfillInstitution_Billionaire(cik, id) {
  let institution = await getInstitutionByCIK(cik);

  if (institution[0]) {
    return;
  }

  let titan = await titans.getBillionaire(id);
  if (!titan) {
    return;
  }
  let { name } = titan;
  let query = {
    text:
      "INSERT INTO institutions (name, cik, updated_at) VALUES ( $1, $2, now() ) RETURNING *",
    values: [name, cik],
  };
  await db(query);
}

export async function fetchHoldings() {
  let result = await db(`
    SELECT *
    FROM institutions
    `);

  for (let i in result) {
    let { id } = result[i];
    await queue.publish_ProcessInstitutionalHoldings(id);
  }
}

export async function calculatePerformances() {
  let result = await db(`
    SELECT *
    FROM institutions
    `);

  for (let i in result) {
    let { cik } = result[i];
    if (cik) {
      await queue.publish_ProcessInstitutionalPerformance(cik);
    }
  }
}

export async function processHoldingsForInstitution(id) {
  let next_page = null;
  let holdings = [];
  let buffer = [];

  let result = await db(`
    SELECT *
    FROM institutions
    WHERE id = ${id}
    `);

  if (result.length == 0) {
    return;
  }

  let { cik } = result[0];

  do {
    let response = await getInstitutionalHoldings(cik, next_page);
    next_page = response["next_page"];

    holdings = response["holdings"];
    if (holdings) {
      buffer = buffer.concat(holdings);
    }

    //console.log(holdings.length);
  } while (next_page);

  let json = buffer.length > 0 ? JSON.stringify(buffer) : null;

  let query = {
    text: "SELECT * FROM institution_holdings WHERE institution_id = $1",
    values: [id],
  };
  result = await db(query);

  if (result.length > 0) {
    query = {
      text:
        "UPDATE institution_holdings SET json_holdings = $1, updated_at = now(), count = $2 WHERE institution_id = $3",
      values: [json, buffer ? buffer.length : 0, id],
    };
    await db(query);
  } else {
    query = {
      text:
        "INSERT INTO institution_holdings (json_holdings, updated_at, count, institution_id) VALUES ( $1, now(), $2, $3) RETURNING *",
      values: [json, buffer ? buffer.length : 0, id],
    };
    await db(query);
  }

  console.log(id + ": institution holdings updated");
}

export async function getInstitutionsHoldings(cik) {
  let result = await db(`
    SELECT i.*, i_h.*
    FROM institutions AS i
    LEFT JOIN institution_holdings AS i_h
    ON i.id = i_h.institution_id
    WHERE i.cik = '${cik}'
  `);

  if (result.length > 0) {
    let { json_holdings } = result[0];
    console.log("json_holdings", json_holdings);
    if (!json_holdings) {
      return null;
    }
    let filtered = json_holdings.filter((o) => {
      return o.shares_held != 0;
    });
    console.log("filtered", filtered);
    return filtered;
  }
  return null;
}

export async function processTop10andSectors(cik) {
  let jsonTop10;
  let jsonAllocations;
  let data = await getInstitutionsHoldings(cik);

  //
  // EVALUATE Allocations and top stocks
  //

  // Evaluate Top Stock
  let top = data ? await evaluateTopStocks(data) : null;

  if (top && top.length > 0) {
    jsonTop10 = JSON.stringify(top);
  }

  let query = {
    text:
      "UPDATE institutions SET json_top_10_holdings=($1), updated_at=now() WHERE cik=($2) RETURNING *",
    values: [jsonTop10, cik],
  };

  await db(query);

  // Calculate sectors
  let allocations = data ? await evaluateSectorCompositions(data) : null;

  if (allocations && allocations.length > 0) {
    jsonAllocations = JSON.stringify(allocations);
  }

  query = {
    text:
      "UPDATE institutions SET json_allocations=($1), updated_at=now() WHERE cik=($2) RETURNING *",
    values: [jsonAllocations, cik],
  };

  await db(query);

  //
  // EVALUATE Performances
  //

  // Calculate fund performances

  // // Calculate portfolio performance
  // await evaluateFundPerformace(cik);
}

const evaluateTopStocks = async (data) => {
  let total = sumBy(data, function (entry) {
    return entry["market_value"];
  });

  let sorted = orderBy(data, ["market_value"], ["desc"]);

  sorted.map((entry) => {
    entry.portfolio_share = (entry["market_value"] / total) * 100;
    return entry;
  });

  return sorted.slice(0, 10);
};

const evaluateSectorCompositions = async (data) => {
  //console.log("data", data);

  let tickers = data.map(({ company }) => {
    if (!company) {
      return;
    }
    return company["ticker"];
  });

  // console.log(tickers);

  let query = {
    text: "SELECT * FROM companies WHERE ticker = ANY($1::text[])",
    values: [tickers],
  };

  let result = await db(query);

  //console.log("result");
  //console.log(result);

  const merge = (companies, holdings) => {
    let merged = [];
    for (let i in companies) {
      let ticker = companies[i].ticker;
      console.log("companies[i]", companies[i]);
      for (let j in holdings) {
        console.log("holdings[j]", holdings[j]);
        let holdingTicker = holdings[j].ticker;
        if (ticker === holdingTicker) {
          console.log("MATCH");
          merged.push({
            company: companies[i],
            holding: holdings[j],
          });
        }
      }
    }
    console.log("1st merged", merged);
    if (merged && merged.length > 0) {
      return merged;
    } else {
      return null;
    }
  };

  // const mergeById = (a1, a2) =>
  //   a1.map((i1) => ({
  //     ...a2.find((i2) => {
  //       if (!i2.company) {
  //         return;
  //       }
  //       console.log("i1", i1);
  //       console.log("i2", i2);
  //       i2.company.ticker === i1.json.ticker && i2;
  //     }),
  //     ...i1,
  //   }));

  //let merged = mergeById(result, data);
  let merged = merge(result, data);

  //console.log("result", result);
  //console.log("data", data);
  console.log("merged", merged);

  // //

  //let sectors = merged.map(({ json }) => json["sector"]);
  //console.log(sectors);

  let buffer = {};
  let total = 0;

  for (let i = 0; i < merged.length; i += 1) {
    // if (counts.hasOwnProperty(key)) {
    //   counts[key] = (counts[key] / total) * 100;
    // }
    let sector = merged[i]["company"]["json"]["sector"];
    let market_value = merged[i]["holding"]["market_value"];

    console.log("merged[i]", merged[i]);

    console.log("market_value", market_value);

    buffer[`${sector}`] = buffer[`${sector}`]
      ? buffer[`${sector}`] + market_value
      : market_value;

    total += market_value;
  }
  // console.log("buffer");
  // console.log(buffer);
  // console.log("total");
  // console.log(total);

  for (let key in buffer) {
    if (buffer.hasOwnProperty(key)) {
      buffer[key] = (buffer[key] / total) * 100;
      // buffer[key] = buffer[key].toFixed(2);
    }
  }

  for (let key in buffer) {
    if (buffer.hasOwnProperty(key)) {
      if (key == "null" || key == null) {
        buffer["Other"] = buffer[key];
        delete buffer[key];
      }

      if (buffer[key] == NaN) {
        delete buffer[key];
      }

      if (buffer[key] == 0) {
        delete buffer[key];
      }
    }
  }

  let response = [];

  for (let key in buffer) {
    if (buffer.hasOwnProperty(key)) {
      response.push({
        sector: key,
        concentration: buffer[key],
      });
    }
  }

  let sorted = orderBy(response, ["concentration"], ["desc"]);

  console.log(sorted);

  return sorted;
};
