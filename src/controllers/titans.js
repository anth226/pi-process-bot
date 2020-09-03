import axios from "axios";
import db from "../db";
import * as queue from "../queue";
// import * as queue from "../queue2";

import * as holdings from "./holdings";

import { orderBy, find, sumBy } from "lodash";

import redis, { KEY_FORBES_TITANS } from "../redis";
import { nullFormatter } from "../components/common/Table/helpers";

export async function getTitans({ sort = [], page = 0, size = 100, ...query }) {
  return await db(`
    SELECT *
    FROM billionaires
    ORDER BY id ASC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}

export async function getBillionaires({
  sort = [],
  page = 0,
  size = 250,
  ...query
}) {
  return await db(`
    SELECT *
    FROM billionaires
    ORDER BY id ASC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}

export async function getBillionaire(id) {
  let result = await db(`
    SELECT *
    FROM billionaires
    WHERE ${id} = id
  `);
  return result[0];
}

export async function getBillionaireURI(id) {
  let result = await db(`
    SELECT uri
    FROM billionaires
    WHERE ${id} = id
  `);
  return result[0].uri;
}

export async function getBillionairesCiks({
  sort = [],
  page = 0,
  size = 250,
  ...query
}) {
  return await db(`
    SELECT b.*, b_c.ciks
    FROM billionaires AS b
    LEFT JOIN (
      SELECT titan_id, json_agg(json_build_object('cik', cik, 'name', name, 'is_primary', is_primary) ORDER BY rank ASC) AS ciks
      FROM billionaire_ciks
      GROUP BY titan_id
    ) AS b_c ON b.id = b_c.titan_id
    ORDER BY b.id ASC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}

export async function getBillionaireCiks(id) {
  return await db(`
    SELECT b.*, b_c.ciks
    FROM billionaires AS b
    LEFT JOIN (
      SELECT titan_id, json_agg(json_build_object('cik', cik, 'name', name, 'is_primary', is_primary) ORDER BY rank ASC) AS ciks
      FROM billionaire_ciks
      GROUP BY titan_id
    ) AS b_c ON b.id = b_c.titan_id
    WHERE b.id = ${id}
  `);
}

export async function getBillionairesCiksAndNotes({
  sort = [],
  page = 0,
  size = 250,
  ...query
}) {
  return await db(`
    SELECT b.name, b.net_worth, b.uri, b.updated_at, b.industry, b.id, b_c.ciks, b_n.notes
    FROM billionaires AS b
    LEFT JOIN (
      SELECT titan_id, json_agg(json_build_object('cik', cik, 'name', name, 'is_primary', is_primary) ORDER BY rank ASC) AS ciks
      FROM billionaire_ciks
      GROUP BY titan_id
    ) AS b_c ON b.id = b_c.titan_id
    LEFT JOIN (
      SELECT billionaire_id, json_agg(json_build_object('note', note, 'created_at', created_at) ORDER BY created_at DESC) AS notes
      FROM billionaire_notes
      GROUP BY billionaire_id
    ) AS b_n ON b.id = b_n.billionaire_id
      ORDER BY b.id ASC
      LIMIT ${size}
      OFFSET ${page * size}
  `);
}

export async function getBillionaires_Complete({
  sort = [],
  page = 0,
  size = 250,
  ...query
}) {
  return await db(`
    SELECT *
    FROM billionaires
    WHERE status='complete'
    ORDER BY id ASC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}

export async function cacheCompanies_Portfolio(cik) {
  let result = await db(`
    SELECT *
    FROM holdings
    WHERE cik = '${cik}'
    ORDER BY batch_id DESC
    LIMIT 1
  `);

  if (result.length > 0) {
    let { data_url } = result[0];
    try {
      let result = await axios.get(data_url, {
        crossdomain: true,
        withCredentials: false,
        headers: {
          "Content-Type": "application/json",
          // "Access-Control-Allow-Origin": "*"
        },
      });

      let holdings = result.data;

      holdings.forEach((holding) => {
        let {
          company: { ticker },
        } = holding;
        console.log(ticker);

        queue.publish_ProcessCompanyLookup(ticker);
        queue.publish_ProcessSecurityPrices(ticker);
      });
    } catch {}
  }
}

export async function generateSummary(cik) {
  console.log(cik);

  let data = await holdings.fetchAll(cik);
  let filtered = data.filter((o) => {
    return o.shares_held != 0;
  });

  //
  // EVALUATE Allocations and top stocks
  //

  // Evaluate Top Stock
  let top = await evaluateTopStocks(filtered);

  let query = {
    text:
      "UPDATE institutions SET json_top_10_holdings=($1), updated_at=now() WHERE cik=($2) RETURNING *",
    values: [top.length > 0 ? { top } : null, cik],
  };

  await db(query);

  // Calculate sectors
  let allocations = await evaluateSectorCompositions(filtered);

  query = {
    text:
      "UPDATE institutions SET json_allocations=($1), updated_at=now() WHERE cik=($2) RETURNING *",
    values: [allocations.length > 0 ? { allocations } : null, cik],
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
  // console.log(data);

  let tickers = data.map(({ company }) => company["ticker"]);

  // console.log(tickers);

  let query = {
    text: "SELECT * FROM companies WHERE ticker = ANY($1::text[])",
    values: [tickers],
  };

  let result = await db(query);

  // console.log(result);

  const mergeById = (a1, a2) =>
    a1.map((i1) => ({
      ...a2.find((i2) => i2.company.ticker === i1.ticker && i2),
      ...i1,
    }));

  let merged = mergeById(result, data);

  // console.log(merged);

  // //

  let sectors = merged.map(({ json }) => json["sector"]);
  // console.log(sectors);

  let buffer = {};
  let total = 0;

  for (let i = 0; i < merged.length; i += 1) {
    // if (counts.hasOwnProperty(key)) {
    //   counts[key] = (counts[key] / total) * 100;
    // }
    let sector = merged[i]["json"]["sector"];
    let market_value = merged[i]["market_value"];

    buffer[`${sector}`] = buffer[`${sector}`]
      ? buffer[`${sector}`] + market_value
      : market_value;

    total += market_value;
  }

  // console.log(buffer);
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

const evaluateFundPerformace = async (cik) => {
  let data_url = `https://intrinio-zaks.s3.amazonaws.com/marketcaps/${cik}.json`;

  try {
    let result = await axios.get(data_url, {
      crossdomain: true,
      withCredentials: false,
      headers: {
        "Content-Type": "application/json",
        // "Access-Control-Allow-Origin": "*"
      },
    });

    let marketcaps = result.data;
    // console.log(marketcaps);

    const findChange = (values, offset) => {
      try {
        let current = {};
        let previous = {};
        let index_current = -1;
        let index_previous = -1;

        for (let i = 0; i < values.length; i += 1) {
          let item = values[i];

          if (item["value"]) {
            index_current = i;
            index_previous = i + offset;
            break;
          }
        }

        current = values[index_current];
        previous = values[index_previous];

        let delta = current["value"] - previous["value"];

        // % increase = Increase ÷ Original Number × 100.

        let percent_change = (delta / current["value"]) * 100;

        return percent_change;
      } catch (e) {
        // console.error(e);
        return null;
      }
    };

    let change = 0;
    let quarterly = marketcaps["quarterly"];

    change = findChange(quarterly, 1);

    console.log("1 quarter", change);
    let yearly = marketcaps["yearly"];

    change = findChange(yearly, 1);
    console.log("1 year", change);

    change = findChange(yearly, 5);
    console.log("5 year", change);
  } catch (e) {
    console.error(e);
  }
};

export async function fetchBillionaireList() {
  let cache = await redis.get(`${KEY_FORBES_TITANS}`);

  if (!cache) {
    let billionaires = [];

    try {
      let url = `https://www.forbes.com/ajax/list/data?year=2020&uri=billionaires&type=person`;
      const result = await axios.get(url);
      billionaires = result.data;
    } catch (e) {
      console.error(e);
    }

    redis.set(
      `${KEY_FORBES_TITANS}`,
      JSON.stringify(billionaires),
      "EX",
      60 * 30 // 30 Minutes
    );
    return billionaires;
  } else {
    return JSON.parse(cache);
  }
}

export async function getNetworth(list, uri) {
  let value;

  //   try {
  //     let url = `https://www.forbes.com/profile/${uri}`;
  //     const result = await axios.get(url);
  //     const $ = await cheerio.load(result.data);
  //     value = $("div.profile-info__item-value").text();
  //   } catch (e) {
  //     console.log(e);
  //   }

  //   console.log("value", value);

  let billionaire = await find(list, { uri });

  console.log(billionaire);

  if (!billionaire) {
    return;
  }

  value = billionaire["realTimeWorth"];
  value = Math.round(value * 1000000);
  return value;
}

export async function updateNetWorth(id) {
  console.log(id);

  // Grab full list of billionaires from forbes
  let data = await fetchBillionaireList();
  //console.log("UPDATE NET WORTH");
  //console.log(data);

  // Get URI from billionaires table based on primary cik
  let uri = await getBillionaireURI(id);

  // Get Net Worth, Don't update missing entries
  let netWorth = await getNetworth(data, uri);
  if (!netWorth) {
    return;
  }

  // Update billionaires table
  let query = {
    text:
      "UPDATE billionaires SET net_worth=($1), updated_at=now() WHERE id=($2) RETURNING *",
    values: [netWorth, id],
  };

  await db(query);
}

export async function processHoldingsPerformanceAndSummary(id) {
  //get primary id
  let titan;
  let hadPrimary = false;
  let result = await getBillionaireCiks(id);
  if (result) {
    titan = result[0];
  }

  if (titan) {
    let ciks = titan.ciks;
    if (ciks && ciks.length > 0) {
      for (let j = 0; j < ciks.length; j += 1) {
        let cik = ciks[j];
        if (cik.cik != "0000000000" && cik.is_primary == true) {
          //console.log(cik.cik);
          hadPrimary = true;
          let batchId = 0;
          let cache = true;

          await queue.publish_ProcessHoldings(cik.cik, id, batchId, cache);
          //institutions.backfillInstitution_Billionaire
          //  cik, id
          //holdings.fetchHoldings_Billionaire
          //  cik, id, batchId, cache
          await queue.publish_ProcessPerformances(cik.cik, id, batchId, cache);
          //performances.calculatePerformance_Billionaire
          //  cik, id, batchId, cache
          //titans.cacheCompanies_Portfolio
          //  cik
          await queue.publish_ProcessSummaries(cik.cik);
          //titans.generateSummary
          //  cik
        }
      }
    }
    if (hadPrimary != true) {
      //check that other thing
    }
  }
}
