import axios from "axios";
import db from "../db";
import * as queue from "../queue";
// import * as queue from "../queue2";

import * as holdings from "./holdings";

import { orderBy, find, sumBy, get } from "lodash";

import redis, { KEY_FORBES_TITANS } from "../redis";
// import { nullFormatter } from "../components/common/Table/helpers";
import * as performances from "./performances";
import * as securities from "./securities";
import * as institutions from "./institutions";
import {getEnv} from "../env";
import {getInstitutionsHoldings} from "./institutions";
import {getInstitutionLargestNewHolding} from "./institutions";

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
    } catch { }
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
  let data_url = `https://${getEnv("AWS_BUCKET_INTRINIO_ZAKS")}.s3.amazonaws.com/marketcaps/${cik}.json`;

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

export async function calculateFallbackPerformance_Billionaire(
  cik,
  billionaireId,
  batchId = null,
  cache = true
) {
  let frequencies = [
    // "daily",
    // "weekly",
    // "monthly",
    "quarterly",
    "yearly",
  ];

  let marketcaps = {};

  for (let i = 0; i < frequencies.length; i += 1) {
    let frequency = frequencies[i];

    let next_page = null;
    let historical_data = [];
    let buffer = {};

    do {
      let response = await performances.getHistoricalData(
        cik,
        frequency,
        next_page
      );
      next_page = response["next_page"];

      buffer = response["historical_data"];
      historical_data = historical_data.concat(buffer);
    } while (next_page);

    marketcaps[frequency] = historical_data;
  }

  let quarPerc = (
    (marketcaps.quarterly[0].value / marketcaps.quarterly[1].value - 1) *
    100
  ).toFixed(2);
  let yearPerc = (
    (marketcaps.yearly[0].value / marketcaps.yearly[1].value - 1) *
    100
  ).toFixed(2);
  let year5Perc = (
    (marketcaps.yearly[0].value / marketcaps.yearly[5].value - 1) *
    100
  ).toFixed(2);

  let perf = {
    performance_quarter: quarPerc,
    performance_one_year: yearPerc,
    performance_five_year: year5Perc,
  };

  //console.log(perf);

  return perf;

  //console.log("marketcaps", marketcaps);
}

export async function processHoldingsPerformanceAndSummary(id) {
  //get primary id
  let titan;
  let result = await getBillionaireCiks(id);
  if (result) {
    titan = result[0];
  }

  if (titan) {
    console.log("titan found");
    let ciks = titan.ciks;
    if (ciks && ciks.length > 0) {
      for (let j = 0; j < ciks.length; j += 1) {
        let cik = ciks[j];
        if (cik.cik != "0000000000" && cik.is_primary == true) {
          console.log("primary cik found");
          let { use_company_performance_fallback } = titan;
          if (use_company_performance_fallback) {
            console.log("fallback set");
            let perf = await calculateFallbackPerformance_Billionaire(cik.cik);
            let query = {
              text:
                "UPDATE companies SET json_calculations=($1) WHERE cik=($2) RETURNING *",
              values: [perf, cik.cik],
            };
            await db(query);
            console.log("json_calcs updated");
          } else {
            // find batch_id
            result = await db(`
              SELECT *
              FROM holdings
              ORDER BY batch_id DESC
              LIMIT 1
            `);

            let batchId = 0;
            if (result.length > 0) {
              batchId = Number(result[0]["batch_id"]) + 1;
            }

            let cache = true;
            await queue.publish_ProcessHoldings(cik.cik, id, batchId, cache);
            //institutions.backfillInstitution_Billionaire
            //  cik, id
            //holdings.fetchHoldings_Billionaire
            //  cik, id, batchId, cache
            setTimeout(function () {
              queue.publish_ProcessPerformances(cik.cik, id, batchId, cache);
            }, 10000);

            //performances.calculatePerformance_Billionaire
            //  cik, id, batchId, cache
            //titans.cacheCompanies_Portfolio
            //  cik

            setTimeout(function () {
              queue.publish_ProcessSummaries(cik.cik);
            }, 10000);

            //titans.generateSummary
            //  cik
          }
        }
      }
    }
  }
}

export async function getTitanLargestHolding(id) {
  let holdingList = [];
  let data = await getTitanHoldings(id);
  let newestDate = data[0].as_of_date;
  for (let i in data) {
    let ticker = data[i].company.ticker;
    let openDate = data[i].as_of_date;
    let marketValue = data[i].market_value;
    if (ticker && openDate == newestDate && marketValue && marketValue > 0) {
      holdingList.push(data[i]);
    }
  }
  holdingList.sort((a, b) => a["market_value"] - b["market_value"]);
  let topStock = holdingList.pop();
  if (topStock) {
    return topStock;
  }
}

export async function getTitanTopHolding(id, sort, direction) {
  let tickerList = [];
  let data = await getTitanHoldings(id);
  for (let i in data) {
    let ticker = data[i].company.ticker;
    tickerList.push(ticker);
  }
  let tickers = await tickerList.map((x) => "'" + x + "'").toString();
  let secs = await securities.getSecuritiesByTickers(tickers);
  if (direction == "asc") {
    secs.sort((a, b) => a[sort] - b[sort]);
  } else {
    secs.sort((a, b) => b[sort] - a[sort]);
  }
  let topStock = secs.pop();
  if (topStock) {
    let topTicker = topStock.ticker;
    for (let i in data) {
      let ticker = data[i].company.ticker;
      if (topTicker == ticker) {
        return data[i];
      }
    }
  }
}

export async function getTitanHoldings(titanId) {
  let primaryCik;
  console.log("titanId", titanId);
  let titan = await getBillionaireCiks(titanId);
  console.log("titan", titan);
  if (titan.length > 0) {
    let ciks = titan[0].ciks;
    for (let i in ciks) {
      let cik = ciks[i].cik;
      let isPrimary = ciks[i].is_primary;
      if (cik != "0000000000" && isPrimary) {
        primaryCik = cik;
      }
    }
  }
  console.log("primaryCik", primaryCik);
  if (primaryCik) {
    //get holds from intrinio as backup
    //https://api-v2.intrinio.com/zacks/institutional_holdings?api_key=OjljMjViZjQzNWU4NGExZWZlZTFmNTY4ZDU5ZmI5ZDI0&owner_cik=0001061165
    //need to account for next page batches
    let holds = await institutions.getInstitutionsHoldings(primaryCik);
    console.log("holds", holds);
    if (holds && holds.length > 0) {
      return holds;
    }
  }
}

export async function calculateHoldingPrice(holding) {
  let shares = holding.amount;
  let marketValue = holding.value;
  if (shares > 0 && marketValue > 0) {
    return marketValue / shares;
  } else {
    console.log("Calculate holding price failed... printing values");
    console.log("holding: ", holding);
    console.log("shares: ", shares);
    console.log("marketValue: ", marketValue);
    console.log("shares: ", shares);
  }
}

export async function getTitanSnapshot(id) {
  let data = await getTitanHoldings(id);
  if (!data) {
    return null;
  }

  let topPerf = await getTitanTopHolding(
    id,
    "price_percent_change_1_year",
    "asc"
  );
  let common = await getTitanTopHolding(
    id,
    "institutional_holdings_count",
    "asc"
  );
  let uncommon = await getTitanTopHolding(
    id,
    "institutional_holdings_count",
    "desc"
  );
  let largest = await getTitanLargestHolding(id);

  // console.log("topPerf", topPerf);
  // console.log("common", common);
  // console.log("uncommon", uncommon);
  // console.log("largest", largest);

  try {
    if (topPerf && common && uncommon && largest) {
      let topPerfPrice = await calculateHoldingPrice(topPerf);
      let commonPrice = await calculateHoldingPrice(common);
      let uncommonPrice = await calculateHoldingPrice(uncommon);
      let largestPrice = await calculateHoldingPrice(largest);
      let topPerfSec = await securities.getSecurityByTicker(
        topPerf.company.ticker
      );
      let commonSec = await securities.getSecurityByTicker(common.company.ticker);
      let uncommonSec = await securities.getSecurityByTicker(
        uncommon.company.ticker
      );
      let largestSec = await securities.getSecurityByTicker(
        largest.company.ticker
      );

      // console.log("topPerfPrice", topPerfPrice);
      // console.log("commonPrice", commonPrice);
      // console.log("uncommonPrice", uncommonPrice);
      // console.log("largestPrice", largestPrice);

      return {
        top_performing: {
          ticker: get(topPerf, "company.ticker") ? topPerf.company.ticker : null,
          name: get(topPerf, "company.name") ? topPerf.company.name : null,
          open_date: get(topPerf, "as_of_date") ? topPerf.as_of_date : null,
          open_price: topPerfPrice ? topPerfPrice : null,
          price_percent_change_1_year: get(topPerfSec, "price_percent_change_1_year") ? topPerfSec.price_percent_change_1_year : null,
        },
        common: {
          ticker: get(common, "company.ticker") ? common.company.ticker : null,
          name: get(common, "company.name") ? common.company.name : null,
          open_date: get(common, "as_of_date") ? common.as_of_date : null,
          open_price: commonPrice ? commonPrice : null,
          price_percent_change_1_year: get(commonSec, "price_percent_change_1_year") ? commonSec.price_percent_change_1_year : null,
        },
        uncommon: {
          ticker: get(uncommon, "company.ticker") ? uncommon.company.ticker : null,
          name: get(uncommon, "company.name") ? uncommon.company.name : null,
          open_date: get(uncommon, "as_of_date") ? uncommon.as_of_date : null,
          open_price: uncommonPrice ? uncommonPrice : null,
          price_percent_change_1_year: get(uncommonSec, "price_percent_change_1_year") ? uncommonSec.price_percent_change_1_year : null,
        },
        largest: {
          ticker: get(largest, "company.ticker") ? largest.company.ticker : null,
          name: get(largest, "company.name") ? largest.company.name : null,
          open_date: get(largest, "as_of_date") ? largest.as_of_date : null,
          open_price: largestPrice ? largestPrice : null,
          price_percent_change_1_year: get(largestSec, "price_percent_change_1_year") ? largestSec.price_percent_change_1_year : null,
        },
      };
    }
  } catch (error) {
    console.log("--------------------Titan Snaphot Error------------------------");
    console.error(error)
  }
}

export async function insertSnapshotTitan(id, snapshot) {
  if (!id || !snapshot) {
    return;
  }

  console.log("snapshot json insert", snapshot);

  let query = {
    text: "SELECT * FROM billionaires WHERE id = $1",
    values: [id],
  };
  let result = await db(query);

  console.log("result", result);

  if (result.length > 0) {
    console.log("in update");
    let query = {
      text: "UPDATE billionaires SET json_snapshot = $2 WHERE id = $1",
      values: [id, snapshot],
    };
    await db(query);
    console.log("updated");
  }
}

export async function processTitansSnapshots() {
  let result = await getBillionaires({ size: 5000 });
  if (result.length > 0) {
    for (let i in result) {
      let id = result[i].id;
      await queue.publish_ProcessSnapshot_Titans(id);
    }
  }
}
