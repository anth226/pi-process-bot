import db from "../db";
import * as titans from "./titans";
import * as securities from "./securities";

import * as queue from "../queue";

import { getInstitutionalHoldings } from "../controllers/intrinio/get_institutional_holdings";
import { getCikHoldingsFromIntrinio } from "../controllers/intrinio/get_cik_holdings";

import { orderBy, sumBy, get, size, map, groupBy } from "lodash";

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

export async function getCikHoldings() {
  return await db(`
    SELECT id, cik
    FROM cik_holdings
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
      "INSERT INTO institutions (name, cik, updated_at, is_institution) VALUES ( $1, $2, now(), false ) RETURNING *",
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

// deprecated
// TODO: Remove process holdings for institutions function, sqs, and lambda
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
    if (json) {
      query = {
        text:
          "UPDATE institution_holdings SET json_holdings = $1, updated_at = now(), count = $2 WHERE institution_id = $3",
        values: [json, buffer ? buffer.length : 0, id],
      };
      await db(query);
    }
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
    SELECT json_holdings
    FROM cik_holdings
    WHERE cik = '${cik}'
  `);

  if (result && result.length > 0) {
    let { json_holdings } = result[0];
    if (!json_holdings) {
      console.log("No json holdings for this institution: ", cik);
      return null;
    }
    return json_holdings;
  }
  console.log("Found no results from query (getInstitutionsHoldings)");
  return null;
}

// deprecated
export async function getInstitutionHoldingsCount() {
  let holdingsCount = new Map();
  let ints = await db(`
    SELECT *
    FROM institution_holdings
    WHERE json_holdings IS NOT NULL
    ORDER BY id DESC
  `);
  if (ints.length > 0) {
    for (let i in ints) {
      //console.log("holdings[" + i + "]", holdings[i]);
      if (ints[i].json_holdings) {
        let holdings = ints[i].json_holdings;
        for (let j in holdings) {
          let ticker = holdings[j].company.ticker;
          let sharesHeld = holdings[j].shares_held;
          if (sharesHeld && sharesHeld > 0) {
            if (holdingsCount.has(ticker)) {
              let count = holdingsCount.get(ticker);
              count++;
              holdingsCount.set(ticker, count);
            } else {
              holdingsCount.set(ticker, 1);
            }
            // console.log("ticker:", ticker);
            // console.log("count:", holdingsCount.get(ticker));
          }
        }
      }
    }
    return holdingsCount;
  }
}

export async function evaluateTop10() {
  let result = await db(`
    SELECT *
    FROM institutions
    WHERE is_institution = true
    `);

  for (let i in result) {
    let { id } = result[i];
    await queue.publish_ProcessTop10_Institutions(id);
  }
}

export async function evaluateAllocations() {
  let result = await db(`
    SELECT *
    FROM institutions
    WHERE is_institution = true
    `);

  for (let i in result) {
    let { id } = result[i];
    await queue.publish_ProcessAllocations_Institutions(id);
  }
}

// deprecated
export async function processTop10(id) {
  let jsonTop10;

  let result = await db(`
    SELECT *
    FROM institutions
    WHERE id = ${id}
  `);

  if (result.length > 0) {
    let { cik } = result[0];

    let data = await getInstitutionsHoldings(cik);

    let top = data ? await evaluateTopStocks(data) : null;

    if (top && top.length > 0) {
      jsonTop10 = JSON.stringify(top);

      let query = {
        text:
          "UPDATE institutions SET json_top_10_holdings=($1), updated_at=now() WHERE cik=($2) RETURNING *",
        values: [jsonTop10, cik],
      };

      await db(query);
    }
  }
}

export async function processSectors(id) {
  let jsonAllocations;

  let result = await db(`
    SELECT *
    FROM institutions
    WHERE id = ${id}
  `);

  if (result.length > 0) {
    let { cik } = result[0];

    let data = await getInstitutionsHoldings(cik);

    // Calculate sectors
    let allocations = data ? await evaluateSectorCompositions(data) : null;

    if (allocations && allocations.length > 0) {
      jsonAllocations = JSON.stringify(allocations);

      let query = {
        text:
          "UPDATE institutions SET json_allocations=($1), updated_at=now() WHERE cik=($2) RETURNING *",
        values: [jsonAllocations, cik],
      };

      await db(query);
    }
  }
}

//  Helper Functions

export const evaluateTopStocks = async (data) => {
  let sorted = orderBy(data, ["portfolio_percent"], ["desc"]);
  return sorted.slice(0, 10);
};

export const evaluateSectorCompositions = async (data) => {
  let tickers = data.map(({ ticker }) => {
    return ticker;
  });

  let query = {
    text: "SELECT * FROM companies WHERE ticker = ANY($1::text[])",
    values: [tickers],
  };

  let result = await db(query);

  const merge = (companies, holdings) => {
    let merged = [];
    for (let i in companies) {
      let ticker = companies[i].ticker;
      for (let j in holdings) {
        let holdingTicker = holdings[j].ticker;
        if (ticker === holdingTicker) {
          merged.push({
            company: companies[i],
            holding: holdings[j],
          });
        }
      }
    }
    if (merged && merged.length > 0) {
      return merged;
    } else {
      return null;
    }
  };

  let merged = merge(result, data);

  if (!merged) {
    return null;
  }

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
    let market_value = merged[i]["holding"]["value"];

    buffer[`${sector}`] = buffer[`${sector}`]
      ? buffer[`${sector}`] + market_value
      : market_value;

    total += market_value;
  }

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

export async function getSnapshotByCik(cik, data) {
  console.log("Snapshots for CIK: ", cik);
  console.log("--------------------start------------------------");

  console.log("Top perf started");
  let topPerf = await getSecuritiesBySort(
    "price_percent_change_1_year",
    "asc",
    data
  );
  console.log(topPerf);
  console.log("Top perf ended");

  console.log("common started");
  let common = await getSecuritiesBySort(
    "institutional_holdings_count",
    "asc",
    data
  );
  console.log(common);
  console.log("common ended");


  console.log("uncommon started");
  let uncommon = await getSecuritiesBySort(
    "institutional_holdings_count",
    "desc",
    data
  );
  console.log(uncommon);
  console.log("uncommon ended");


  console.log("largest started");
  let largest = await getInstitutionLargestNewHolding(data, cik);
  console.log(largest);
  console.log("largest ended");

  // console.log("topPerf", topPerf);
  // console.log("common", common);
  try {
    if (topPerf && common && uncommon && largest) {
      let topPerfPrice = await titans.calculateHoldingPrice(topPerf);
      let topPerfSec = await securities.getSecurityByTicker(topPerf.ticker);
      let commonPrice = await titans.calculateHoldingPrice(common);
      let commonSec = await securities.getSecurityByTicker(common.ticker);
      let uncommonPrice = await titans.calculateHoldingPrice(uncommon);
      let uncommonSec = await securities.getSecurityByTicker(uncommon.ticker);
      let largestPrice = await titans.calculateHoldingPrice(largest);
      let largestSec = await securities.getSecurityByTicker(largest.ticker);

      // console.log("topPerfPrice", topPerfPrice);
      // console.log("topPerfSec", topPerfSec);
      // console.log("commonPrice", commonPrice);
      // console.log("commonSec", commonSec);
      // console.log("uncommonPrice", uncommonPrice);
      // console.log("uncommonSec", uncommonSec);
      // console.log("largestPrice", largestPrice);
      // console.log("largestSec", largestSec);

      return {
        top_performing: {
          ticker: get(topPerf, "ticker") ? topPerf.ticker : null,
          name: get(topPerf, "security_name") ? topPerf.security_name : null,
          open_date: get(topPerf, "filing_date").substring(0,10) ? topPerf.filing_date.substring(0,10) : null,
          open_price: topPerfPrice ? topPerfPrice : null,
          price_percent_change_1_year: get(topPerfSec, "price_percent_change_1_year") ? topPerfSec.price_percent_change_1_year : null,
        },
        common: {
          ticker: get(common, "ticker") ? common.ticker : null,
          name: get(common, "security_name") ? common.security_name : null,
          open_date: get(common, "filing_date").substring(0,10) ? common.filing_date.substring(0,10) : null,
          open_price: commonPrice ? commonPrice : null,
          price_percent_change_1_year: get(commonSec, "price_percent_change_1_year") ? commonSec.price_percent_change_1_year : null,
        },
        uncommon: {
          ticker: get(uncommon, "ticker") ? uncommon.ticker : null,
          name: get(uncommon, "security_name") ? uncommon.security_name : null,
          open_date: get(uncommon, "filing_date").substring(0,10) ? uncommon.filing_date.substring(0,10) : null,
          open_price: uncommonPrice ? uncommonPrice : null,
          price_percent_change_1_year: get(uncommonSec, "price_percent_change_1_year") ? uncommonSec.price_percent_change_1_year : null,
        },
        largest: {
          ticker: get(largest, "ticker") ? largest.ticker : null,
          name: get(largest, "security_name") ? largest.security_name : null,
          open_date: get(largest, "filing_date").substring(0,10) ? largest.filing_date.substring(0,10) : null,
          open_price: largestPrice ? largestPrice : null,
          price_percent_change_1_year: get(largestSec, "price_percent_change_1_year") ? largestSec.price_percent_change_1_year : null,
        },
      };
    }
  } catch (error) {
    console.log("--------------------CIk Snapshot Error------------------------");
    console.error(error)
  }
}

const getSecuritiesBySort = async (sort, direction, data) => {
  console.log("starting securities by sort");
  let tickerList = [];
  for (let i in data) {
    let ticker = data[i].ticker;
    tickerList.push(ticker);
  }

  console.log("Got tickers: ", tickerList.length);

  let tickers = await tickerList.map((x) => "'" + x + "'").toString();

  console.log("Tickers string: ", tickers.length);

  let secs = await securities.getSecuritiesByTickers(tickers);

  if (secs) {
    if (direction == "asc") {
      secs.sort((a, b) => a[sort] - b[sort]);
    } else {
      secs.sort((a, b) => b[sort] - a[sort]);
    }

    let topStock = secs.pop();
    if (topStock) {
      let topTicker = topStock.ticker;
      for (let i in data) {
        let ticker = data[i].ticker;
        if (topTicker == ticker) {
          return data[i];
        }
        if (i >= (data.length - 1)) {
          console.log("Could not find the company")
        }
      }
    } else {
      console.log("There is no last item");
    }
  } else {
    console.log("There are no securities");
  }
}

export async function insertSnapshotCik(cik, snapshot) {
  if (!cik || !snapshot) {
    return;
  }
  console.log("snapshot json insert", snapshot);

  let query = {
    text: "UPDATE cik_holdings SET json_snapshot = $2, updated_at = now() WHERE cik = $1",
    values: [cik, snapshot],
  };
  await db(query);
}

export async function insertTop10Cik(cik, top10) {
  if (!cik || !top10) {
    return;
  }
  console.log("top10 json insert", top10);

  let query = {
    text: "UPDATE cik_holdings SET json_top_10 = $2, updated_at = now() WHERE cik = $1",
    values: [cik, top10],
  };
  await db(query);
}


export async function insertAllocationsCik(cik, allocations) {
  if (!cik || !allocations) {
    return;
  }
  console.log("allocation json insert", allocations);

  let query = {
    text: "UPDATE cik_holdings SET json_allocations = $2, updated_at = now() WHERE cik = $1",
    values: [cik, allocations],
  };
  await db(query);
}


export async function getInstitutionLargestHolding(data) {
  let holdingList = [];
  let hList = [];
  let newestDate = data[0].as_of_date;
  for (let i in data) {
    let ticker = data[i].company.ticker;
    let openDate = data[i].as_of_date;
    let marketValue = data[i].market_value;
    if (ticker && openDate == newestDate && marketValue && marketValue > 0) {
      holdingList.push(data[i]);
    } else if (ticker && marketValue && marketValue > 0) {
      hList.push(data[i]);
    }
    if (i >= (data.length - 1) && holdingList.length === 0) {
      console.log("Could not find a largest new holding");
    }
  }
  holdingList.sort((a, b) => a["market_value"] - b["market_value"]);

  let topStock = holdingList.pop();
  if (topStock) {
    return topStock;
  } else {
    console.log("Could not find last value");
  }

  hList = orderBy(
    hList,
    ["hList[0].as_of_date", "hList[1].market_value"],
    ["desc", "desc"]
  );

  console.log("Failed: attempting backup option");

  let topStockTwo = hList.pop();
  if (topStockTwo) {
    return topStockTwo;
  } else {
    console.log("Could not find last value of hList either");
  }
}

export async function getInstitutionLargestNewHolding(currentHoldings, cik) {
  let currentTickers = [];
  let filterDate;

  if (currentHoldings && currentHoldings.length > 0) {
    filterDate = currentHoldings[0].period_ended; // by putting in the filter date as the most recent period_ended date, the endpoint from intrinio will return the previous holdings rather than the current holdings
  }

  for (let i in currentHoldings) {
    let ticker = currentHoldings[i].ticker;
    let value = currentHoldings[i].value;
    if (ticker && value) {
      currentTickers.push({ticker: ticker, value: value});
    }
  }

  let previousHoldings = await fetchAllHoldingsOfCik(cik, filterDate);

  console.log("prev holdings: ", previousHoldings.length);

  let previousTickers = [];
  for (let j in previousHoldings) {
    let ticker = previousHoldings[j].ticker;
    let value = previousHoldings[j].value;
    if (ticker && value) {
      previousTickers.push({ticker: ticker, value: value});
    }
  }

  console.log("prev holdings tickers: ", previousTickers.length);

  if (previousTickers && previousTickers.length > 0) {
    console.log("Comparing previous and current tickers");
    // find tickers that are new
    let newTickers = [];

    currentTickers.forEach( function (i) {
      let found = previousTickers.find(h => {
        return h.ticker === i.ticker
      })
      if (!found) {
        newTickers.push(i);
      }
    });

    newTickers.sort((a, b) => a["value"] - b["value"]);
    let largestNewTicker = newTickers.pop();

    let largestNewHolding;
    if (largestNewTicker) {
      largestNewHolding = currentHoldings.find(h => {
        return h.ticker === largestNewTicker.ticker
      })
    }
    if (largestNewHolding) {
      return largestNewHolding;
    } else {
      console.log("Could not find largest value of current holdings that are new (last value)");
    }
  }

  console.log("Findings the largest of the current holdings");
  currentHoldings.sort((a, b) => a["value"] - b["value"]);
  let largestCurrent = currentHoldings.pop();
  if (largestCurrent) {
    return largestCurrent;
  } else {
    console.log("Could not find largest value of current holdings (last value)");
  }
}

export async function processCiks(type) {
  let result = await getCikHoldings();
  if (result.length > 0) {
    for (let i in result) {
      let id = result[i].id;
      let cik = result[i].cik;
      switch(type) {
        case "snapshots":
          await queue.publish_ProcessSnapshot_ciks(id, cik);
          break;
        case "top10andallocations":
          await queue.publish_ProcessTop10_and_Allocations_ciks(id, cik);
          break;
        default:
          break;
      }
    }
  }
}

export const fetchAllCiks = async () => {
  let result = await db(`
    SELECT cik as cik from institutions
    UNION (
    SELECT bc.cik as cik
    from billionaires b
    left join billionaire_ciks bc on bc.titan_id = b.id AND bc.is_primary = true
    WHERE bc.cik != '0000000000' AND bc.cik IS NOT NULL
    )
  `);

  if (size(result) !== 0) {
    map(result, async (data) => {
      await queue.publish_ProcessCiks(data.cik)
      return data.cik
    })
  }
}

export const fetchAllHoldingsOfCik = async (id, date) => {
  let next_page = null;
  let holdings = [];
  let buffer = [];
  let allHoldings = []

  do {
    let response = await getCikHoldingsFromIntrinio(id, next_page, date);
    next_page = response["next_page"];

    holdings = response["holdings"];
    if (holdings) {
      buffer = buffer.concat(holdings);
    }

  } while (next_page);

  buffer = await groupBy(buffer, (data) => data.ticker);

  let totalValueofAllHoldings = 0
  buffer = await map(buffer, async (data) => {
    let value = 0
    let amount = 0
    let sole_voting_authority = 0
    await map(data, (holding) => {
      value += holding.value
      amount += holding.amount
      sole_voting_authority += holding.sole_voting_authority
      totalValueofAllHoldings += holding.value
    })
    return { ...data[0], value, amount, sole_voting_authority }
  })

  for await (let holding of buffer) {
    let portfolio_percent = (holding.value / totalValueofAllHoldings) * 100
    allHoldings.push({ ...holding, portfolio_percent })
  }

  return allHoldings;
}

export const processHoldingsOfCik = async (id) => {

  let allHoldings = await fetchAllHoldingsOfCik(id);

  let json = allHoldings.length > 0 ? JSON.stringify(allHoldings) : null;

  let query = {
    text: "SELECT * FROM cik_holdings WHERE cik = $1",
    values: [id],
  };
  const result = await db(query);

  if (result.length > 0) {
    if (json) {
      query = {
        text:
          "UPDATE cik_holdings SET json_holdings = $1, updated_at = now() WHERE cik = $2",
        values: [json, id],
      };
      await db(query);
    }
  } else {
    query = {
      text:
        "INSERT INTO cik_holdings (cik, json_holdings) VALUES ( $1, $2 ) RETURNING *",
      values: [id, json],
    };
    await db(query);
  }

  return allHoldings
}