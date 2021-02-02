import db from "../db";
import * as titans from "./titans";
import * as securities from "./securities";

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
    SELECT i.*, i_h.*
    FROM institutions AS i
    LEFT JOIN institution_holdings AS i_h
    ON i.id = i_h.institution_id
    WHERE i.cik = '${cik}'
  `);

  if (result && result.length > 0) {
    let { json_holdings } = result[0];
    if (!json_holdings) {
      console.log("No json holdings for this institution: ", cik);
      return null;
    }
    let filtered = json_holdings.filter((o) => {
      return o.shares_held != 0;
    });
    return filtered;
  }
  console.log("Found no results from query (getInstitutionsHoldings)");
  return null;
}

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
  let tickers = data.map(({ company }) => {
    if (!company) {
      return;
    }
    return company["ticker"];
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
        let holdingTicker = holdings[j].company.ticker;
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
    let market_value = merged[i]["holding"]["market_value"];

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

export async function getInstitutionSnapshot(id) {

  console.log("Get institution id: ", id);

  let result = await db(`
    SELECT *
    FROM institutions
    WHERE id = ${id}
  `);
  console.log("--------------------start------------------------");

  let data
  if (result.length > 0) {
    console.log("Found the institution: ", result[0].id);
    data = await getInstitutionsHoldings(result[0].cik);
    if (!data || data.length === 0) {
      console.log("data: ", data);
      console.log("Found no holdings data for this institution: ", id)
      console.log("Failed: ", id);
      console.log("--------------------end------------------------");
      return null;
    }
  } else {
    console.log("Did not find this institution: ", id);
    console.log("Failed: ", id);
    console.log("--------------------end------------------------");
  }

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
  let largest = await getInstitutionLargestHolding(data);
  console.log(largest);
  console.log("largest ended");

  // console.log("topPerf", topPerf);
  // console.log("common", common);

  if (topPerf && common && uncommon) {
    let topPerfPrice = await titans.calculateHoldingPrice(topPerf);
    let topPerfSec = await securities.getSecurityByTicker(topPerf.company.ticker);
    let commonPrice = await titans.calculateHoldingPrice(common);
    let commonSec = await securities.getSecurityByTicker(common.company.ticker);
    let uncommonPrice = await titans.calculateHoldingPrice(uncommon);
    let uncommonSec = await securities.getSecurityByTicker(uncommon.company.ticker);
    let largestPrice = await titans.calculateHoldingPrice(largest);
    let largestSec = await securities.getSecurityByTicker(largest.company.ticker);

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
        ticker: topPerf.company.ticker,
        name: topPerf.company.name,
        open_date: topPerf.as_of_date,
        open_price: topPerfPrice,
        price_percent_change_1_year: topPerfSec.price_percent_change_1_year,
      },
      common: {
        ticker: common.company.ticker,
        name: common.company.name,
        open_date: common.as_of_date,
        open_price: commonPrice,
        price_percent_change_1_year: commonSec.price_percent_change_1_year,
      },
      uncommon: {
        ticker: uncommon.company.ticker,
        name: uncommon.company.name,
        open_date: uncommon.as_of_date,
        open_price: uncommonPrice,
        price_percent_change_1_year: uncommonSec.price_percent_change_1_year,
      },
      largest: {
        ticker: largest.company.ticker,
        name: largest.company.name,
        open_date: largest.as_of_date,
        open_price: largestPrice,
        price_percent_change_1_year: largestSec.price_percent_change_1_year,
      },
    };
  }
}

const getSecuritiesBySort = async (sort, direction, data) => {
  console.log("starting securities by sort");
  let tickerList = [];
  for (let i in data) {
    let ticker = data[i].company.ticker;
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
        let ticker = data[i].company.ticker;
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

export async function insertSnapshotInstitution(id, snapshot) {
  if (!id || !snapshot) {
    return;
  }

  console.log("snapshot json insert", snapshot);

  let query = {
    text: "SELECT * FROM institutions WHERE id = $1",
    values: [id],
  };
  let result = await db(query);

  if (result.length > 0) {
    console.log("in update");
    let query = {
      text: "UPDATE institutions SET json_stock_snapshot = $2 WHERE id = $1",
      values: [id, snapshot],
    };
    await db(query);
    console.log("updated");
  }
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

export async function processInstitutionsSnapshots() {
  let result = await getInstitutions({ size: 5000 });
  if (result.length > 0) {
    for (let i in result) {
      let id = result[i].id;
      await queue.publish_ProcessSnapshot_Institutions(id);
    }
  }
}