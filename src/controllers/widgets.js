import db from "../db";
import axios from "axios";
import * as queue from "../queue";
import * as companies from "./companies";

/* START Scraper */

const s3AllInsider =
  "https://terminal-scrape-data.s3.amazonaws.com/all-insider-trading/allInsider.json";

async function getAllInsider() {
  try {
    const response = await axios.get(s3AllInsider);
    return response.data;
  } catch (error) {
    console.error(error);
  }
}

export function getSecurityLastPrice(symbol) {
  let lastPrice = axios
    .get(
      `${process.env.INTRINIO_BASE_PATH}/securities/${symbol}/prices/realtime?source=iex&api_key=${process.env.INTRINIO_API_KEY}`
    )
    .then(function (res) {
      return res;
    })
    .catch(function (err) {
      return err;
    });
  //
  return lastPrice.then((data) => data.data);
}

/* END Scraper */

export async function getWidgets() {
  let result = await db(`
    SELECT widget_instances.*, widget_data.*, widgets.*
    FROM widget_instances
    JOIN widget_data ON widget_data.id = widget_instances.widget_data_id 
    JOIN widgets ON widgets.id = widget_instances.widget_id 
  `);

  return result;
}

export async function getGlobalWidgets() {
  let result = await db(`
    SELECT widget_instances.*, widget_data.*, widgets.*, widget_instances.id AS widget_instance_id
    FROM widget_instances
    JOIN widget_data ON widget_data.id = widget_instances.widget_data_id 
    JOIN widgets ON widgets.id = widget_instances.widget_id
    WHERE widget_instances.dashboard_id = 0
  `);

  return result;
}

export async function getLocalWidgets() {
  let result = await db(`
    SELECT widget_instances.*, widget_data.*, widgets.*, widget_instances.id AS widget_instance_id
    FROM widget_instances
    JOIN widget_data ON widget_data.id = widget_instances.widget_data_id 
    JOIN widgets ON widgets.id = widget_instances.widget_id
    WHERE widget_instances.dashboard_id != 0
  `);

  return result;
}

export async function getWidget(widgetInstanceId) {
  let result = await db(`
    SELECT widget_instances.*, widget_data.*, widgets.*
    FROM widget_instances
    JOIN widget_data ON widget_data.id = widget_instances.widget_data_id 
    JOIN widgets ON widgets.id = widget_instances.widget_id
    WHERE widget_instances.id = ${widgetInstanceId}
  `);

  return result;
}

export async function updateGlobal() {
  let widgets = await getGlobalWidgets();

  for (let i = 0; i < widgets.length; i += 1) {
    let widget = widgets[i];
    let widgetInstanceId = widget.widget_instance_id;

    await queue.publish_UpdateGlobalDashboard(widgetInstanceId);
  }
}

export async function updateLocal() {
  let widgets = await getLocalWidgets();
  for (let i = 0; i < widgets.length; i += 1) {
    let widget = widgets[i];
    let widgetInstanceId = widget.widget_instance_id;

    await queue.publish_UpdateLocalDashboards(widgetInstanceId);
  }
}

export async function processWidget(widgetInstanceId) {
  let widget;
  let output;

  let result = await getWidget(widgetInstanceId);

  if (result) {
    widget = result[0];
  }

  if (widget) {
    let type = widget.type;
    let input = widget.input;
    let dataId = widget.widget_data_id;
    let params = {};
    if (input) {
      Object.entries(input).map((item) => {
        params[item[0]] = item[1];
      });
    }

    /*          MUTUAL FUNDS */
    //Discount/Premium
    if (type == "MutualFundsTopNDiscount" || type == "MutualFundsTopNPremium") {
      let topFunds;
      if (params.count && params.count > 0) {
        let topNum = params.count;
        if (type == "MutualFundsTopNDiscount") {
          topFunds = await getMutualFundsTopNDiscountOrPremium(topNum, true);
        } else if (type == "MutualFundsTopNPremium") {
          topFunds = await getMutualFundsTopNDiscountOrPremium(topNum, false);
        }

        if (topFunds) {
          output = topFunds;
        }
      }
    }
    //Net Assets
    else if (type == "MutualFundsTopNNetAssets") {
      let topFunds;
      if (params.count && params.count > 0) {
        let topNum = params.count;
        topFunds = await getMutualFundsTopNNetAssets(topNum);

        if (topFunds) {
          output = topFunds;
        }
      }
    }
    //Yield
    else if (type == "MutualFundsTopNYield") {
      let topFunds;
      if (params.count && params.count > 0) {
        let topNum = params.count;
        topFunds = await getMutualFundsTopNYield(topNum);

        if (topFunds) {
          output = topFunds;
        }
      }
    }
    /*          INSIDERS */
    //Movers
    else if (type == "InsidersNMovers") {
      if (params.count && params.count > 0) {
        let topNum = params.count;
        let topComps = { topComps: await getInsidersNMovers(topNum) };

        if (topComps) {
          output = topComps;
        }
      }
    }
    /*          COMPANIES */
    //Prices
    else if (type == "CompanyPrice") {
      if (params.ticker) {
        let ticker = params.ticker;
        let price = await getCompanyPrice(ticker);
        let comp = await companies.getCompanyByTicker(ticker);

        if (price && comp && comp.json.name) {
          let tick = { ticker: ticker, name: comp.json.name, price: price };
          output = tick;
        }
      }
    }

    if (output) {
      let query = {
        text: "UPDATE widget_data SET output = $2 WHERE id = $1",
        values: [dataId, output],
      };
      await db(query);
      console.log("output updated");
    }
  }
}

export async function getMutualFundsTopNDiscountOrPremium(topNum, isDiscount) {
  let eFunds = [];
  let fFunds = [];
  let oFunds = [];

  let result = await db(`
    SELECT *
    FROM mutual_funds
  `);

  if (result.length > 0) {
    for (let i in result) {
      let fund = result[i];
      let fundCategory = fund.json.fundCategory;

      if (fund["json"]) {
        if (fund["json"]["nav"] && fund["json"]["mktPrice"]) {
          let difference = (
            (fund.json.mktPrice / fund.json.nav - 1) *
            100
          ).toFixed(2);
          if (fundCategory[0] == "E") {
            eFunds.push({
              fund: fund,
              diff: difference,
            });
          } else if (fundCategory[0] == "F") {
            fFunds.push({
              fund: fund,
              diff: difference,
            });
          } else if (fundCategory[0] == "H" || fundCategory[0] == "C") {
            oFunds.push({
              fund: fund,
              diff: difference,
            });
          }
        }
      }
    }
  }
  let equityFunds;
  let fixedFunds;
  let otherFunds;

  switch (isDiscount) {
    case true:
      equityFunds = eFunds
        .sort((a, b) => b.diff - a.diff)
        .slice(Math.max(eFunds.length - topNum, 0));
      fixedFunds = fFunds
        .sort((a, b) => b.diff - a.diff)
        .slice(Math.max(fFunds.length - topNum, 0));
      otherFunds = oFunds
        .sort((a, b) => b.diff - a.diff)
        .slice(Math.max(oFunds.length - topNum, 0));
    case false:
      equityFunds = eFunds
        .sort((a, b) => a.diff - b.diff)
        .slice(Math.max(eFunds.length - topNum, 0));
      fixedFunds = fFunds
        .sort((a, b) => a.diff - b.diff)
        .slice(Math.max(fFunds.length - topNum, 0));
      otherFunds = oFunds
        .sort((a, b) => a.diff - b.diff)
        .slice(Math.max(oFunds.length - topNum, 0));
  }

  let funds = {
    equityFunds,
    fixedFunds,
    otherFunds,
  };

  return funds;
  //console.log(funds);
}

export async function getMutualFundsTopNYield(topNum) {
  let eFunds = [];
  let fFunds = [];
  let oFunds = [];

  let result = await db(`
    SELECT *
    FROM mutual_funds
  `);

  if (result.length > 0) {
    for (let i in result) {
      let fund = result[i];
      let fundCategory = fund.json.fundCategory;

      if (fund.json.yield > 0) {
        if (fundCategory[0] == "E") {
          eFunds.push(fund);
        } else if (fundCategory[0] == "F") {
          fFunds.push(fund);
        } else if (fundCategory[0] == "H" || fundCategory[0] == "C") {
          oFunds.push(fund);
        }
      }
    }
  }
  let equityFunds;
  let fixedFunds;
  let otherFunds;

  equityFunds = eFunds
    .sort((a, b) => a.json.yield - b.json.yield)
    .slice(Math.max(eFunds.length - topNum, 0));
  fixedFunds = fFunds
    .sort((a, b) => a.json.yield - b.json.yield)
    .slice(Math.max(fFunds.length - topNum, 0));
  otherFunds = oFunds
    .sort((a, b) => a.json.yield - b.json.yield)
    .slice(Math.max(oFunds.length - topNum, 0));

  let funds = {
    equityFunds,
    fixedFunds,
    otherFunds,
  };

  return funds;
  //console.log(funds);
}

export async function getMutualFundsTopNNetAssets(topNum) {
  let eFunds = [];
  let fFunds = [];
  let oFunds = [];

  let result = await db(`
    SELECT *
    FROM mutual_funds
  `);

  if (result.length > 0) {
    for (let i in result) {
      let fund = result[i];
      let fundCategory = fund.json.fundCategory;

      if (fund.json_summary && fund.json_summary.netAssets > 0) {
        if (fundCategory[0] == "E") {
          eFunds.push(fund);
        } else if (fundCategory[0] == "F") {
          fFunds.push(fund);
        } else if (fundCategory[0] == "H" || fundCategory[0] == "C") {
          oFunds.push(fund);
        }
      }
    }
  }
  let equityFunds;
  let fixedFunds;
  let otherFunds;

  equityFunds = eFunds
    .sort((a, b) => a.json_summary.netAssets - b.json_summary.netAssets)
    .slice(Math.max(eFunds.length - topNum, 0));
  fixedFunds = fFunds
    .sort((a, b) => a.json_summary.netAssets - b.json_summary.netAssets)
    .slice(Math.max(fFunds.length - topNum, 0));
  otherFunds = oFunds
    .sort((a, b) => a.json_summary.netAssets - b.json_summary.netAssets)
    .slice(Math.max(oFunds.length - topNum, 0));

  let funds = {
    equityFunds,
    fixedFunds,
    otherFunds,
  };

  return funds;
  //console.log(funds);
}

export async function getInsidersNMovers(topNum) {
  let comps = [];
  let compsMap = new Map();

  let result = await getAllInsider();
  for (let i in result) {
    let tran = result[i];
    let ticker = tran[0];
    if (compsMap.has(ticker)) {
      let trans = compsMap.get(ticker).trans;
      trans.push({
        //name: tran[1],
        //title: tran[2],
        type: tran[4],
        //cost: tran[5],
        numShares: tran[6],
        value: tran[7],
      });
    } else {
      compsMap.set(ticker, {
        trans: [
          {
            //name: tran[1],
            //title: tran[2],
            type: tran[4],
            //cost: tran[5],
            numShares: tran[6],
            value: tran[7],
          },
        ],
      });
    }
  }
  compsMap.forEach((value, key) => {
    let trans = value.trans;
    let sold = 0;
    let bought = 0;
    let option = 0;
    let valueChange = 0;
    for (let t in trans) {
      let type = trans[t].type;
      let numShares = parseInt(trans[t].numShares.replace(/,/g, ""));
      let value = parseInt(trans[t].value.replace(/,/g, ""));
      if (type[0] == "S") {
        sold += numShares;
        valueChange -= value;
      } else if (type[0] == "B") {
        bought += numShares;
        valueChange += value;
      } else if (type[0] == "O") {
        option += numShares;
        valueChange += value;
      }
    }
    comps.push({
      ticker: key,
      sold: sold,
      bought: bought,
      option: option,
      valueChange: valueChange,
    });
  });

  let compsSorted = comps
    .sort((a, b) => Math.abs(a.valueChange) - Math.abs(b.valueChange))
    .slice(Math.max(comps.length - topNum, 0));

  //console.log(compsSorted);
  return compsSorted;
}

export async function getCompanyPrice(ticker) {
  let data = await getSecurityLastPrice(ticker);
  if (data.last_price) {
    return data.last_price;
  }
}
