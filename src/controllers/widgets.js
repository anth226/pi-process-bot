import db from "../db";
import axios from "axios";
import * as queue from "../queue";
import * as companies from "./companies";
import * as mutualfunds from "./mutualfunds";

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
  let widgetData = new Map();
  let widgets = await getLocalWidgets();
  for (let i = 0; i < widgets.length; i += 1) {
    let widget = widgets[i];
    let widgetInstanceId = widget.widget_instance_id;
    let widgetDataId = widget.widget_data_id;
    widgetData.set(widgetDataId, widgetInstanceId);
  }
  widgetData.forEach((id) => queue.publish_UpdateLocalDashboards(id));
}

export async function processInput(widgetInstanceId) {
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
        let count = params.count;
        topFunds = await getMutualFundsTopNNetAssets(count);

        if (topFunds) {
          output = topFunds;
        }
      }
    }
    //Yield
    else if (type == "MutualFundsTopNYield") {
      let topFunds;
      if (params.count && params.count > 0) {
        let count = params.count;
        topFunds = await getMutualFundsTopNYield(count);

        if (topFunds) {
          output = topFunds;
        }
      }
    }
    //MutualFundPrice
    //Price
    else if (type == "MutualFundPrice") {
      if (params.ticker) {
        let ticker = params.ticker;
        let price = await getCompanyPrice(ticker);
        let fund = await mutualfunds.getMutualFundByTicker(ticker);
        let metrics = await companies.getCompanyMetrics(ticker);

        if (
          price &&
          fund &&
          fund.json &&
          fund.json.name &&
          metrics &&
          metrics.Change
        ) {
          let delta = metrics.Change;
          let tick = {
            ticker: ticker,
            name: fund.json.name,
            price: price,
            delta: delta,
          };
          output = tick;
        }
      }
    }
    /*          INSIDERS */
    //Movers
    else if (type == "InsidersNMovers") {
      if (params.count && params.count > 0) {
        let count = params.count;
        let topComps = { topComps: await getInsidersNMovers(count) };

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
        let metrics = await companies.getCompanyMetrics(ticker);

        if (
          price &&
          comp &&
          comp.json &&
          comp.json.name &&
          metrics &&
          metrics.Change
        ) {
          let delta = metrics.Change;
          let tick = {
            ticker: ticker,
            name: comp.json.name,
            price: price,
            delta: delta,
          };
          output = tick;
        }
      }
    }
    /*          ETFS */
    //Top any stats/analytics data
    else if (type == "ETFTopNDataByType") {
      if (params.type && params.data && params.count) {
        let { type, data, count } = params;
        let topETFs = { topETFs: await getETFsTopNData(count, type, data) };

        if (topETFs) {
          output = topETFs;
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
    } else {
      let query = {
        text: "DELETE FROM widget_instances WHERE id=($1)",
        values: [widgetInstanceId],
      };

      await db(query);

      query = {
        text: "DELETE FROM widget_data WHERE id=($1)",
        values: [dataId],
      };

      await db(query);
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
  if (data && data.last_price) {
    return data.last_price;
  }
}

export async function getETFsTopNData(count, type, data) {
  let etfList = [];
  let dataObj;

  let etfs = await db(`
      SELECT *, json -> 'type' AS type
      FROM etfs
    `);
  for (let i in etfs) {
    if (etfs[i].json_stats && etfs[i].json_analytics && etfs[i].type == type) {
      etfList.push(etfs[i]);
    }
  }

  switch (data) {
    case "net_asset_value":
    case "beta_vs_spy":
    case "trailing_one_month_return_split_and_dividend":
    case "trailing_one_month_return_split_only":
    case "trailing_one_year_return_split_and_dividend":
    case "trailing_one_year_return_split_only":
    case "trailing_one_year_volatility_annualized":
    case "trailing_three_year_annualized_return_split_and_dividend":
    case "trailing_three_year_annualized_return_split_only":
    case "trailing_three_year_volatility_annualized":
    case "trailing_five_year_annualized_return_split_and_dividend":
    case "trailing_five_year_annualized_return_split_only":
    case "trailing_five_year_volatility_annualized":
    case "trailing_ten_year_annualized_return_split_and_dividend":
    case "trailing_ten_year_annualized_return_split_only":
    case "inception_annualized_return_split_and_dividend":
    case "inception_annualized_return_split_only":
    case "calendar_year_5_return_split_and_dividend":
    case "calendar_year_5_return_split_only":
    case "calendar_year_4_return_split_and_dividend":
    case "calendar_year_4_return_split_only":
    case "calendar_year_3_return_split_and_dividend":
    case "calendar_year_3_return_split_only":
    case "calendar_year_2_return_split_and_dividend":
    case "calendar_year_2_return_split_only":
    case "calendar_year_1_return_split_and_dividend":
    case "calendar_year_1_return_split_only":
    case "calendar_year_to_date_return_split_and_dividend":
    case "calendar_year_to_date_return_split_only":
      dataObj = "json_stats";
      break;
    case "fifty_two_week_high":
    case "fifty_two_week_low":
    case "volume_traded":
    case "average_daily_volume_one_month":
    case "average_daily_volume_three_month":
    case "average_daily_volume_six_month":
    case "market_cap":
    case "shares_outstanding":
      dataObj = "json_analytics";
      break;
  }

  let topETFs = etfList
    .sort((a, b) => a[dataObj][data] - b[dataObj][data])
    .slice(Math.max(etfList.length - count, 0));

  //console.log(topETFs);
  return topETFs;
}
