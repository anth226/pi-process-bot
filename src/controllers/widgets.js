import db from "../db";
import db1 from "../db1";
import axios from "axios";
import cheerio from "cheerio";
import intrinioSDK from "intrinio-sdk";
import moment from "moment";
import { orderBy } from "lodash";

import * as getSecurityData from "./intrinio/get_security_data";
import * as queue from "../queue";
import * as companies from "./companies";
import * as institutions from "./institutions";
import * as mutualfunds from "./mutualfunds";
import * as etfs from "./etfs";
import * as securities from "./securities";
import * as earnings from "./earnings";
import * as quodd from "./quodd";
import { getPortfolioByDashboardID } from "./userportfolios";

// init intrinio
intrinioSDK.ApiClient.instance.authentications["ApiKeyAuth"].apiKey =
  process.env.INTRINIO_API_KEY;

intrinioSDK.ApiClient.instance.basePath = `${process.env.INTRINIO_BASE_PATH}`;

const companyAPI = new intrinioSDK.CompanyApi();
const securityAPI = new intrinioSDK.SecurityApi();
const indexAPI = new intrinioSDK.IndexApi();

/* START API */

const s3AllInsider = `https://${process.env.AWS_BUCKET_TERMINAL_SCRAPE}.s3.amazonaws.com/all-insider-trading/allInsider.json`;

async function getAllInsider() {
  try {
    const response = await axios.get(s3AllInsider);
    return response.data;
  } catch (error) {
    console.error(error);
  }
}

/* END API */

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

export async function getLocalPriceWidgets() {
  let result = await db(`
    SELECT widget_instances.*, widget_data.*, widgets.*, widget_instances.id AS widget_instance_id
    FROM widget_instances
    JOIN widget_data ON widget_data.id = widget_instances.widget_data_id 
    JOIN widgets ON widgets.id = widget_instances.widget_id
    WHERE widget_instances.dashboard_id != 0 AND widgets.type in ('ETFPrice', 'MutualFundPrice', 'CompanyPrice')
  `);

  return result;
}

export async function getLocalPriceWidgetsByPortId(portId) {
  let result = await db(`
    SELECT widget_instances.*, widget_data.*, widgets.*, widget_instances.id AS widget_instance_id
    FROM widget_instances
    JOIN widget_data ON widget_data.id = widget_instances.widget_data_id 
    JOIN widgets ON widgets.id = widget_instances.widget_id
    WHERE widget_instances.dashboard_id = ${portId} AND widgets.type in ('ETFPrice', 'MutualFundPrice', 'CompanyPrice')
  `);

  return result;
}

export async function getLocalPerfomanceWidgetForDashboard(dashboard_id) {
  let result = await db(`
  SELECT widget_instances.*, widget_data.*, widgets.*, widget_instances.id AS widget_instance_id
  FROM widget_instances
  JOIN widget_data ON widget_data.id = widget_instances.widget_data_id 
  JOIN widgets ON widgets.id = widget_instances.widget_id
  WHERE widget_instances.dashboard_id = ${dashboard_id} AND widgets.type = 'UsersPerformance'
    `);

  return result;
}

export async function getWidgetTypeId(widgetType) {
  let result = await db(`
    SELECT id
    FROM widgets
    WHERE type = '${widgetType}'
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
    let dashboardId = widget.dashboard_id;
    let params = {};
    if (input) {
      Object.entries(input).map((item) => {
        params[item[0]] = item[1];
      });
    }

    /*          USER */
    //Movers
    if (type == "UsersPortfolioPerf") {
      return;
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
    /*          TITANS */
    //Trending Titans
    else if (type == "TitansTrending") {
      let data = await updateTrendingTitans();
      if (data && data.length === 0) {
        return;
      }
      console.log("data", data);
      let json = JSON.stringify(data);
      console.log("json", json);

      if (data) {
        output = json;
      }
    }
    /*          COMPANIES */
    //Strong Buys
    else if (type == "CompanyStrongBuys") {
      if (params.tickers) {
        let data = await getStrongBuys(params.tickers);
        let json = JSON.stringify(data);

        if (data) {
          output = json;
        }
      }
    }
    //Top Stocks
    else if (type == "CompanyTopStocks") {
      let data = await getTopStocks();
      let json = JSON.stringify(data);

      if (data) {
        output = json;
      }
    }
    //Prices
    else if (type == "CompanyPrice") {
      if (params.ticker) {
        let name;
        let ticker = params.ticker;
        let price = await getPrice(ticker);
        let comp = await companies.getCompanyByTicker(ticker);
        let metrics = await companies.getCompanyMetrics(ticker);
        //get perf from db instead
        let performance = await securities.getSecurityPerformance(ticker);

        if (comp && comp.json && comp.json.name) {
          name = comp.json.name;
        } else {
          let sec = await getSecurityData.lookupSecurity(securityAPI, ticker);
          name = sec.name;
        }

        if (price && metrics && metrics.Change) {
          let delta = metrics.Change;
          let tick = {
            ticker: ticker,
            name: name,
            price: price,
            delta: delta,
            performance: performance,
          };
          output = tick;
        }
      }
    }
    /*          MUTUAL FUNDS */
    //Discount/Premium
    else if (
      type == "MutualFundsTopNDiscount" ||
      type == "MutualFundsTopNPremium"
    ) {
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
    //Performance
    else if (type == "MutualFundsTopNPerformance") {
      let topFunds;
      if (params.count && params.count > 0 && params.freq) {
        let count = params.count;
        let freq = params.freq;
        topFunds = await getMutualFundsTopNPerformance(count, freq);

        if (topFunds) {
          output = topFunds;
        }
      }
    }
    //Price
    else if (type == "MutualFundPrice") {
      if (params.ticker) {
        let name;
        let ticker = params.ticker;
        let price = await getPrice(ticker);
        let fund = await mutualfunds.getMutualFundByTicker(ticker);
        let metrics = await companies.getCompanyMetrics(ticker);
        //get perf from db instead
        let performance = await securities.getSecurityPerformance(ticker);

        if (fund && fund.json && fund.json.name) {
          name = fund.json.name;
        } else {
          let sec = await getSecurityData.lookupSecurity(securityAPI, ticker);
          name = sec.name;
        }

        if (price && metrics && metrics.Change) {
          let delta = metrics.Change;
          let tick = {
            ticker: ticker,
            name: name,
            price: price,
            delta: delta,
            performance: performance,
          };
          output = tick;
        }
      }
    }
    /*          ETFS */
    //Price
    else if (type == "ETFPrice") {
      if (params.ticker) {
        let name;
        let ticker = params.ticker;
        let price = await getPrice(ticker);
        let etf = await etfs.getETFByTicker(ticker);
        let metrics = await companies.getCompanyMetrics(ticker);
        //get perf from db instead
        let performance = await securities.getSecurityPerformance(ticker);
        let topHoldings = await getETFHoldings(ticker, 10);

        if (etf && etf.json && etf.json.name) {
          name = etf.json.name;
        } else {
          let sec = await getSecurityData.lookupSecurity(securityAPI, ticker);
          name = sec.name;
        }

        if (price && metrics && metrics.Change) {
          let delta = metrics.Change;
          let tick = {
            ticker: ticker,
            name: name,
            price: price,
            delta: delta,
            performance: performance,
            top_holdings: topHoldings,
          };
          output = tick;
        }
      }
    }
    //Top any stats/analytics data by type
    else if (type == "ETFTopNDataByType") {
      if (params.type && params.data_key && params.count) {
        let { type, data_key, count } = params;
        let topETFs = {
          topETFs: await getETFsTopNDataByType(count, type, data_key),
        };

        if (topETFs) {
          output = topETFs;
        }
      }
    }
    //Top any stats/analytics data by sector
    else if (type == "ETFTopNDataBySector") {
      if (params.sector && params.data_key && params.count) {
        let { sector, data_key, count } = params;
        let topETFs = {
          topETFs: await getETFsTopNDataBySector(count, sector, data_key),
        };

        if (topETFs) {
          output = topETFs;
        }
      }
    }
    /*          SECURITIES */
    //Top Aggregate Analyst Ratings
    else if (type == "SecuritiesTopAggAnalyst") {
      let data = await getAggRatings();
      let json = JSON.stringify(data);

      if (data) {
        output = json;
      }
    }

    //Earnings Calendar
    else if (type == "SecuritiesEarningsCalendar") {
      let data = await getEarningsCalendar();
      let json = JSON.stringify(data);

      if (data) {
        output = json;
      }
    }

    if (output) {
      let query = {
        text:
          "UPDATE widget_data SET output = $2, updated_at = now() WHERE id = $1",
        values: [dataId, output],
      };
      await db(query);
      console.log("output updated");
    } else if (!output && dashboardId != 0 && type != "UsersPerformance") {
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

// Helper functions

export async function getETFHoldings(ticker, count) {
  // .get(
  //   `${
  //     process.env.INTRINIO_BASE_PATH
  //   }/zacks/etf_holdings?etf_ticker=${ticker.toUpperCase()}&api_key=${
  //     process.env.INTRINIO_API_KEY
  //   }`
  // )
  let holdings = [];
  let result = await axios
    .get(
      `${
        process.env.INTRINIO_BASE_PATH
      }/etfs/${ticker.toUpperCase()}/holdings?api_key=${
        process.env.INTRINIO_API_KEY
      }`
    )
    .then(function (res) {
      return res.data;
    })
    .catch(function (err) {
      console.log(err);
      return {};
    });

  if (result && result.holdings) {
    for (let i = 0; i < count; i++) {
      if (result.holdings[i]) {
        holdings.push(result.holdings[i]);
      }
    }
    return holdings;
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
      if (fund.json && fund.json.fundCategory) {
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
      if (fund.json && fund.json.fundCategory) {
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
      if (fund.json && fund.json.fundCategory) {
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

export async function getMutualFundsTopNPerformance(topNum, freq) {
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
      if (fund.json && fund.json.fundCategory) {
        let fundCategory = fund.json.fundCategory;

        if (fund.json_performance && fund.json_performance[freq]) {
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
  }
  let equityFunds;
  let fixedFunds;
  let otherFunds;

  equityFunds = eFunds
    .sort((a, b) => a.json_performance[freq] - b.json_performance[freq])
    .slice(Math.max(eFunds.length - topNum, 0));
  fixedFunds = fFunds
    .sort((a, b) => a.json_performance[freq] - b.json_performance[freq])
    .slice(Math.max(fFunds.length - topNum, 0));
  otherFunds = oFunds
    .sort((a, b) => a.json_performance[freq] - b.json_performance[freq])
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

export async function getPrice(ticker) {
  let data = await quodd.getLastPrice(ticker);
  if (data && data.last_price) {
    return data.last_price;
  }
}

export async function getETFsTopNDataByType(count, type, data_key) {
  let etfList = [];
  let dataObj;

  let etfs = await db(`
      SELECT *, json -> 'type' AS type
      FROM etfs
    `);
  for (let i in etfs) {
    if (
      etfs[i].json_stats &&
      etfs[i].json_stats.net_asset_value &&
      etfs[i].json_analytics &&
      etfs[i].type == type
    ) {
      etfList.push(etfs[i]);
    }
  }

  switch (data_key) {
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
    .sort((a, b) => b[dataObj][data_key] - a[dataObj][data_key])
    .slice(Math.max(etfList.length - count, 0));

  //console.log(topETFs);
  return topETFs;
}

export async function getETFsTopNDataBySector(count, sector, data_key) {
  let topETFs = [];
  let num = 0;
  let dataObj;

  let etfs = await db(`
      SELECT *
      FROM etfs
    `);

  switch (data_key) {
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

  let etfList = etfs.sort(
    (a, b) => b[dataObj][data_key] - a[dataObj][data_key]
  );

  for (let i in etfList) {
    let ticker = etfList[i].ticker;
    let nav;
    if (etfList[i].json_stats) {
      nav = etfList[i].json_stats.net_asset_value;
    }
    let query = {
      text: "SELECT * FROM ticker_classifications WHERE ticker = $1",
      values: [ticker],
    };
    let result = await db(query);

    if (result && result.length > 0) {
      let topSector = result[0].json_tags[0].label;
      //console.log(topSector);
      if (topSector == sector && nav) {
        topETFs.push(etfList[i]);
        num += 1;
        if (num == count) {
          break;
        }
      }
    }
  }

  return topETFs;
}

export async function getStrongBuys(list) {
  let buys = [];
  //    INTRINIO SCREENER
  // const url = `${process.env.INTRINIO_BASE_PATH}/securities/screen?order_column=zacks_analyst_rating_strong_buys&order_direction=desc&page_size=9&api_key=${process.env.INTRINIO_API_KEY}`;
  // const body = {
  //   operator: "AND",
  //   clauses: [
  //     {
  //       field: "zacks_analyst_rating_strong_buys",
  //       operator: "gt",
  //       value: "0",
  //     },
  //   ],
  // };
  // let res = axios
  //   .post(url, body)
  //   .then(function (data) {
  //     //console.log(data);
  //     return data;
  //   })
  //   .catch(function (err) {
  //     console.log(err);
  //     return err;
  //   });
  // let data = await res.then((data) => data.data);
  let data = list;
  for (let i in data) {
    let logo_url;
    let delta;
    let name;
    let strongBuys;
    let compTicker;
    let ticker = data[i];

    try {
      let url = `${process.env.INTRINIO_BASE_PATH}/securities/${ticker}/zacks/analyst_ratings?api_key=${process.env.INTRINIO_API_KEY}`;

      let res = await axios.get(url);

      if (res.data) {
        if (res.data.analyst_ratings && res.data.analyst_ratings.strong_buys) {
          strongBuys = res.data.analyst_ratings.strong_buys;
        }
        if (res.data.security && res.data.security.composite_ticker) {
          compTicker = res.data.security.composite_ticker;
        }
      }
    } catch (e) {
      console.error(e);
    }

    let company = await companies.getCompanyByTicker(ticker);

    if (company && company.json) {
      name = company.json.name;
    } else {
      let sec = await getSecurityData.lookupSecurity(securityAPI, ticker);
      if (sec) {
        name = sec.name;
        if (!compTicker) {
          compTicker = sec.composite_ticker;
        }
      }
    }
    if (company && company.logo_url) {
      logo_url = company.logo_url;
    }
    let price = await getPrice(ticker);
    // let metrics = await companies.getCompanyMetrics(ticker);
    // if (metrics) {
    //   //delta = metrics.Change;
    //   delta = metrics["Perf Month"];
    // }
    //
    //
    //get perf from db instead
    let perf = await securities.getSecurityPerformance(ticker);
    if (perf) {
      let perf90Day = perf.price_percent_change_3_months.toFixed(2);
      let perfString = perf90Day.toString();
      delta = perfString + "%";
    }

    buys.push({
      ticker: ticker,
      composite_ticker: compTicker,
      name: name,
      strong_buys: strongBuys,
      last_price: price,
      delta: delta,
      logo_url: logo_url,
    });
  }
  return buys;
}

export async function getAggRatings() {
  let comps = [];
  let aMonthAgo = moment().subtract(1, "months").format("YYYY-MM-DD");
  const url = `${process.env.INTRINIO_BASE_PATH}/securities/screen?order_column=zacks_analyst_rating_mean&order_direction=asc&page_size=66&api_key=${process.env.INTRINIO_API_KEY}`;
  const body = {
    operator: "AND",
    clauses: [
      {
        field: "zacks_analyst_rating_mean",
        operator: "gt",
        value: "0",
      },
      {
        field: "zacks_analyst_rating_total",
        operator: "gt",
        value: "2",
      },
      {
        field: "price_date",
        operator: "gt",
        value: aMonthAgo,
      },
    ],
  };

  let res = axios
    .post(url, body)
    .then(function (data) {
      // console.log(data);
      return data;
    })
    .catch(function (err) {
      console.log(err);
      return err;
    });

  let data = await res.then((data) => data.data);

  data = orderBy(
    data,
    ["data[0].number_value", "data[1].number_value"],
    ["asc", "desc"]
  );

  let rank = 0;
  for (let i in data) {
    rank += 1;
    let ticker = data[i].security.ticker;
    let name = data[i].security.name;
    let preRating = data[i].data[0].number_value;
    let rating = (preRating - 6) * -1;

    comps.push({
      rank: rank,
      ticker: ticker,
      name: name,
      rating: rating,
    });
  }
  return comps;
}

export async function getTrendingTitans() {
  let titans = [];
  const response = await axios.get(
    `https://${process.env.PROD_API_URL}/billionaires/list`
  );

  const formatted = response.data.map((item) => {
    if (item.industry) {
      item.industry = item.industry.toLowerCase();
    }

    if (item.json !== null) {
      item.performance_one_year = item.json.performance_one_year;
      item.performance_five_year = item.json.performance_five_year;
      item.fund_size = item.json.fund_size.toLowerCase();
    } else if (item.json_calculations !== null) {
      item.performance_one_year = item.json_calculations.performance_one_year;
      item.performance_five_year = item.json_calculations.performance_five_year;
      item.fund_size = null;
    } else {
      item.performance_one_year = null;
      item.performance_five_year = null;
      item.fund_size = null;
    }

    return {
      ...item,
    };
  });

  const holdingsSorted = formatted.sort(
    (a, b) =>
      b.performance_one_year * b.sortFactor -
      a.performance_one_year * a.sortFactor
  );
  for (let i = 0; i < 25; i++) {
    let id = holdingsSorted[i].id;
    //remove specific titan
    if (id == 6) {
      continue;
    }
    let name = holdingsSorted[i].name;
    let perf = holdingsSorted[i].performance_one_year;
    let photo = holdingsSorted[i].photo_url;
    let uri = holdingsSorted[i].uri;
    titans.push({
      id: id,
      name: name,
      performance_one_year: perf,
      photo_url: photo,
      uri: uri,
    });
  }
  return titans;
}

export async function scrapeTopStocks() {
  try {
    const response = await axios.get(
      `https://finviz.com/screener.ashx?v=111&s=ta_topgainers`
    );

    const $ = cheerio.load(response.data);

    let comps = [];

    $("a.screener-link-primary").each(function (idx, element) {
      let find = $(element).text();
      comps.push(find);
    });

    return comps;
  } catch (error) {
    return null;
  }
}

export async function getTopStocks() {
  let stocks = [];
  let comps = await scrapeTopStocks();

  for (let i = 0; i < comps.length; i++) {
    let ticker = comps[i];
    let comp = await companies.getCompanyByTicker(ticker);
    let sec = await securities.getSecurityByTicker(ticker);
    let price = await getPrice(ticker);
    let comp_metrics = await companies.getCompanyMetrics(ticker);

    if (
      price &&
      comp &&
      sec &&
      sec.json_metrics &&
      comp.json &&
      comp.json.name &&
      comp_metrics &&
      comp_metrics.Change
    ) {
      let id = comp.id;
      let name = comp.json.name;
      let delta = comp_metrics.Change;
      let metrics = sec.json_metrics;
      stocks.push({
        id: id,
        ticker: ticker,
        name: name,
        price: price,
        delta: delta,
        metrics: metrics,
      });
    }
  }

  return stocks;
}

export async function getEarningsCalendar() {
  let reports = [];
  let data = await earnings.getEarningsReports();
  for (let i in data) {
    let ticker = data[i].ticker;
    let name = data[i].name;
    let earningsDate = data[i].earnings_date;
    let time_of_day = data[i].time_of_day;
    let eps_actual = data[i].eps_actual;
    let eps_estimate = data[i].eps_estimate;
    let suprise_percentage = data[i].suprise_percentage;
    let ranking = data[i].ranking;
    let logo_url = data[i].logo_url;
    let actual_reported_date = data[i].actual_reported_date;
    let type = data[i].type;
    reports.push({
      earnings_date: earningsDate,
      ticker: ticker,
      type: type,
      name: name,
      time_of_day: time_of_day,
      eps_actual: eps_actual,
      eps_estimate: eps_estimate,
      suprise_percentage: suprise_percentage,
      ranking: ranking,
      logo_url: logo_url,
      actual_reported_date: actual_reported_date,
    });
  }
  let sorted = reports.sort(function (a, b) {
    var aa = a.earnings_datez + "".split("-").join(),
      bb = b.earnings_date + "".split("-").join();
    return aa < bb ? -1 : aa > bb ? 1 : 0;
  });
  return sorted;
}

export async function getTitanPerformance(uri) {
  let item;
  let company;
  let data;
  let result = await db(`
    SELECT b.*, b_c.ciks
    FROM public.billionaires AS b
    LEFT JOIN (
      SELECT titan_id, json_agg(json_build_object('cik', cik, 'name', name, 'is_primary', is_primary, 'rank', rank) ORDER BY rank ASC) AS ciks
      FROM public.billionaire_ciks
      GROUP BY titan_id
    ) AS b_c ON b.id = b_c.titan_id
    WHERE uri = '${uri}'
  `);

  if (result.length > 0) {
    let ciks = result[0].ciks;
    if (ciks && ciks.length > 0) {
      for (let j = 0; j < ciks.length; j += 1) {
        let cik = ciks[j];
        if (cik.cik != "0000000000" && cik.is_primary == true) {
          item = await institutions.getInstitutionByCIK(cik.cik);

          let { use_company_performance_fallback } = result[0];
          if (use_company_performance_fallback) {
            company = await companies.getCompanyByCIK(cik.cik);
          }
        }
      }
    }
    data = {
      profile: result[0],
      summary: item,
      company,
    };
  }

  let json = data.summary[0].json;

  if (json) {
    let perf = {
      performance_five_year: json.performance_five_year,
      performance_one_year: json.performance_one_year,
      performance_quarter: json.performance_quarter,
    };

    return perf;
  }
}

export async function getSnPPerformance() {
  let data = await db(`
    SELECT *
      FROM indices_candles_daily
      WHERE index = 'INDEXSP'
      ORDER BY timestamp DESC
    `);

  if (
    data[0] &&
    data[5] &&
    data[12] &&
    data[24] &&
    data[64] &&
    data[0].close &&
    data[5].close &&
    data[12].close &&
    data[24].close &&
    data[64].close
  ) {
    let perf = {
      price_percent_change_7_days: (data[0].close / data[5].close - 1) * 100,
      price_percent_change_14_days: (data[0].close / data[12].close - 1) * 100,
      price_percent_change_30_days: (data[0].close / data[24].close - 1) * 100,
      price_percent_change_3_months: (data[0].close / data[64].close - 1) * 100,
      values: {
        today: data[0].close,
        week: data[5].close,
        twoweek: data[12].close,
        month: data[24].close,
        threemonth: data[64].close,
      },
    };
    return perf;
  }
}

export async function processUsersPortPerf() {
  let res = await getWidgetTypeId("UsersPerformance");
  let userPerfWidgetId = res[0].id;
  let widgets = await getLocalPriceWidgets();
  //console.log("widgets", widgets);
  let dashboards = new Map();

  for (let i in widgets) {
    let values;
    let dashboardId = widgets[i].dashboard_id;
    let output = widgets[i].output;
    if (output && output.performance && output.performance.values) {
      values = output.performance.values;
    }

    if (dashboards.has(dashboardId)) {
      //stocks historical
      if (values) {
        let totals = dashboards.get(dashboardId).totals;
        if (totals) {
          let today = totals.today + values.today.value;
          let week = totals.week + values.week.value;
          let twoweek = totals.twoweek + values.twoweek.value;
          let month = totals.month + values.month.value;
          let threemonth = totals.threemonth + values.threemonth.value;
          totals = {
            today: today,
            week: week,
            twoweek: twoweek,
            month: month,
            threemonth: threemonth,
          };
        }
      }
    } else {
      //stocks historical
      let totals;
      if (values) {
        totals = {
          today: values.today.value,
          week: values.week.value,
          twoweek: values.twoweek.value,
          month: values.month.value,
          threemonth: values.threemonth.value,
        };
      }

      //user portfolio
      let portfolio = await getPortfolioByDashboardID(dashboardId);
      let portfolioId = portfolio.id;
      let portfolioHistory = await getPortfolioHistory(portfolioId);

      dashboards.set(dashboardId, {
        portfolio_id: portfolioId,
        portfolio_history: portfolioHistory,
        totals: totals,
      });
    }
  }

  dashboards.forEach(async (value, key) => {
    //stocks historical
    let stocksHistory;
    let totals = value.totals;

    if (totals) {
      stocksHistory = {
        price_percent_change_7_days: (totals.today / totals.week - 1) * 100,
        price_percent_change_14_days: (totals.today / totals.twoweek - 1) * 100,
        price_percent_change_30_days: (totals.today / totals.month - 1) * 100,
        price_percent_change_3_months:
          (totals.today / totals.threemonth - 1) * 100,
      };
    }

    let snp = await getSnPPerformance();
    stocksHistory.price_percent_change_7_days =
      stocksHistory.price_percent_change_7_days -
      snp.price_percent_change_7_days;
    stocksHistory.price_percent_change_14_days =
      stocksHistory.price_percent_change_14_days -
      snp.price_percent_change_14_days;
    stocksHistory.price_percent_change_30_days =
      stocksHistory.price_percent_change_30_days -
      snp.price_percent_change_30_days;
    stocksHistory.price_percent_change_3_months =
      stocksHistory.price_percent_change_3_months -
      snp.price_percent_change_3_months;

    //user portfolio
    let history = value.portfolio_history;
    let stocks = new Map();

    for (let i in history) {
      let ticker = history[i].ticker;
      let type = history[i].type;
      let open_price = history[i].open_price;
      let open_date = history[i].open_date;
      let close_price = history[i].close_price;
      let close_date = history[i].close_date;
      let today_date = new Date();

      if (open_price && open_date) {
        if (stocks.has(ticker)) {
          let priceChange;
          let percentChange;
          let trades = stocks.get(ticker).trades;
          if (close_price && close_date) {
            priceChange = close_price - open_price;
            percentChange = (close_price / open_price - 1) * 100;
          } else {
            let today_price = await getPrice(ticker);
            priceChange = today_price - open_price;
            percentChange = (today_price / open_price - 1) * 100;
          }
          let trade = {
            price_change: priceChange,
            performance: percentChange,
            open_date: open_date,
            open_price: open_price,
            close_date: close_date,
            close_price: close_price,
          };
          trades.push(trade);
        } else {
          let priceChange;
          let percentChange;
          let trades = [];
          if (close_price && close_date) {
            priceChange = close_price - open_price;
            percentChange = (close_price / open_price - 1) * 100;
          } else {
            let today_price = await getPrice(ticker);
            priceChange = today_price - open_price;
            percentChange = (today_price / open_price - 1) * 100;
          }
          let trade = {
            price_change: priceChange,
            performance: percentChange,
            open_date: open_date,
            open_price: open_price,
            close_date: close_date,
            close_price: close_price,
          };
          trades.push(trade);
          stocks.set(ticker, {
            type: type,
            trades: trades,
          });
        }
      }
    }

    let stocksPerformance = {};

    stocks.forEach(async (value, key) => {
      stocksPerformance[key] = {
        type: value.type,
        trades: value.trades,
      };
    });

    let followedTitans = await getTitansFollowed(key);
    let titansPerformance;
    let titansWithPerf = 0;
    let total_performance_five_year = 0;
    let total_performance_one_year = 0;
    let total_performance_quarter = 0;
    for (let i in followedTitans) {
      let uri = followedTitans[i].uri;
      let titansPerf = await getTitanPerformance(uri);

      if (titansPerf) {
        titansWithPerf += 1;
        total_performance_five_year += titansPerf.performance_five_year;
        total_performance_one_year += titansPerf.performance_one_year;
        total_performance_quarter += titansPerf.performance_quarter;
      }
    }

    if (titansWithPerf > 0) {
      titansPerformance = {
        performance_five_year: total_performance_five_year / titansWithPerf,
        performance_one_year: total_performance_one_year / titansWithPerf,
        performance_quarter: total_performance_quarter / titansWithPerf,
      };
    }

    let perf = {
      stocks_historical: stocksHistory,
      stocks: stocksPerformance,
      titans: titansPerformance,
    };

    let widget = await getLocalPerfomanceWidgetForDashboard(key);

    if (widget && widget.length > 0) {
      //update
      let widgetDataId = widget[0].widget_data_id;
      let output = perf;
      let query = {
        text:
          "UPDATE widget_data SET output = $1, updated_at = now() WHERE id = $2 RETURNING *",
        values: [output, widgetDataId],
      };

      await db(query);
      console.log("portfolio output updated");
    } else {
      //insert
      let output = perf;
      let query = {
        text:
          "INSERT INTO widget_data (output, updated_at) VALUES ($1, now()) RETURNING *",
        values: [output],
      };

      let result = await db(query);

      query = {
        text:
          "INSERT INTO widget_instances (dashboard_id, widget_id, widget_data_id, weight, is_pinned) VALUES ($1, $2, $3, $4, $5) RETURNING *",
        values: [key, userPerfWidgetId, result[0].id, 0, true],
      };

      await db(query);
      console.log("widget added and output updated");
    }
  });
}

const updateTrendingTitans = async () => {
  const start = moment().subtract(7, "d").format();
  const end = moment().format();

  const groupByTitans = await db1(`
    SELECT titan_uri, count(titan_uri) FROM titans WHERE created_at BETWEEN '${start}' AND '${end}' group by titan_uri order by COUNT DESC
  `);

  let titans = [];

  for (const titan of groupByTitans) {
    const { titan_uri, count } = titan;

    const titanData = await db(`
      SELECT * FROM billionaires WHERE uri = '${titan_uri}'
    `);

    if (titanData && titanData.length > 0) {
      titans.push({
        id: titanData[0].id,
        name: titanData[0].name,
        photo_url: titanData[0].photo_url,
        uri: titanData[0].uri,
        views: count,
      });
    }
  }

  if (titans.length > 25) {
    titans.length = 25;
  }

  return titans;
};

//test
