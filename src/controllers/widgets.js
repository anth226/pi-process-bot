import db from "../db";
import axios from "axios";
import cheerio from "cheerio";
import intrinioSDK from "intrinio-sdk";
import * as getSecurityData from "./intrinio/get_security_data";
import * as queue from "../queue";
import * as companies from "./companies";
import * as institutions from "./institutions";
import * as mutualfunds from "./mutualfunds";
import * as etfs from "./etfs";
import * as securities from "./securities";

// init intrinio
intrinioSDK.ApiClient.instance.authentications["ApiKeyAuth"].apiKey =
  process.env.INTRINIO_API_KEY;

intrinioSDK.ApiClient.instance.basePath = `${process.env.INTRINIO_BASE_PATH}`;

const companyAPI = new intrinioSDK.CompanyApi();
const securityAPI = new intrinioSDK.SecurityApi();
const indexAPI = new intrinioSDK.IndexApi();

/* START Scraper */

const s3AllInsider = `https://${process.env.AWS_BUCKET_TERMINAL_SCRAPE}.s3.amazonaws.com/all-insider-trading/allInsider.json`;

async function getAllInsider() {
  try {
    const response = await axios.get(s3AllInsider);
    return response.data;
  } catch (error) {
    console.error(error);
  }
}

export async function getSecurityLastPrice(symbol) {
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

  let price = await lastPrice.then((data) => data.data);

  if (price) {
    return price;
  } else {
    let backupLastPrice = axios
      .get(
        `${process.env.INTRINIO_BASE_PATH}/securities/${symbol}/prices/realtime?source=bats_delayed&api_key=${process.env.INTRINIO_API_KEY}`
      )
      .then(function (res) {
        return res;
      })
      .catch(function (err) {
        return err;
      });

    return backupLastPrice.then((data) => data.data);
  }
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

export async function getTitansFollowed(dashboard_id) {
  let result = await db(`
    SELECT bw.titan_id, d.id AS dashboard_id, b.uri
    FROM billionaire_watchlists AS bw
    JOIN dashboards AS d ON bw.user_id = d.user_id
    JOIN billionaires AS b ON b.id = bw.titan_id
    WHERE d.id = ${dashboard_id}
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
    else if (type == "UsersPortfolioPerf") {
      // if (params.count && params.count > 0) {
      //   //let count = params.count;
      //   //let topComps = { topComps: await getInsidersNMovers(count) };
      //   if (topComps) {
      //     output = topComps;
      //   }
      // }
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
      let data = await getTrendingTitans();
      let json = JSON.stringify(data);

      if (data) {
        output = json;
      }
    }
    /*          COMPANIES */
    //Strong Buys
    else if (type == "CompanyStrongBuys") {
      let data = await getStrongBuys();
      let json = JSON.stringify(data);

      if (data) {
        output = json;
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
        let ticker = params.ticker;
        let price = await getCompanyPrice(ticker);
        let comp = await companies.getCompanyByTicker(ticker);
        let metrics = await companies.getCompanyMetrics(ticker);
        let performance = await getSecurityPerformance(ticker);

        if (
          performance &&
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
            performance: performance,
          };
          output = tick;
        }
      }
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
        let ticker = params.ticker;
        let price = await getCompanyPrice(ticker);
        let fund = await mutualfunds.getMutualFundByTicker(ticker);
        let metrics = await companies.getCompanyMetrics(ticker);
        let performance = await getSecurityPerformance(ticker);

        if (
          performance &&
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
        let ticker = params.ticker;
        let price = await getCompanyPrice(ticker);
        let etf = await etfs.getETFByTicker(ticker);
        let metrics = await companies.getCompanyMetrics(ticker);
        let performance = await getSecurityPerformance(ticker);

        if (
          performance &&
          price &&
          etf &&
          etf.json &&
          etf.json.name &&
          metrics &&
          metrics.Change
        ) {
          let delta = metrics.Change;
          let tick = {
            ticker: ticker,
            name: etf.json.name,
            price: price,
            delta: delta,
            performance: performance,
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

export async function getCompanyPrice(ticker) {
  let data = await getSecurityLastPrice(ticker);
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

export async function getStrongBuys() {
  let buys = [];
  const url = `${process.env.INTRINIO_BASE_PATH}/securities/screen?order_column=zacks_analyst_rating_strong_buys&order_direction=desc&page_size=9&api_key=${process.env.INTRINIO_API_KEY}`;
  const body = {
    operator: "AND",
    clauses: [
      {
        field: "zacks_analyst_rating_strong_buys",
        operator: "gt",
        value: "0",
      },
    ],
  };

  let res = axios
    .post(url, body)
    .then(function (data) {
      //console.log(data);
      return data;
    })
    .catch(function (err) {
      console.log(err);
      return err;
    });

  let data = await res.then((data) => data.data);
  for (let i in data) {
    let delta;
    let ticker = data[i].security.ticker;
    let compTicker = data[i].security.composite_ticker;
    let name = data[i].security.name;
    let numberBuys = data[i].data[0].number_value;
    let price = await getCompanyPrice(ticker);
    let metrics = await companies.getCompanyMetrics(ticker);
    if (metrics) {
      delta = metrics.Change;
    }
    //let pic = await getCompanyPicture(ticker);
    buys.push({
      ticker: ticker,
      composite_ticker: compTicker,
      name: name,
      strong_buys: numberBuys,
      last_price: price,
      delta: delta,
      //pic_url: ,
    });
  }
  return buys;
}

export async function getAggRatings() {
  let comps = [];
  const url = `${process.env.INTRINIO_BASE_PATH}/securities/screen?order_column=zacks_analyst_rating_mean&order_direction=desc&page_size=66&api_key=${process.env.INTRINIO_API_KEY}`;
  const body = {
    operator: "AND",
    clauses: [
      {
        field: "zacks_analyst_rating_mean",
        operator: "gt",
        value: "0",
      },
    ],
  };

  let res = axios
    .post(url, body)
    .then(function (data) {
      //console.log(data);
      return data;
    })
    .catch(function (err) {
      console.log(err);
      return err;
    });

  let data = await res.then((data) => data.data);
  let rank = 0;
  for (let i in data) {
    rank += 1;
    let ticker = data[i].security.ticker;
    let name = data[i].security.name;
    let rating = data[i].data[0].number_value;

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
  for (let i = 0; i < 24; i++) {
    let id = holdingsSorted[i].id;
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
    let price = await getCompanyPrice(ticker);
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
  let secs = [];
  let data = await securities.getSecurities();

  for (let i in data) {
    let ticker = data[i].ticker;
    let name = data[i].name;
    let jsonEarnings = data[i].json_earnings;

    if (jsonEarnings) {
      let earningsDate = jsonEarnings.earnings_date;
      let time_of_day = jsonEarnings.time_of_day;
      let eps_actual = jsonEarnings.eps_actual;
      let eps_estimate = jsonEarnings.eps_estimate;
      let suprise_percentage = jsonEarnings.suprise_percentage;
      secs.push({
        earnings_date: earningsDate,
        ticker: ticker,
        name: name,
        time_of_day: time_of_day,
        eps_actual: eps_actual,
        eps_estimate: eps_estimate,
        suprise_percentage: suprise_percentage,
      });
    }
  }

  let sorted = secs.sort(function (a, b) {
    var aa = a.earnings_date.split("-").join(),
      bb = b.earnings_date.split("-").join();
    return aa < bb ? -1 : aa > bb ? 1 : 0;
  });

  return sorted;
}

export async function getSecurityPerformance(ticker) {
  let data = await getSecurityData.getChartData(securityAPI, ticker);
  if (
    data.daily[0] &&
    data.daily[6] &&
    data.daily[13] &&
    data.daily[29] &&
    data.daily[87] &&
    data.daily[0].value &&
    data.daily[6].value &&
    data.daily[13].value &&
    data.daily[29].value &&
    data.daily[87].value
  ) {
    let perf = {
      price_percent_change_7_days:
        (data.daily[0].value / data.daily[6].value - 1) * 100,
      price_percent_change_14_days:
        (data.daily[0].value / data.daily[13].value - 1) * 100,
      price_percent_change_30_days:
        (data.daily[0].value / data.daily[29].value - 1) * 100,
      price_percent_change_3_months:
        (data.daily[0].value / data.daily[87].value - 1) * 100,
      values: {
        today: data.daily[0],
        week: data.daily[6],
        twoweek: data.daily[13],
        month: data.daily[29],
        threemonth: data.daily[87],
      },
    };
    return perf;
  }
  return data;
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

  let perf = {
    performance_five_year: json.performance_five_year,
    performance_one_year: json.performance_one_year,
    performance_quarter: json.performance_quarter,
  };

  return perf;
}

export async function processUsersPortPerf() {
  let res = await getWidgetTypeId("UsersPerformance");
  let userPerfWidgetId = res[0].id;
  let widgets = await getLocalPriceWidgets();
  let dashboards = new Map();

  for (let i in widgets) {
    let dashboardId = widgets[i].dashboard_id;
    let values = widgets[i].output.performance.values;
    if (dashboards.has(dashboardId)) {
      let totals = dashboards.get(dashboardId);
      let today = totals.today + values.today.value;
      let week = totals.week + values.week.value;
      let twoweek = totals.twoweek + values.twoweek.value;
      let month = totals.month + values.month.value;
      let threemonth = totals.threemonth + values.threemonth.value;
      dashboards.set(dashboardId, {
        today: today,
        week: week,
        twoweek: twoweek,
        month: month,
        threemonth: threemonth,
      });
    } else {
      dashboards.set(dashboardId, {
        today: values.today.value,
        week: values.week.value,
        twoweek: values.twoweek.value,
        month: values.month.value,
        threemonth: values.threemonth.value,
      });
    }
  }

  dashboards.forEach(async (value, key) => {
    // console.log("key", key);
    // console.log("value", value);
    let stocksPerformance = {
      price_percent_change_7_days: (value.today / value.week - 1) * 100,
      price_percent_change_14_days: (value.today / value.twoweek - 1) * 100,
      price_percent_change_30_days: (value.today / value.month - 1) * 100,
      price_percent_change_3_months: (value.today / value.threemonth - 1) * 100,
    };

    let followedTitans = await getTitansFollowed(key);
    let total_performance_five_year;
    let total_performance_one_year;
    let total_performance_quarter;
    for (let i in followedTitans) {
      let uri = followedTitans[i].uri;
      let titansPerf = await getTitanPerformance(uri);
      total_performance_five_year += titansPerf.performance_five_year;
      total_performance_one_year += titansPerf.performance_one_year;
      total_performance_quarter += titansPerf.performance_quarter;
    }

    let titansPerformance = {
      performance_five_year:
        total_performance_five_year / followedTitans.length,
      performance_one_year: total_performance_one_year / followedTitans.length,
      performance_quarter: total_performance_quarter / followedTitans.length,
    };

    let perf = {
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
      console.log("output updated");
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
