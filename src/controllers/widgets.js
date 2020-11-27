import db from "../db";
import axios from "axios";
import cheerio from "cheerio";
import * as queue from "../queue";
import * as companies from "./companies";
import * as mutualfunds from "./mutualfunds";
import * as etfs from "./etfs";
import * as securities from "./securities";

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
    //Earnings Calendar
    else if (type == "CompanyEarningsCalendar") {
      let data = await getEarningsCalendar();
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
    /*          ETFS */
    //Price
    else if (type == "ETFPrice") {
      if (params.ticker) {
        let ticker = params.ticker;
        let price = await getCompanyPrice(ticker);
        let etf = await etfs.getETFByTicker(ticker);
        let metrics = await companies.getCompanyMetrics(ticker);

        if (
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

    if (output) {
      let query = {
        text:
          "UPDATE widget_data SET output = $2, updated_at = now() WHERE id = $1",
        values: [dataId, output],
      };
      await db(query);
      console.log("output updated");
    } else if (!output && dashboardId != 0) {
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

export async function getEarningsDates() {
  let comps = [];
  let today = new Date().toISOString().slice(0, 10);

  let url = `${process.env.INTRINIO_BASE_PATH}/securities/screen?order_column=next_earnings_date&order_direction=asc&page_size=50000&api_key=${process.env.INTRINIO_API_KEY}`;
  const body = {
    operator: "AND",
    clauses: [
      {
        field: "next_earnings_date",
        operator: "gt",
        value: "2021-01-20",
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

  return res.then((data) => data.data);
}

export async function getEarningsCalendar() {
  let comps = [];

  let data = await getEarningsDates();
  for (let i in data) {
    let time_of_day;
    let estimatedEPS;
    let actualEPS;
    let suprisePercent;
    let ticker = data[i].security.ticker;
    let name = data[i].security.name;
    let earningsDate = data[i].data[0].text_value;

    if (ticker) {
      let url = `${process.env.INTRINIO_BASE_PATH}/securities/${ticker}/earnings/latest?api_key=${process.env.INTRINIO_API_KEY}`;

      let res = await axios.get(url);

      if (res.data && res.data.time_of_day) {
        time_of_day = res.data.time_of_day;
      }

      url = `${process.env.INTRINIO_BASE_PATH}/securities/${ticker}/zacks/eps_surprises?api_key=${process.env.INTRINIO_API_KEY}`;

      res = await axios.get(url);

      if (res.data && res.data.eps_surprises.length > 0) {
        let suprise = res.data.eps_surprises[0];
        estimatedEPS = suprise.eps_mean_estimate;
        actualEPS = suprise.eps_actual;
        suprisePercent = suprise.eps_percent_diff;
      }

      comps.push({
        ticker: ticker,
        name: name,
        earnings_date: earningsDate,
        time_of_day: time_of_day,
        eps_actual: actualEPS,
        eps_estimate: estimatedEPS,
        suprise_percentage: suprisePercent,
      });
      console.log(ticker, "earnings date:", earningsDate);
    }
  }
  return comps;
}
