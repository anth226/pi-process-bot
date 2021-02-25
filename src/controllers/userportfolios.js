import db from "../db";
import * as queue from "../queue";
import size from "lodash/size";

import * as widgets from "./widgets";
import * as institutions from "./institutions";
import * as securities from "./securities";
import * as companies from "./companies";

export async function getPortfolios() {
  let result = await db(`
    SELECT *
    FROM portfolios
    `);

  return result;
}

export async function getPortfolioByDashboardID(dashbardId) {
  let dashbard_id = parseInt(dashbardId);
  let result = await db(`
      SELECT *
      FROM portfolios
      WHERE dashboard_id = ${dashbard_id}
    `);

  return result[0];
}

export async function getPortfolioHistory(portfolioId) {
  let result = await db(`
      SELECT *
      FROM portfolio_histories
      WHERE portfolio_id = ${portfolioId}
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

  if (!data.summary) {
    return;
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

export async function getIfStocksPinned(portId, priceWidgets) {
  for (let i in priceWidgets) {
    let dashboardId = priceWidgets[i].dashboard_id;
    if (dashboardId == portId) {
      return true;
    }
  }
  return false;
}

export const getPinnedPortfolios = async () => {
  const result = await db(`
    SELECT portfolios.id
    from portfolios
    JOIN portfolio_histories ON portfolios.id = portfolio_histories.portfolio_id 
    WHERE close_date is null and type in ('common_stock', 'etf') GROUP BY portfolios.id
  `);
  return result
}

export async function fillUsersPortPerfs() {
  let ports = [];
  // get portfolios
  let result = await getPinnedPortfolios();

  if (result && result.length > 0) {
    for (let i in result) {
      let portId = result[i].id;
      if (!ports.includes(portId)) {
        ports.push(portId);
      }
    }
  }
  for (let i in ports) {
    await queue.publish_ProcessPerformances_UserPortfolios(ports[i]);
  }
}

export async function fillUserPortPerf(portId) {
  await queue.publish_ProcessPerformances_UserPortfolios(portId);
}

export async function getStocksHistorical(priceWidgets) {
  // dont forget missing records from sec table, need backfill during widget price pinning
  //
  //
  let stocksHistory;
  let totals = {
    today: 0,
    week: 0,
    twoweek: 0,
    month: 0,
    threemonth: 0,
    year: 0,
  };
  for (let i in priceWidgets) {
    let ticker = priceWidgets[i].ticker;
    // can also get security perf directly instead of db
    let sec = await securities.getSecurityByTicker(ticker);
    if (sec && sec.perf_values) {
      let perfVals = sec.perf_values;
      totals.today += perfVals.today.value;
      totals.week += perfVals.week.value;
      totals.twoweek += perfVals.twoweek.value;
      totals.month += perfVals.month.value;
      totals.threemonth += perfVals.threemonth.value;
      //totals.year += perfVals.year.value;
    }
  }
  if (totals.today > 0) {
    stocksHistory = {
      price_percent_change_7_days: (totals.today / totals.week - 1) * 100,
      price_percent_change_14_days: (totals.today / totals.twoweek - 1) * 100,
      price_percent_change_30_days: (totals.today / totals.month - 1) * 100,
      price_percent_change_3_months:
        (totals.today / totals.threemonth - 1) * 100,
    };

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
  }
  return stocksHistory;
}

export async function getStocks(portId) {
  let result = await getPortfolioHistory(portId);
  if (!result || result.length < 1) {
    return;
  }
  let stocks = new Map();

  for (let i in result) {
    let ticker = result[i].ticker;
    let type = result[i].type;
    let open_price = result[i].open_price;
    let open_date = result[i].open_date;
    let close_price = result[i].close_price;
    let close_date = result[i].close_date;
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
          let today_price = await widgets.getPrice(ticker);
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
          let today_price = await widgets.getPrice(ticker);
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
  return stocksPerformance;
}

export async function getTitans(portId) {
  let followedTitans = await getTitansFollowed(portId);
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
  return titansPerformance;
}

export async function insertUserPortPerf(
  portId,
  stocksHistorical,
  stocks,
  titans
) {
  try {
    //update
    let query = {
      text:
        "UPDATE portfolios SET stocks_historical = $2, stocks = $3, titans = $4 WHERE id = $1",
      values: [portId, stocksHistorical, stocks, titans],
    };

    const performanceData = await db(`
      SELECT * FROM portfolio_performances WHERE portfolio_id = ${portId}
    `)

    if (size(performanceData) !== 0) {
      const updateQuery = {
        text:
          "UPDATE portfolio_performances SET price_percent_change_7_days = $2, price_percent_change_14_days = $3, price_percent_change_30_days = $4, price_percent_change_3_months = $5 WHERE id = $1",
        values: [portId, stocksHistorical.price_percent_change_7_days, stocksHistorical.price_percent_change_14_days, stocksHistorical.price_percent_change_30_days, stocksHistorical.price_percent_change_3_months],
      }

      await db(updateQuery);
    } else {
      const insertQuery = {
        text:
          "INSERT INTO portfolio_performances (portfolio_id, price_percent_change_7_days, price_percent_change_14_days , price_percent_change_30_days, price_percent_change_3_months) VALUES ($1, $2, $3, $4, $5)",
        values: [portId, stocksHistorical.price_percent_change_7_days, stocksHistorical.price_percent_change_14_days, stocksHistorical.price_percent_change_30_days, stocksHistorical.price_percent_change_3_months],
      }

      await db(insertQuery);
    }

    await db(query);
    console.log("portfolio updated");
  } catch (error) {
    console.error(error)
  }
}

// export async function processUsersPortPerf() {
//   //let res = await getWidgetTypeId("UsersPerformance");
//   //let userPerfWidgetId = res[0].id;
//   let widgets = await widgets.getLocalPriceWidgets();
//   //console.log("widgets", widgets);
//   let dashboards = new Map();

//   for (let i in widgets) {
//     let values;
//     let dashboardId = widgets[i].dashboard_id;
//     let output = widgets[i].output;
//     if (output && output.performance && output.performance.values) {
//       values = output.performance.values;
//     }

//     if (dashboards.has(dashboardId)) {
//       //stocks historical
//       if (values) {
//         let totals = dashboards.get(dashboardId).totals;
//         if (totals) {
//           let today = totals.today + values.today.value;
//           let week = totals.week + values.week.value;
//           let twoweek = totals.twoweek + values.twoweek.value;
//           let month = totals.month + values.month.value;
//           let threemonth = totals.threemonth + values.threemonth.value;
//           totals = {
//             today: today,
//             week: week,
//             twoweek: twoweek,
//             month: month,
//             threemonth: threemonth,
//           };
//         }
//       }
//     } else {
//       //stocks historical
//       let totals;
//       if (values) {
//         totals = {
//           today: values.today.value,
//           week: values.week.value,
//           twoweek: values.twoweek.value,
//           month: values.month.value,
//           threemonth: values.threemonth.value,
//         };
//       }

//       //user portfolio
//       let portfolio = await getPortfolioByDashboardID(dashboardId);
//       let portfolioId = portfolio.id;
//       let portfolioHistory = await getPortfolioHistory(portfolioId);

//       dashboards.set(dashboardId, {
//         portfolio_id: portfolioId,
//         portfolio_history: portfolioHistory,
//         totals: totals,
//       });
//     }
//   }

//   dashboards.forEach(async (value, key) => {
//     //stocks historical
//     let stocksHistory;
//     let totals = value.totals;

//     if (totals) {
//       stocksHistory = {
//         price_percent_change_7_days: (totals.today / totals.week - 1) * 100,
//         price_percent_change_14_days: (totals.today / totals.twoweek - 1) * 100,
//         price_percent_change_30_days: (totals.today / totals.month - 1) * 100,
//         price_percent_change_3_months:
//           (totals.today / totals.threemonth - 1) * 100,
//       };
//     }

//     let snp = await getSnPPerformance();
//     stocksHistory.price_percent_change_7_days =
//       stocksHistory.price_percent_change_7_days -
//       snp.price_percent_change_7_days;
//     stocksHistory.price_percent_change_14_days =
//       stocksHistory.price_percent_change_14_days -
//       snp.price_percent_change_14_days;
//     stocksHistory.price_percent_change_30_days =
//       stocksHistory.price_percent_change_30_days -
//       snp.price_percent_change_30_days;
//     stocksHistory.price_percent_change_3_months =
//       stocksHistory.price_percent_change_3_months -
//       snp.price_percent_change_3_months;

//     //user portfolio
//     let history = value.portfolio_history;
//     let stocks = new Map();

//     for (let i in history) {
//       let ticker = history[i].ticker;
//       let type = history[i].type;
//       let open_price = history[i].open_price;
//       let open_date = history[i].open_date;
//       let close_price = history[i].close_price;
//       let close_date = history[i].close_date;
//       let today_date = new Date();

//       if (open_price && open_date) {
//         if (stocks.has(ticker)) {
//           let priceChange;
//           let percentChange;
//           let trades = stocks.get(ticker).trades;
//           if (close_price && close_date) {
//             priceChange = close_price - open_price;
//             percentChange = (close_price / open_price - 1) * 100;
//           } else {
//             let today_price = await widgets.getPrice(ticker);
//             priceChange = today_price - open_price;
//             percentChange = (today_price / open_price - 1) * 100;
//           }
//           let trade = {
//             price_change: priceChange,
//             performance: percentChange,
//             open_date: open_date,
//             open_price: open_price,
//             close_date: close_date,
//             close_price: close_price,
//           };
//           trades.push(trade);
//         } else {
//           let priceChange;
//           let percentChange;
//           let trades = [];
//           if (close_price && close_date) {
//             priceChange = close_price - open_price;
//             percentChange = (close_price / open_price - 1) * 100;
//           } else {
//             let today_price = await widgets.getPrice(ticker);
//             priceChange = today_price - open_price;
//             percentChange = (today_price / open_price - 1) * 100;
//           }
//           let trade = {
//             price_change: priceChange,
//             performance: percentChange,
//             open_date: open_date,
//             open_price: open_price,
//             close_date: close_date,
//             close_price: close_price,
//           };
//           trades.push(trade);
//           stocks.set(ticker, {
//             type: type,
//             trades: trades,
//           });
//         }
//       }
//     }

//     let stocksPerformance = {};

//     stocks.forEach(async (value, key) => {
//       stocksPerformance[key] = {
//         type: value.type,
//         trades: value.trades,
//       };
//     });

//     let followedTitans = await getTitansFollowed(key);
//     let titansPerformance;
//     let titansWithPerf = 0;
//     let total_performance_five_year = 0;
//     let total_performance_one_year = 0;
//     let total_performance_quarter = 0;
//     for (let i in followedTitans) {
//       let uri = followedTitans[i].uri;
//       let titansPerf = await getTitanPerformance(uri);

//       if (titansPerf) {
//         titansWithPerf += 1;
//         total_performance_five_year += titansPerf.performance_five_year;
//         total_performance_one_year += titansPerf.performance_one_year;
//         total_performance_quarter += titansPerf.performance_quarter;
//       }
//     }

//     if (titansWithPerf > 0) {
//       titansPerformance = {
//         performance_five_year: total_performance_five_year / titansWithPerf,
//         performance_one_year: total_performance_one_year / titansWithPerf,
//         performance_quarter: total_performance_quarter / titansWithPerf,
//       };
//     }

//     let perf = {
//       stocks_historical: stocksHistory,
//       stocks: stocksPerformance,
//       titans: titansPerformance,
//     };

//     //      replace with check to portfolios
//     let portfolio = await getPortfolioByDashboardID(dashboardId);

//     if (widget && widget.length > 0) {
//       //update
//       let widgetDataId = widget[0].widget_data_id;
//       let output = perf;
//       let query = {
//         text:
//           "UPDATE widget_data SET output = $1, updated_at = now() WHERE id = $2 RETURNING *",
//         values: [output, widgetDataId],
//       };

//       await db(query);
//       console.log("portfolio output updated");
//     } else {
//       //insert
//       let output = perf;
//       let query = {
//         text:
//           "INSERT INTO widget_data (output, updated_at) VALUES ($1, now()) RETURNING *",
//         values: [output],
//       };

//       let result = await db(query);

//       query = {
//         text:
//           "INSERT INTO widget_instances (dashboard_id, widget_id, widget_data_id, weight, is_pinned) VALUES ($1, $2, $3, $4, $5) RETURNING *",
//         values: [key, userPerfWidgetId, result[0].id, 0, true],
//       };

//       await db(query);
//       console.log("widget added and output updated");
//     }
//   });
// }
