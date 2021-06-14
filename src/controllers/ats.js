import atsDb from "../atsDB";
import eqDb from "../equitiesDb"
import * as darkpool from "./darkpool"
import optionsDb from "../optionsProdDb"
import { connectATSCache, connectATSFallbackCache, ATS_HIGH_DARK_FLOW, ATS_TRENDING_HIGH_DARK_FLOW, ATS_TOP_TICKERS, ATS_HISTORICAL_TRENDING_HIGH_DARK_FLOW, connectChartRedis, connectPriceCache } from '../redis';
import { getEnv } from "../env";
import { filterSecurityNames } from "../controllers/securities"
import { getLastPrice, getTrendOpenPrice } from "./quodd";
import moment from "moment";
import chartHistDb from "../chartHistoryDb";


export const get30DaysClosePrice = async (ticker) => {
    let result = await chartHistDb(`
        SELECT close
        FROM charts_history
        WHERE ticker = '${ticker}'
        AND time >= (SELECT MAX(time) FROM charts_history WHERE ticker = '${ticker}') - INTERVAL '30 DAY'
        ORDER BY time DESC
        LIMIT 1
    `);

    if (result.length > 0) {
        return result[0];
    }
    return null;
};

const getHistoricalHighDarkFlow = async () => {
    let dates;

    let firstDateQuery = `
        SELECT date FROM minutes ORDER BY date DESC LIMIT 1
    `;
    let firstDate = await atsDb(firstDateQuery);

    firstDate = moment(firstDate[0].date).format("YYYY-MM-DD");

    let dateQuery = `
        SELECT DISTINCT(date) FROM minutes
        WHERE date >= ('${firstDate}'::date - INTERVAL '7 DAY')
        AND date < '${firstDate}'::date
        ORDER BY date DESC
        `;

    dates = await atsDb(dateQuery);

    let currNumDates = dates.length || 1;

    dateQuery = `
        SELECT DISTINCT(date) FROM minutes
        WHERE date >= ('${firstDate}'::date - INTERVAL '30 DAY')
        AND date < ('${firstDate}'::date - INTERVAL '7 DAY') 
        ORDER BY date DESC
        `;

    dates = await atsDb(dateQuery);

    let prevNumDates = dates.length || 1;

    let atsQuery = `
        SELECT ticker, (current_dollar_volume/previous_dollar_value) as multiplier, current_dollar_volume as dollar_value
        FROM (
            SELECT ticker, 
            SUM(current_dollar_volume)/${currNumDates} as current_dollar_volume, 
            SUM(previous_dollar_value)/${prevNumDates} as previous_dollar_value, 
            SUM(current_total_trades)/${currNumDates} as current_total_trades, 
            SUM(previous_total_trades)/${prevNumDates} as previous_total_trades
            FROM (
                    (
                    SELECT ticker, 
                    SUM("totalTrades") as current_total_trades,
                    0 as previous_total_trades,
                    (SUM("totalPrice")/SUM("totalTrades")*SUM("totalVolume")) as current_dollar_volume,
                    0 as previous_dollar_value
                    FROM minutes
                    WHERE date >= ('${firstDate}'::date - INTERVAL '7 DAY') 
                    GROUP BY ticker
                    ) UNION (
                    SELECT ticker,
                    0 as current_total_trades,
                    SUM("totalTrades") as previous_total_trades,
                    0 as current_dollar_volume,
                    (SUM("totalPrice")/SUM("totalTrades")*SUM("totalVolume")) as previous_dollar_value
                    FROM minutes
                    WHERE date >= ('${firstDate}'::date - INTERVAL '30 DAY')
                    AND date < ('${firstDate}'::date - INTERVAL '7 DAY') 
                    AND "openTime" <= (SELECT "openTime" FROM minutes ORDER BY date DESC, "openTime" DESC LIMIT 1)
                    GROUP BY ticker
                    )
                ) current_previous group by ticker 
            ) ats
        WHERE current_dollar_volume > 0 AND previous_dollar_value > 0
        AND (current_dollar_volume/previous_dollar_value) >= 1.5
        ORDER BY multiplier DESC
        `;

    const results = await atsDb(atsQuery);

    return results
}

const getHighDarkFlow = async () => {
    let dates;

    let dateQuery = `
        SELECT DISTINCT(date) FROM minutes
        WHERE date >= ((SELECT date FROM minutes ORDER BY date DESC LIMIT 1) - INTERVAL '7 DAY')
        AND date < (SELECT date FROM minutes ORDER BY date DESC LIMIT 1)
        ORDER BY date DESC
        `;

    dates = await atsDb(dateQuery);

    let numDates = dates.length || 1;

    let atsQuery = `
        SELECT ticker, (current_dollar_volume/previous_dollar_value) as multiplier, current_dollar_volume as dollar_value
        FROM (
            SELECT ticker, 
            SUM(current_dollar_volume) as current_dollar_volume, 
            SUM(previous_dollar_value)/${numDates} as previous_dollar_value, 
            SUM(current_total_trades) as current_total_trades, 
            SUM(previous_total_trades)/${numDates} as previous_total_trades
            FROM (
                    (
                    SELECT ticker, 
                    SUM("totalTrades") as current_total_trades,
                    0 as previous_total_trades,
                    (SUM("totalPrice")/SUM("totalTrades")*SUM("totalVolume")) as current_dollar_volume,
                    0 as previous_dollar_value
                    FROM minutes
                    WHERE date = (SELECT date FROM minutes ORDER BY date DESC LIMIT 1) 
                    GROUP BY ticker
                    ) UNION (
                    SELECT ticker,
                    0 as current_total_trades,
                    SUM("totalTrades") as previous_total_trades,
                    0 as current_dollar_volume,
                    (SUM("totalPrice")/SUM("totalTrades")*SUM("totalVolume")) as previous_dollar_value
                    FROM minutes
                    WHERE date >= ((SELECT date FROM minutes ORDER BY date DESC LIMIT 1) - INTERVAL '7 DAY')
                    AND date < (SELECT date FROM minutes ORDER BY date DESC LIMIT 1) 
                    AND "openTime" <= (SELECT "openTime" FROM minutes ORDER BY date DESC, "openTime" DESC LIMIT 1)
                    GROUP BY ticker
                    )
                ) current_previous group by ticker 
            ) ats
        WHERE current_dollar_volume > 0 AND previous_dollar_value > 0
        AND (current_dollar_volume/previous_dollar_value) >= 1.5
        ORDER BY multiplier DESC
        `;

    const results = await atsDb(atsQuery);

    return results
}

export const filterOptions = async (data) => {
    let tickers = data.map(s => s.ticker).join(",");

    let optionsQuery = `
                SELECT DISTINCT(ticker)
                FROM options
                WHERE to_timestamp(time)::date >= (SELECT to_timestamp(time)::date - INTERVAL '3 DAY' FROM options ORDER BY time DESC LIMIT 1)
                AND ticker = ANY('{${tickers}}')
                `;

    const options = await optionsDb(optionsQuery);

    if (options && options.length > 0) {
        const optionsMap = options.reduce((o, option) => ({ ...o, [option.ticker]: true }), {});

        data = data.map(s => {
            s.has_options = optionsMap[s.ticker];
            return s;
        }).filter(s => s.has_options);
    }

    return data;
}

export const processHighDarkFlow = async (req) => {
    let darkFlow = await getHighDarkFlow();

    if (darkFlow) {
        darkFlow = await filterSecurityNames(darkFlow);
        darkFlow = await filterOptions(darkFlow);

        if (darkFlow.length > 25) {
            darkFlow.length = 25
        }

        let jsonDarkflow = JSON.stringify(darkFlow);

        let atsCache = connectATSCache();
        atsCache.set(`${ATS_HIGH_DARK_FLOW}`, jsonDarkflow);

    }

    return darkFlow
};

export const processTrendingHighDarkflow = async (req) => {
    let darkFlow = await getHighDarkFlow();

    if (darkFlow) {
        let totalTickers = 16;
        let all;
        darkFlow = await filterSecurityNames(darkFlow);
        darkFlow = await filterOptions(darkFlow);

        // split darkflow between trending up and down
        // trending up: positive performance since prev close or open; trending down: negative since prev close or open
        let trendingUp = [];
        let trendingDown = [];

        // first split darkflow into trending up and trending down
        // then split trending up into trending down if needed
        for (let df of darkFlow) {
            // fetch performance
            let currentPrice = await getLastPrice(df.ticker);
            currentPrice = currentPrice.last_price;
            let openPrice = await getTrendOpenPrice(df.ticker);
            let perf = (currentPrice / openPrice - 1) * 100
            df.trend_start = openPrice;
            df.perf = perf;

            if (perf > 0) {
                trendingUp.push(df);
            } else {
                trendingDown.push(df);
            }
        }

        trendingUp.sort(function (a, b) {
            return b.perf - a.perf;
        });

        trendingDown.sort(function (a, b) {
            return a.perf - b.perf;
        });

        if (trendingUp.length > totalTickers) {
            trendingUp.length = totalTickers
        }
        if (trendingDown.length > totalTickers) {
            trendingDown.length = totalTickers
        }

        all = trendingUp.concat(trendingDown);

        const promises = all.map(sec => getAllData(sec.ticker, sec.multiplier, sec.name, sec.trend_start, sec.perf));

        const topTicks = await Promise.all(promises);

        let atsCache = connectATSCache();

        let data = JSON.stringify(topTicks);

        atsCache.set(`${ATS_TOP_TICKERS}`, data);

        console.log("FINISHED DARKFLOW PROCESSING");

        // return darkFlow
    }

    // return darkFlow
};


export const processHistoricalTrendingHighDarkflow = async (req) => {
    let darkFlow = await getHistoricalHighDarkFlow();

    if (darkFlow) {
        darkFlow = await filterSecurityNames(darkFlow);
        darkFlow = await filterOptions(darkFlow);

        // split darkflow between trending up and down
        // trending up: positive performance since prev close or open; trending down: negative since prev close or open
        let trendingUp = [];
        let trendingDown = [];

        // first split darkflow into trending up and trending down
        // then split trending up into trending down if needed
        for (let df of darkFlow) {
            // fetch performance
            let currentPrice = await getLastPrice(df.ticker);
            currentPrice = currentPrice.last_price;
            let oldPrice = await get30DaysClosePrice(df.ticker);
            oldPrice = oldPrice.close;
            let perf = (currentPrice / oldPrice - 1) * 100
            df.trend_start = oldPrice;
            df.perf = perf;

            if (perf > 0) {
                trendingUp.push(df);
            } else {
                trendingDown.push(df);
            }
        }

        trendingUp.sort(function (a, b) {
            return b.multiplier - a.multiplier;
        });

        trendingDown.sort(function (a, b) {
            return b.multiplier - a.multiplier;
        });

        if (trendingUp.length > 25) {
            trendingUp.length = 25
        }
        if (trendingDown.length > 25) {
            trendingDown.length = 25
        }

        darkFlow = { trending_up: trendingUp, trending_down: trendingDown };

        let jsonDarkflow = JSON.stringify(darkFlow);

        let atsCache = connectATSCache();
        atsCache.set(`${ATS_HISTORICAL_TRENDING_HIGH_DARK_FLOW}`, jsonDarkflow);

        return darkFlow
    }

    return darkFlow
};

async function fetchATSData(dates, ticker, jMin, day) {
    let current = {};

    let atsCache = connectATSCache();

    let trades = 0;
    let volume = 0;
    let price = 0;
    let num = 0;
    let score;
    for (let date of dates) {
        let minute = await atsCache.zrevrangebyscore(`DAY:${date}:${ticker}`, jMin, 0, 'WITHSCORES', 'LIMIT', 0, 1);
        if (!minute || !minute[0] || !minute[1]) {
            continue;
        }
        let minuteData = JSON.parse(minute[0]);
        trades += minuteData.totalTrades;
        volume += minuteData.totalVolume;
        price += minuteData.totalPrice;

        score = minute[1];
        num += 1;
    }

    let days = num || 1;
    current.trades = Number(trades / days);
    current.volume = Number(volume / days);
    current.price = Number(price / days);

    // if day exists, subtract the above result from the current day to find the remaining data
    if (day) {
        current.trades = Number(day.day_trades - current.trades);
        current.volume = Number(day.day_volume - current.volume);
        current.price = Number(day.day_price - current.price);
    }
    current.dollar_volume = current.trades ? (current.price / current.trades) * current.volume : 0;
    current.score = score;

    return current;
}

export const getAllData = async (ticker, multiplier, name, trend_start, perf) => {
    let jMin = 2000;
    let all = {};
    let ats = {};
    all.ticker = ticker;
    all.multiplier = multiplier;
    all.name = name;
    all.trend_start = trend_start;
    all.perf = perf;

    // let chartCache = connectChartRedis();
    let priceCache = connectPriceCache();
    //let atsCache = connectATSCache();

    let bounds = await priceCache.get(`BOUNDS:${ticker}`);
    let day = await priceCache.get(`DAY:${ticker}`);

    let chartResult, gType;
    let chart = {};
    let chartData = [];

    let chartQuery = `
        SELECT count(*) FROM candles WHERE date = (SELECT MAX(date_time)::date FROM candles WHERE ticker = '${ticker}') AND ticker = '${ticker}' AND group_type = 15
    `;
    chartResult = await eqDb(chartQuery);


    if (chartResult && chartResult.length > 0 && chartResult[0]?.count >= 10) {
        gType = 15;
    } else {
        gType = 1;
    }

    chartQuery = `
        SELECT * 
        FROM candles
        WHERE ticker = '${ticker}'
        AND date = (SELECT MAX(date_time)::date FROM candles WHERE ticker = '${ticker}')
        AND group_type = ${gType}
        ORDER BY "openTime" ASC
    `;

    chartResult = await eqDb(chartQuery);

    if (chartResult?.length > 0) {

        for (let minute of chartResult) {
            let price = parseFloat(minute.last);
            chartData.push(price);
        }

        chart.date = chartResult[0].date;
        chart.ticker = ticker;
        chart.data = chartData;
    }

    ats = await getATSFrequencySnapshots(ticker);

    let t = ticker.toLowerCase();
    let options = await darkpool.helperGetSnapshot(t);

    all.bounds = await JSON.parse(bounds);
    all.day = await JSON.parse(day);
    all.chart = chart;
    all.options = options;
    all.ats = ats;


    console.log(`${ticker} DARKFLOW COMPLETE`);

    return all;
};


async function fetchATSDates(ticker) {
    let atsCache = connectATSCache();

    // fetch the latest date of the data
    let cachedDate = await atsCache.get(`DATE:${ticker}`);

    // find the last 7 dates
    let date = moment(cachedDate);
    let pastDates = [];
    for (let i = 1; i <= 7; i++) {
        let iDate = date.subtract(1, "days");
        pastDates.push(iDate.format("YYYY-MM-DD"));
    }

    return { date: cachedDate, pastDates: pastDates };
}


export const getATSFrequencySnapshots = async (ticker) => {
    let jMin = 2000;

    let current = {};
    let previous = {};
    let compared = {};

    let { date, pastDates } = await fetchATSDates(ticker);
    if (!date) {
        return {};
    }

    // get the latest date's data
    let currentMinData = await fetchATSData([date], ticker, jMin);
    if (currentMinData) {
        current.day_trades = currentMinData.trades;
        current.day_volume = currentMinData.volume;
        current.day_price = currentMinData.price;
        current.day_dollar_volume = current.day_trades ? (current.day_price / current.day_trades) * current.day_volume : 0;
        jMin = currentMinData.score; // julian minute of latest date
    }

    // get the previous date's data
    let previousMinData = await fetchATSData(pastDates, ticker, jMin);
    if (previousMinData) {
        previous.day_trades = previousMinData.trades;
        previous.day_volume = previousMinData.volume;
        previous.day_price = previousMinData.price;
        previous.day_dollar_volume = previous.day_trades ? (previous.day_price / previous.day_trades) * previous.day_volume : 0;
    }

    compared.day_trades = calculateNomralizedChange(current.day_trades, previous.day_trades);
    compared.day_volume = calculateNomralizedChange(current.day_volume, previous.day_volume);
    compared.day_price = calculateNomralizedChange(current.day_price, previous.day_price);
    compared.day_dollar_volume = calculateNomralizedChange(current.day_dollar_volume, previous.day_dollar_volume);

    jMin -= 60; // hour ago julian minute
    let currentHourAgoData = await fetchATSData([date], ticker, jMin, current);
    if (currentHourAgoData) {
        current.hour_trades = currentHourAgoData.trades;
        current.hour_volume = currentHourAgoData.volume;
        current.hour_price = currentHourAgoData.price;
        current.hour_dollar_volume = current.hour_trades ? (current.hour_price / current.hour_trades) * current.hour_volume : 0;
    }

    // get the previous date's hour data
    let previousHourAgoData = await fetchATSData(pastDates, ticker, jMin, previous);
    if (previousHourAgoData) {
        previous.hour_trades = previousHourAgoData.trades;
        previous.hour_volume = previousHourAgoData.volume;
        previous.hour_price = previousHourAgoData.price;
        previous.hour_dollar_volume = previous.hour_trades ? (previous.hour_price / previous.hour_trades) * previous.hour_volume : 0;
    }

    compared.hour_trades = calculateNomralizedChange(current.hour_trades, previous.hour_trades);
    compared.hour_volume = calculateNomralizedChange(current.hour_volume, previous.hour_volume);
    compared.hour_price = calculateNomralizedChange(current.hour_price, previous.hour_price);
    compared.hour_dollar_volume = calculateNomralizedChange(current.hour_dollar_volume, previous.hour_dollar_volume);

    return {
        date,
        ticker,
        current,
        previous,
        compared,
    };
};



function calculateNomralizedChange(current, previous) {
    let result;
    if (current && previous) {
        let calc = ((((current - previous) / (previous * 2))) + 0.5) * 100;
        result = Number(calc.toFixed(2));
    }
    return result;
}

// daily trending darkflow
// export const processTrendingHighDarkflow = async (req) => {
//     let darkFlow = await getHighDarkFlow();
//
//     if (darkFlow) {
//         darkFlow = await filterSecurityNames(darkFlow);
//         darkFlow = await filterOptions(darkFlow);
//
//         // split darkflow between trending up and down
//         // trending up: positive performance since prev close or open; trending down: negative since prev close or open
//         let trendingUp = [];
//         let trendingDown = [];
//
//         // first split darkflow into trending up and trending down
//         // then split trending up into trending down if needed
//         for (let df of darkFlow) {
//             // fetch performance
//             let currentPrice = await getLastPrice(df.ticker);
//             currentPrice = currentPrice.last_price;
//             let openPrice = await getTrendOpenPrice(df.ticker);
//             let perf = (currentPrice / openPrice - 1) * 100
//             df.trend_start = openPrice;
//             df.perf = perf;
//
//             if (perf > 0) {
//                 trendingUp.push(df);
//             } else {
//                 trendingDown.push(df);
//             }
//         }
//
//         trendingUp.sort(function (a, b) {
//             return b.perf - a.perf;
//         });
//
//         trendingDown.sort(function (a, b) {
//             return a.perf - b.perf;
//         });
//
//         if (trendingUp.length > 25) {
//             trendingUp.length = 25
//         }
//         if (trendingDown.length > 25) {
//             trendingDown.length = 25
//         }
//
//         darkFlow = { trending_up: trendingUp, trending_down: trendingDown };
//
//         let jsonDarkflow = JSON.stringify(darkFlow);
//
//         let topTickers = [];
//         for (let i = 0; i < 4; i++) {
//             let tick = trendingUp[i].ticker;
//             topTickers.push(tick);
//         }
//         let jsonTopTickers = JSON.stringify(topTickers);
//
//         let atsCache = connectATSCache();
//         atsCache.set(`${ATS_TRENDING_HIGH_DARK_FLOW}`, jsonDarkflow);
//         atsCache.set(`${ATS_TOP_TICKERS}`, jsonTopTickers);
//
//         if (getEnv("RELEASE_STAGE") == "production") {
//             let atsFallCache = connectATSFallbackCache();
//             atsFallCache.set(`${ATS_TRENDING_HIGH_DARK_FLOW}`, jsonDarkflow);
//         }
//
//         return darkFlow
//     }
//
//     return darkFlow
// };