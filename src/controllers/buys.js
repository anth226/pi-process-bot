import db from "../db";
import atsDb from "../atsDB";
import chartHistDb from "../chartHistoryDb";
import * as darkpool from "./darkpool"
import * as ats from "./ats"
import optionsDb from "../optionsProdDb"
import { connectATSCache, connectATSFallbackCache, ATS_HIGH_DARK_FLOW, ATS_TRENDING_HIGH_DARK_FLOW, ATS_TOP_TICKERS, ATS_HISTORICAL_TRENDING_HIGH_DARK_FLOW, connectChartRedis, connectPriceCache } from '../redis';
import { getEnv } from "../env";
import { filterSecurityNames } from "../controllers/securities"
import { filterOptions } from "../controllers/ats"
import * as quodd from "./quodd"
import eqDb from "../equitiesDb"
import * as securities from "./securities";
import moment from "moment";

export const getTopZacks = async () => {
    let result = await db(`
        SELECT *
        FROM zacks_rank
        WHERE rank = 1 OR rank = 2
        AND NOT sector LIKE '%Defense%'
        AND NOT sector LIKE '%Oil and Gas%'
        AND NOT sector LIKE '%Coal%'
        ORDER BY rank ASC
    `);

    if (result.length > 0) {
        return result;
    }
    return null;
};

export const getHardStrongBuys = async () => {
    let result = await db(`
        SELECT *
        FROM strong_buys
    `);

    if (result.length > 0) {
        return result;
    }
    return null;
};


export const getChartHist = async (ticker) => {
    let result = await chartHistDb(`
        SELECT MAX(high)
        FROM charts_history
        WHERE ticker = '${ticker}'
        AND time >= (SELECT MAX(time) FROM charts_history WHERE ticker = '${ticker}') - INTERVAL '30 DAY'
    `);

    if (result.length > 0) {
        return result[0];
    }
    return null;
};



export const getBuys = async () => {
    let buys = [];
    let result = await getTopZacks();

    let chartCache = connectChartRedis();
    let priceCache = connectPriceCache();

    if (result) {
        result = await filterSecurityNames(result);
        result = await filterOptions(result);

        for (let sec of result) {
            let ticker = sec.ticker;

            // //get historical darkflow avg
            // let darkflow = await ats.getATSFrequencySnapshots(ticker);
            // let dfCompared = darkflow?.compared?.day_dollar_volume;

            // console.log("dfCompared", dfCompared);
            // if (!dfCompared > 1.5) {
            // console.log(`DISCLUDED ${ticker} DUE TO LOW DARKFLOW`);
            //     return;
            // }



            //get 30 day options flow + 80%
            let today = moment().format("YYYY-MM-DD");
            let aMonthAgo = moment().subtract(1, "months").format("YYYY-MM-DD");
            let dates = `${today},${aMonthAgo}`;
            let t = ticker.toLowerCase();
            let options = await darkpool.getFlowSentiment(t, dates);
            let optSent = options?.flow_sentiment;

            if (!optSent > .80) {
                console.log(`DISCLUDED ${ticker} DUE TO LOW FLOW: ${optSent}`);
                continue;
            }

            //get previous highs for 30 days, compare to last_price
            let prices = await getPrices(ticker);
            let price = parseFloat(prices?.price);
            let max = await getChartHist(ticker);
            max = parseFloat(max?.max);

            if (price > max) {
                buys.push({
                    ticker: ticker,
                    price: price,
                    max: max
                })
            }
        }
    }
    console.log("buys", buys);
    return buys;
};


export const getHardcodedBuys = async () => {
    let buys = [];
    let result = await getHardStrongBuys();

    let priceCache = connectPriceCache();

    if (result) {
        for (let sec of result) {
            let ticker = sec.ticker;
            let added_date = sec.added_date;
            let chart = await getSparklines(ticker);
            let prices = await getPrices(ticker);
            let security = await securities.getSecurityByTicker(ticker);

            let perf = security?.perf_values;
            let threeMonth = perf?.threemonth?.value;

            buys.push({
                ticker: ticker,
                name: security?.name,
                three_month_price: threeMonth,
                added_date: added_date,
                chart: chart,
                prices: prices
            });
        }
    }

    buys = JSON.stringify(buys);

    priceCache.set(`STRONGBUYS`, buys);

    console.log(`STRONG BUYS SET`);
    return;
};


export const getPrices = async (ticker) => {
    let priceCache = connectPriceCache();

    let result = await priceCache.get(`NOW:${ticker}`);
    if (result) {
        let data = JSON.parse(result);
        result = await priceCache.get(`BOUNDS:${ticker}`);
        result = JSON.parse(result);
        if (result?.high) {
            data.bounds = result;
        }
        result = await priceCache.get(`DAY:${ticker}`);
        result = JSON.parse(result);
        if (result?.open) {
            data.day = result;
        }
        return data;
    }
};


export const getSparklines = async (ticker) => {
    let chartResult;
    let chart = {};
    let chartData = [];

    let chartQuery = `
        SELECT * 
        FROM charts_history
        WHERE ticker = '${ticker}'
        AND time >= (SELECT MAX(time) FROM charts_history WHERE ticker = '${ticker}') - INTERVAL '90 DAY'
        ORDER BY time ASC
    `;

    chartResult = await chartHistDb(chartQuery);

    if (chartResult?.length > 0) {

        for (let day of chartResult) {
            let price = parseFloat(day.close);
            chartData.push(price);
        }

        chart.ticker = ticker;
        chart.data = chartData;
    }
    return chart;
};