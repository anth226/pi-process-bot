import db from "../db";

import axios from "axios";
import * as quodd from "./quodd"
import {getEnv} from "../env";

const symbol = ["ARKF", "ARKG", "ARKK", "ARKQ", "ARKW"];

export async function getTradesFromARK() {
	let checkDateResult,
		prices,
		tickers30Days,
		tickerResult,
		totalShares = 0,
		totalETFPercent = 0,
		totalOpenMarketValue = 0,
		openMarketValueResult,
		closedMarketValueResult,
		totalGain = 0, 
		arkTrades = [],
		tradeIndex = -1;
	
	checkDateResult = await db(`SELECT to_char("created_at", 'YYYY-MM-DD') as latest_date FROM daily_trades ORDER by created_at DESC limit 1`);
	for(let i = 0; i < symbol.length; i++){
		var response = await axios.get(`${getEnv("ARK_API_URL")}/api/v1/etf/trades?symbol=${symbol[i]}`);
		
		if(response.status === 200 && response.data.trades.length > 0) {
			let trades = response.data.trades;
		
			
			if(trades.length > 0 && checkDateResult.length > 0){
				if (trades[0].date <= checkDateResult[0].latest_date){
					continue;
				}
			}

			for(let z = 0; z < trades.length; z++) {				
				tradeIndex = arkTrades.findIndex(e => e.ticker === trades[z].ticker);
				if (tradeIndex === -1) {
					arkTrades.push(trades[z]);
				} else {
					arkTrades[tradeIndex].shares = parseFloat(arkTrades[tradeIndex].shares) + parseFloat(trades[z].shares);
					arkTrades[tradeIndex].etf_percent = parseFloat(arkTrades[tradeIndex].etf_percent) + parseFloat(trades[z].etf_percent);
				}
				
			}
		}
	}

	for(let x = 0; x < arkTrades.length; x++) {

		prices = await quodd.getLastPriceChange(arkTrades[x].ticker);

		if(prices.last_price > 0 && prices.open_price > 0) {
			let query = {
				text:
					"INSERT INTO daily_trades(created_at, direction, ticker, cusip, company, shares, etf_percent, open_price, market_value) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
				values: [arkTrades[x].date, arkTrades[x].direction, arkTrades[x].ticker, arkTrades[x].cusip, arkTrades[x].company, arkTrades[x].shares, arkTrades[x].etf_percent, prices.open_price, arkTrades[x].shares * prices.open_price],
			};
						
			await db(query);

		}
	}

	tickers30Days = await db(`
		SELECT distinct ticker, cusip, company FROM daily_trades WHERE created_at > NOW() - INTERVAL '30 days'
		ORDER BY ticker
		`);

	for(let z = 0; z < tickers30Days.length; z++){
		totalShares = 0;
		totalETFPercent = 0;
		totalGain = 0;
		totalOpenMarketValue = 0;

		tickerResult = await db(`
			SELECT * FROM daily_trades WHERE created_at > NOW() - INTERVAL '30 days' AND ticker = '${tickers30Days[z].ticker}'
			ORDER BY created_at
			`);
		
		for(let y = 0; y < tickerResult.length; y++){
			if(tickerResult[y].direction === "Buy") {
				totalShares = parseFloat(totalShares) + parseFloat(tickerResult[y].shares);
				totalETFPercent = parseFloat(totalETFPercent) + parseFloat(tickerResult[y].etf_percent);
				totalOpenMarketValue = parseFloat(totalOpenMarketValue) + parseFloat(tickerResult[y].market_value)
			} else {
				totalShares = parseFloat(totalShares) - parseFloat(tickerResult[y].shares);
				totalETFPercent = parseFloat(totalETFPercent) - parseFloat(tickerResult[y].etf_percent);
				totalOpenMarketValue = parseFloat(totalOpenMarketValue) - parseFloat(tickerResult[y].market_value)
			}

		}

		if(totalShares > 0) {
			let afterAnalysis = {
				text:
					"INSERT INTO ark_portfolio(ticker, cusip, company, shares, etf_percent, open_market_value, trade_date, created_at, status) VALUES ($1, $2, $3, $4, $5, $6, $7, now(), 'open')",
				values: [tickers30Days[z].ticker, tickers30Days[z].cusip, tickers30Days[z].company, totalShares, totalETFPercent, totalOpenMarketValue,tickerResult[tickerResult.length-1].created_at],
			};

			await db(afterAnalysis);
		} else {
			openMarketValueResult = await db(`
				SELECT * FROM daily_trades WHERE created_at > NOW() - INTERVAL '30 days' AND ticker = '${tickers30Days[z].ticker}' AND direction = 'Buy'
				ORDER BY created_at LIMIT 1
				`);

			if (openMarketValueResult.length > 0) {
				closedMarketValueResult = await db(`
					SELECT * FROM daily_trades WHERE created_at > NOW() - INTERVAL '30 days' AND ticker = '${tickers30Days[z].ticker}' AND direction = 'Sell'
					ORDER BY created_at DESC LIMIT 1
					`);

				totalGain = (((closedMarketValueResult[0].open_price * closedMarketValueResult[0].shares) / (openMarketValueResult[0].open_price * openMarketValueResult[0].shares)) - 1) * 100;

				let afterAnalysis = {
					text:
						"INSERT INTO ark_portfolio(ticker, cusip, company, shares, etf_percent, open_market_value, close_market_value, total_gain, trade_date, created_at, status) VALUES ($1, $2, $3, 0, 0, $4, $5, $6, $7, now(), 'closed')",
					values: [tickers30Days[z].ticker, tickers30Days[z].cusip, tickers30Days[z].company, openMarketValueResult[0].open_price * openMarketValueResult[0].shares, closedMarketValueResult[0].open_price * closedMarketValueResult[0].shares, totalGain, closedMarketValueResult[0].created_at],
				};

				await db(afterAnalysis);
			}
		}

	}	
}

export async function getTop3Buy() {
  	const result = await db(`
        SELECT * FROM daily_trades WHERE direction = 'Buy' AND created_at = (SELECT created_at FROM public.daily_trades ORDER by created_at DESC limit 1) ORDER BY market_value DESC Limit 3
        `);
  	return result;
}

export async function getTop3Sell() {
  	const result = await db(`
        SELECT * FROM daily_trades WHERE direction = 'Sell' AND created_at = (SELECT created_at FROM public.daily_trades ORDER by created_at DESC limit 1) ORDER BY market_value DESC Limit 3
        `);
  	return result;
}