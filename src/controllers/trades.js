import db from "../db";

import axios from "axios";
import * as quodd from "./quodd"

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
		totalGain = 0;
	
	checkDateResult = await db(`SELECT to_char("created_at", 'YYYY-MM-DD') as latest_date FROM daily_trades ORDER by created_at DESC limit 1`);
	for(let i = 0; i < symbol.length; i++){
		var response = await axios.get(`${process.env.ARK_API_URL}/api/v1/etf/trades?symbol=${symbol[i]}`);
		
		if(response.status === 200 && response.data.trades.length > 0) {
		let trades = response.data.trades;
		
		if(trades.length > 0 && checkDateResult.length > 0){
			if (trades[0].date === checkDateResult[0].latest_date){
				continue;
			}
		}
			for(let x = 0; x < trades.length; x++) {
				prices = await quodd.getLastPriceChange(trades[x].ticker);

				if(prices.last_price > 0 && prices.open_price > 0) {
					let query = {
						text:
							"INSERT INTO daily_trades(fund, created_at, direction, ticker, cusip, company, shares, etf_percent, open_price, market_value) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
						values: [response.data.symbol, trades[x].date, trades[x].direction, trades[x].ticker, trades[x].cusip, trades[x].company, trades[x].shares, trades[x].etf_percent, prices.open_price, trades[x].shares * prices.open_price],
					};
					
					await db(query);

				}
			}
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
					"INSERT INTO ark_portfolio(ticker, cusip, company, shares, etf_percent, open_market_value, created_at, status) VALUES ($1, $2, $3, $4, $5, $6, now(), 'open')",
				values: [tickers30Days[z].ticker, tickers30Days[z].cusip, tickers30Days[z].company, totalShares, totalETFPercent, totalOpenMarketValue],
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
						"INSERT INTO ark_portfolio(ticker, cusip, company, shares, etf_percent, open_market_value, close_market_value, total_gain, created_at, status) VALUES ($1, $2, $3, 0, 0, $4, $5, $6, now(), 'closed')",
					values: [tickers30Days[z].ticker, tickers30Days[z].cusip, tickers30Days[z].company, openMarketValueResult[0].open_price * openMarketValueResult[0].shares, closedMarketValueResult[0].open_price * closedMarketValueResult[0].shares, totalGain],
				};

				await db(afterAnalysis);
			}
		}

	}	
}

export async function getTop3Buy() {
  	const result = await db(`
        SELECT * FROM daily_trades WHERE direction = 'Buy' AND created_at = (SELECT created_at FROM public.daily_trades ORDER by created_at DESC limit 1) ORDER BY SHARES DESC Limit 3
        `);
  	return result;
}

export async function getTop3Sell() {
  	const result = await db(`
        SELECT * FROM daily_trades WHERE direction = 'Sell' AND created_at = (SELECT created_at FROM public.daily_trades ORDER by created_at DESC limit 1) ORDER BY SHARES DESC Limit 3
        `);
  	return result;
}
