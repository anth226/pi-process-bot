import atsDb from "../atsDB";
import db from "../db";
import optionsDb from "../optionsProdDb"
import {connectATSCache, connectATSFallbackCache, ATS_HIGH_DARK_FLOW} from '../redis';
import {getEnv} from "../env";

export const getHighDarkFlow = async (req) => {
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

        if (results) {
            let tickers = results.map(s => s.ticker).join(",");

            const securities = await db(`
                SELECT ticker, name FROM securities WHERE ticker = ANY('{${tickers}}')
                `);

            const securityMap = securities.reduce((s,security) => ({...s, [security.ticker]: security.name}), {});

            let darkFlow = results.map(s => {
                s.name = securityMap[s.ticker];
                return s;
            }).filter(s => s.name);

            tickers = darkFlow.map(s => s.ticker).join(",");

            let optionsQuery = `
                SELECT DISTINCT(ticker)
                FROM options
                WHERE to_timestamp(time)::date = (SELECT to_timestamp(time)::date FROM options ORDER BY time DESC LIMIT 1)
                AND ticker = ANY('{${tickers}}')
                `;

            const options = await optionsDb(optionsQuery);

            if (options && options.length > 0) {
                const optionsMap = options.reduce((o,option) => ({...o, [option.ticker]: true}), {});

                darkFlow = darkFlow.map(s => {
                    s.has_options = optionsMap[s.ticker];
                    return s;
                }).filter(s => s.has_options);
            }

            if (darkFlow.length > 25) {
                darkFlow.length = 25
            }

            let jsonDarkflow = JSON.stringify(darkFlow);

            let atsCache = connectATSCache();
            atsCache.set(`${ATS_HIGH_DARK_FLOW}`, jsonDarkflow);

            if (getEnv("RELEASE_STAGE") == "production") {
                let atsFallCache = connectATSFallbackCache();
                atsFallCache.set(`${ATS_HIGH_DARK_FLOW}`, jsonDarkflow);
            }

            return darkFlow
        }


        return results
};