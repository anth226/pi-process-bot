//import axios from "axios";
import "dotenv/config";
const { Client } = require("pg");

const MTZ = require("moment-timezone");

import * as widgets from "../controllers/widgets";
import * as getSecurityData from "./intrinio/get_security_data";
import moment from "moment";

import {
  CACHED_DAY,
  CACHED_NOW,
  CACHED_PERF,
  connectPriceCache
} from "../redis";
import {getEnv} from "../env";
import {connect} from "mqtt";

let dbs = {};
const isDev = getEnv("IS_DEV");

// const connectDatabase = (credentials) => {
//   if (!dbs[credentials.host]) {
//     const client = new Client(credentials);

//     client.connect();

//     dbs[credentials.host] = async (sql, cb) =>
//       (await client.query(sql, cb)).rows;
//   }
//   return dbs[credentials.host];
// };

// export const setup = async () => {
//   let host = await sharedCache.get(AWS_POSTGRES_DB_HOST);

//   if (!isDev == "true" || host) {
//     return;
//   }

//   try {
//     await sharedCache.set(
//       AWS_POSTGRES_DB_DATABASE,
//       process.env.AWS_POSTGRES_DB_1_NAME
//     );
//     await sharedCache.set(
//       AWS_POSTGRES_DB_HOST,x
//       process.env.AWS_POSTGRES_DB_1_HOST
//     );
//     await sharedCache.set(
//       AWS_POSTGRES_DB_PORT,
//       process.env.AWS_POSTGRES_DB_1_PORT
//     );
//     await sharedCache.set(
//       AWS_POSTGRES_DB_USER,
//       process.env.AWS_POSTGRES_DB_1_USER
//     );
//     await sharedCache.set(
//       AWS_POSTGRES_DB_PASSWORD,
//       process.env.AWS_POSTGRES_DB_1_PASSWORD
//     );
//   } catch (error) {
//     console.log(error, "---error---");
//   }
// };

// export const getCredentials = async () => {
//   connectSharedCache();

//   await setup();

//   let host = await sharedCache.get(AWS_POSTGRES_DB_HOST);
//   let port = await sharedCache.get(AWS_POSTGRES_DB_PORT);
//   let database = await sharedCache.get(AWS_POSTGRES_DB_DATABASE);
//   let user = await sharedCache.get(AWS_POSTGRES_DB_USER);
//   let password = await sharedCache.get(AWS_POSTGRES_DB_PASSWORD);

//   return {
//     host,
//     port,
//     database,
//     user,
//     password,
//   };
// };

// // export async function test() {
// //   let dateString;
// //   let today = MTZ().tz("America/New_York");

// //   console.log("today", today.day());

// //   // let newYork = today;

// //   // let year = newYork.format("YYYY");
// //   // let month = newYork.format("M");
// //   // let day = newYork.format("D");

// //   // dateString = `${year}-${month}-${day}`;

// //   if (today.day() == 6) {
// //     // dateString = `${new Date().getUTCFullYear()}-${
// //     //   new Date().getUTCMonth() + 1
// //     // }-${new Date().getUTCDate() - 1}`;

// //     let newYork = today.subtract(1, "days");

// //     let year = newYork.format("YYYY");
// //     let month = newYork.format("M");
// //     let day = newYork.format("D");

// //     dateString = `${year}-${month}-${day}`;
// //   } else if (today.day() == 0) {
// //     let newYork = today.subtract(2, "days");

// //     let year = newYork.format("YYYY");
// //     let month = newYork.format("M");
// //     let day = newYork.format("D");

// //     dateString = `${year}-${month}-${day}`;
// //   }

// //   let newYork = today.subtract(2, "days");

// //   let year = newYork.format("YYYY");
// //   let month = newYork.format("M");
// //   let day = newYork.format("D");

// //   dateString = `${year}-${month}-${day}`;

// //   console.log(dateString);
// // }

// export async function getAllForTicker(ticker) {
//   let credentials = await getCredentials();

//   let db = connectDatabase(credentials);

//   console.time("getAllForTicker");

//   console.timeLog("getAllForTicker");

//   // let result = await db(`
//   //   SELECT
//   //   date_trunc('minute', timestamp) as timestamp,
//   //   MAX(price) / 100 as price,
//   //   count(1)
//   //   from equities_current
//   //   WHERE symbol='e${ticker}' AND timestamp > (now() - interval '5h')::date
//   //   group by 1
//   //   ORDER by 1 DESC
//   // `);

//   let result = await db(`
//     SELECT timestamp,
//     price::decimal / 100 as price
//     FROM equities_current
//     WHERE symbol='e${ticker}' AND timestamp > (now() - interval '5h')::date
//     ORDER BY timestamp ASC
//   `);

//   let series = [];
//   let url;

//   console.timeLog("getAllForTicker");
//   console.log("getAllForTicker", result.length);

//   if (result) {
//     if (result.length > 0) {
//       series = result.map((item) => [item.timestamp, parseFloat(item.price)]);
//     } else {
//       // evaluate date string for weekends
//       let dateString;

//       let today = MTZ().tz("America/New_York");

//       if (today.day() == 6) {
//         let newYork = today.subtract(1, "days");

//         let year = newYork.format("YYYY");
//         let month = newYork.format("M");
//         let day = newYork.format("D");

//         dateString = `${year}-${month}-${day}`;
//       } else if (today.day() == 0) {
//         let newYork = today.subtract(2, "days");

//         let year = newYork.format("YYYY");
//         let month = newYork.format("M");
//         let day = newYork.format("D");

//         dateString = `${year}-${month}-${day}`;
//       } else {
//         const range = ["05:00", "14:30"];

//         const t1 = moment.utc(range[0], "HH:mm");
//         const t2 = moment.utc(range[1], "HH:mm");

//         const now = moment.utc();

//         if (now.isAfter(t1) && now.isBefore(t2)) {
//           console.log("in range");
//           let newYork = today.subtract(1, "days");

//           let year = newYork.format("YYYY");
//           let month = newYork.format("M");
//           let day = newYork.format("D");

//           dateString = `${year}-${month}-${day}`;
//         }
//       }

//       if (dateString) {
//         let key = `e${ticker}/${dateString}.json`;

//         url = `https://${process.env.AWS_BUCKET_PRICE_ACTION}.s3.amazonaws.com/${key}`;
//       }
//     }
//   }

//   let response = {
//     series,
//     url,
//   };

//   console.timeLog("getAllForTicker");

//   console.timeEnd("getAllForTicker");

//   return response;
// }

export async function getLastPrice(ticker) {
  let prices;
  let realtime;

  let sharedCache = connectPriceCache();

  // let cachedPrice_R = await sharedCache.get(
  //   `${CACHED_PRICE_REALTIME}${qTicker}`
  // );

  // if (cachedPrice_R) {
  //   realtime = cachedPrice_R / 100;
  // }

  let now = await sharedCache.get(`${CACHED_NOW}${ticker}`);

  if (now) {
    let parsedNow = JSON.parse(now);
    let price = Number(parsedNow.price);

    if (price) {
      realtime = price;
      prices = {
        //last_price_realtime: realtime,
        last_price: realtime,
      };
    }
  } else {
    let intrinioResponse = await getSecurityData.getSecurityLastPrice(ticker);
    if (intrinioResponse && intrinioResponse.last_price) {
      prices = {
        //last_price_realtime: intrinioPrice.last_price,
        last_price: intrinioResponse.last_price,
      };
    }
  }
  return prices;
}

export async function getLastPriceChange(ticker) {
  console.log("\n\nLAST PRICE CHANGE");
  let response;
  let realtime, open, close, prev_close, date;

  console.log("ticker", ticker);

  let sharedCache = connectPriceCache();

  // let cachedPrice_R = await sharedCache.get(
  //   `${CACHED_PRICE_REALTIME}${qTicker}`
  // );

  // if (cachedPrice_R) {
  //   realtime = cachedPrice_R / 100;
  // }

  let now = await sharedCache.get(`${CACHED_NOW}${ticker}`);
  let day = await sharedCache.get(`${CACHED_DAY}${ticker}`);
  let perf = await sharedCache.get(`${CACHED_PERF}${ticker}`);

  if (now) {
    let parsedNow = JSON.parse(now);
    realtime = Number(parsedNow.price);
  }
  if (day) {
    let parsedDay = JSON.parse(day);
    close = parsedDay?.close;
    if (close) {
      close = Number(close);
    }
    prev_close = parsedDay?.prev_close;
    if (prev_close) {
      prev_close = Number(prev_close)
    }
    open = parsedDay?.open;
    if (open) {
      open = Number(open);
    }
    date = parsedDay?.date;
  }

  if (perf) {
    let jsonPerf = JSON.parse(perf);
    let vals = jsonPerf.values;
    let openVal = open || vals.today.value;
    let openDate = date || vals.today.date;

    delete vals["today"];
    vals["open"] = {
      date: openDate,
      value: openVal,
    };

    if (realtime) {
      let percentChange = (realtime / openVal - 1) * 100;

      response = {
      //last_price_realtime: realtime,
      close_price: close,
      prev_close_price: prev_close,
      last_price: realtime,
      open_price: openVal,
      performance: percentChange,
      values: vals,
      };
    } else {
      let intrinioResponse = await getSecurityData.getSecurityLastPrice(ticker);
      console.log("intrinioResponse", intrinioResponse);

      if (intrinioResponse && intrinioResponse.last_price) {
      let lastPrice = intrinioResponse.last_price;
      let percentChange = (lastPrice / openVal - 1) * 100;

      response = {
        //last_price_realtime: intrinioPrice.last_price,
        close_price: close,
        prev_close_price: prev_close,
        last_price: lastPrice,
        open_price: openVal,
        performance: percentChange,
        values: vals,
      };
      console.log("response 1", response);
      } else {
        response = {
          //last_price_realtime: intrinioPrice.last_price,
          close_price: close,
          prev_close_price: prev_close,
          last_price: 0,
          open_price: openVal,
          performance: 0,
          values: vals,
        };
      }
    }

    return response;
  } else {
      let intrinioResponse = await getSecurityData.getSecurityLastPrice(ticker);
      console.log("intrinioResponse", intrinioResponse);

      const last_price =
      realtime ||
      (intrinioResponse && intrinioResponse.last_price) ||
      0;

      const open_price =
        open ||
        (intrinioResponse && intrinioResponse.open_price) || 0;

      const open_date =
        date ||
        (intrinioResponse && intrinioResponse.last_time) || "";

      console.log("last_price", last_price);
      console.log("open_price", open_price);

      return {
        close_price: close,
        prev_close_price: prev_close,
        last_price,
        open_price,
        performance: (last_price / open_price - 1) * 100,
        values: {
          open: {
            date: open_date,
            value: open_price,
          },
       },
     };
  }
}


export async function getOpenPrice(ticker) {
  if (!ticker) {
    return;
  }
  let open;

  let sharedCache = connectPriceCache();

  let day = await sharedCache.get(`${CACHED_DAY}${ticker}`);
  if (day) {
    let parsed = JSON.parse(day);
    open = parsed?.open;
    if (open) {
      open = Number(open)
    }
  }

  return open;
}

export async function setPerfCache(ticker, perf) {
  if (!ticker || !perf) {
    return;
  }

  const priceCache = connectPriceCache();

  let json = JSON.stringify(perf);

  await priceCache.set(`${CACHED_PERF}${ticker}`, json);
}

// export async function testCache() {
//   connectSharedCache();

//   let data = await sharedCache.get(`${KEY_SECURITY_PERFORMANCE}-TSLA`);
//   return data;
// }
