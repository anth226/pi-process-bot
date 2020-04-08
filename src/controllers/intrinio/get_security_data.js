import axios from "axios";

export function getIntradayPrices(intrinioApi, identifier) {
  const opts = {
    source: null, // String | Return intraday prices from the specified data source
    startDate: new Date("2019-12-25"), // Date | Return intraday prices starting at the specified date
    startTime: null, // String | Return intraday prices starting at the specified time on the `start_date` (timezone is UTC)
    endDate: new Date("2019-12-28"), // Date | Return intraday prices stopping at the specified date
    endTime: Date.now(), // String | Return intraday prices stopping at the specified time on the `end_date` (timezone is UTC)
    pageSize: 100, // Number | The number of results to return
    nextPage: null // String | Gets the next page of data from a previous API call
  };

  let res = intrinioApi
    .getSecurityIntradayPrices(identifier, opts)
    .then(function(data) {
      return data;
    })
    .catch(function(error) {
      return error;
    });

  return res;
}

function historicalPages(intrinioApi, identifier, opts, hist) {
  return intrinioApi
    .getSecurityHistoricalData(identifier, "adj_close_price", opts)
    .then(
      function(data) {
        data.historical_data.forEach((item, i) => {
          hist.push(item);
        });
        if (data.next_page == null) {
          return hist;
        } else {
          let newOpts = opts;
          newOpts.nextPage = data.next_page;
          return historicalPages(intrinioApi, identifier, newOpts, hist);
        }
      },
      function(error) {
        return error;
      }
    );
}

export function getHistoricalData(intrinioApi, identifier, days) {
  let startDate = new Date(Date.now());
  startDate.setDate(startDate.getDate() - days);
  var opts = {
    frequency: "daily", // String | Return historical data in the given frequency
    type: null, // String | Filter by type, when applicable
    startDate: startDate, // Date | Get historical data on or after this date
    endDate: null, // Date | Get historical date on or before this date
    sortOrder: "desc", // String | Sort by date `asc` or `desc`
    pageSize: 100, // Number | The number of results to return
    nextPage: null // String | Gets the next page of data from a previous API call
  };

  return historicalPages(intrinioApi, identifier, opts, []);
}

export function getSecurityLastPrice(symbol) {
  let lastPrice = axios
    .get(
      `https://api-v2.intrinio.com/securities/${symbol}/prices/realtime?source=iex&api_key=${process.env.INTRINIO_API_KEY}`
    )
    .then(function(res) {
      return res;
    })
    .catch(function(err) {
      return err;
    });
  //
  return lastPrice.then(data => data.data);
}

export function lookupSecurity(intrinioApi, ticker) {
  let res = intrinioApi
    .getSecurityById(ticker)
    .then(function(data) {
      return data;
    })
    .catch(function(error) {
      return error;
    });

  return res;
}
// export function getRealtimePrice(intrinioApi, identifier) {

//     var opts = {
//         'source': 'iex'// String | Return the realtime price from the specified data source. If no source is specified, the best source available is used.
//     };

//     let res =
//         intrinioApi.getSecurityRealtimePrice(identifier, opts)
//     .then(function(data) {
//         return data;
//     })
//     .catch(function(error) {
//         return error;
//     })

//     return res;
// }
