export function indexHistorical(intrinioApi, identifier) {
  let tag = "level";

  var opts = {
    type: null, // String | Filter by type, when applicable
    startDate: new Date("2018-01-01"), // Date | Get historical data on or after this date
    endDate: null, // Date | Get historical data on or before this date
    sortOrder: "desc", // String | Sort by date `asc` or `desc`
    pageSize: 100, // Number | The number of results to return
    nextPage: null // String | Gets the next page of data from a previous API call
  };

  let res = intrinioApi
    .getStockMarketIndexHistoricalData(identifier, tag, opts)
    .then(function(data) {
      return data;
    })
    .catch(function(error) {
      return error;
    });

  return res;
}

export function getIndexPrice(intrinioApi, identifier) {
  let tag = "level";

  let res = intrinioApi
    .getStockMarketIndexDataPointNumber(identifier, tag)
    .then(
      function(data) {
        return data;
      },
      function(error) {
        return error;
      }
    );

  return res;
}
