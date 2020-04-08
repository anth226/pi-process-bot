export function lookupCompany(intrinioApi, ticker) {
  let res = intrinioApi
    .getCompany(ticker)
    .then(function(data) {
      return data;
    })
    .catch(function(error) {
      return error;
    });

  return res;
}

export function companyFundamentals(intrinioApi, ticker) {
  const opts = {
    filedAfter: null, // Date | Filed on or after this date
    filedBefore: null, // Date | Filed on or before this date
    reportedOnly: false, // Boolean | Only as-reported fundamentals
    fiscalYear: null, // Number | Only for the given fiscal year
    statementCode: null, // String | Only of the given statement code
    type: null, // String | Only of the given type
    startDate: null, // Date | Only on or after the given date
    endDate: null, // Date | Only on or before the given date
    pageSize: 100, // Number | The number of results to return
    nextPage: null // String | Gets the next page of data from a previous API call
  };

  let res = intrinioApi
    .getCompanyFundamentals(ticker, opts)
    .then(function(data) {
      return data;
    })
    .catch(function(error) {
      return error;
    });

  return res;
}

export function companyNews(intrinioApi, ticker) {
  const opts = {
    pageSize: 20, // Number | The number of results to return
    nextPage: null // String | Gets the next page of data from a previous API call
  };

  let res = intrinioApi
    .getCompanyNews(ticker, opts)
    .then(function(data) {
      return data;
    })
    .catch(function(error) {
      return error;
    });

  return res;
}

export async function searchCompanies(intrinioApi, query, secApi) {
  const opts = {
    pageSize: 20 // Number | The number of results to return
  };

  let res = intrinioApi
    .searchCompanies(query, opts)
    .then(async function(data) {
      const etfs = await searchETF(secApi, query);
      etfs.forEach(function(etf, idx) {
        let etfTicker = etf.ticker;
        data.companies.forEach(function(com, i) {
          if (etfTicker == com.ticker) {
            data.companies.splice(i, 1);
          }
        });
      });
      return interleaveArray(data.companies, etfs);
    })
    .catch(function(error) {
      return error;
    });

  return res;
}

export function searchSec(intrinioApi, query) {
  const opts = {
    pageSize: 20 // Number | The number of results to return
  };

  let res = intrinioApi
    .searchSecurities(query, opts)
    .then(function(data) {
      return data;
    })
    .catch(function(error) {
      return error;
    });

  return res;
}

async function searchETF(intrinioApi, query) {
  const opts = {
    pageSize: 30 // Number | The number of results to return
  };

  let res = intrinioApi
    .searchSecurities(query, opts)
    .then(function(data) {
      return data.securities.filter(
        security =>
          security.currency == "USD" &&
          security.code == "ETF" &&
          security.composite_ticker.endsWith(":US")
      );
    })
    .catch(function(error) {
      return error;
    });

  return res;
}

function interleaveArray(array1, array2) {
  let result = [];
  let i,
    l = Math.min(array1.length, array2.length);

  for (i = 0; i < l; i++) {
    result.push(array1[i], array2[i]);
  }
  result.push(...array1.slice(l), ...array2.slice(l));
  return result;
}

export async function getDataPoint(intrinioApi, reqData) {
  const symbols = Object.keys(reqData);
  let reqs = [];

  symbols.forEach(symbol => {
    reqData[symbol].forEach(tag => {
      reqs.push({
        symbol: symbol.toUpperCase(),
        tag: tag
      });
    });
  });

  const res = await Promise.all(
    reqs
      .map(req => intrinioApi.getSecurityDataPointText(req.symbol, req.tag))
      .map(p => p.catch(e => e))
  );

  let currentSymbol = 0;
  let lastTag = reqData[symbols[0]].length - 1;
  let counter = 0;

  let result = {};
  res.forEach((data, i) => {
    if (!result[symbols[currentSymbol]]) {
      result[symbols[currentSymbol]] = {};
    }

    // console.log(reqData[symbols[currentSymbol]]);

    if (data instanceof Error) {
      result[symbols[currentSymbol]][reqData[symbols[currentSymbol]][counter]] =
        "Unavailable";
    } else {
      result[symbols[currentSymbol]][
        reqData[symbols[currentSymbol]][counter]
      ] = data;
    }

    counter++;
    if (counter > lastTag && currentSymbol < symbols.length - 1) {
      currentSymbol++;
      counter = 0;
      lastTag = reqData[symbols[currentSymbol]].length - 1;
    }
  });
  return result;
}

export async function getNumberDataPoint(intrinioApi, symbols, tags) {
  let reqs = [];

  symbols.forEach(symbol => {
    tags.forEach(tag => {
      reqs.push({
        symbol: symbol.toUpperCase(),
        tag: tag
      });
    });
  });

  const res = await Promise.all(
    reqs
      .map(req => intrinioApi.getCompanyDataPointNumber(req.symbol, req.tag))
      .map(p => p.catch(e => e))
  );
  const div = res.length / symbols.length;
  // console.log(res.length);
  let sym = 0;
  let counter = 0;

  let result = {};

  res.forEach((data, i) => {
    counter++;
    if (counter > div) {
      sym++;
      counter = 1;
    }
    if (!result[symbols[sym]]) {
      result[symbols[sym]] = {};
    }
    if (data instanceof Error) {
      result[symbols[sym]][tags[i % tags.length]] = "Unavailable";
    } else {
      result[symbols[sym]][tags[i % tags.length]] = data;
    }
  });

  return result;
}

export async function getTextDataPoint(intrinioApi, symbols, tags) {
  const res = await Promise.all(
    symbols.map(symbol =>
      tags
        .map(tag => intrinioApi.getCompanyDataPointText(symbol, tag))
        .map(p => p.catch(e => e))
    )
  );

  return res.map((data, i) => {
    if (data instanceof Error) {
      return [tags[i], "Unavailable"];
    } else {
      return [tags[i], data];
    }
  });
}
