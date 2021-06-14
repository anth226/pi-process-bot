import optionsDB from '../db2';
import { CACHED_PRICE_15MIN } from "../redis";
import redis from "redis";
import asyncRedis from "async-redis";
import moment from "moment";
const MTZ = require("moment-timezone");

export async function getSnapshot(req) {
  let ticker,
    putSum,
    putFlow,
    putPremTotal,
    callSum,
    callFlow,
    callPremTotal,
    totalSum,
    putToCall,
    flowSentiment,
    exps;

  let { query } = req;

  if (query.ticker && query.ticker.length > 0) {
    ticker = query.ticker.toLowerCase();
  }

  if (query.expiry && query.expiry.length > 0) {
    exps = query.expiry;
  }

  const snapshotQuery = getSnapshotQuery({
    ticker,
    date: query.date,
    exps
  });

  let result;
  try {
    result = await optionsDB(snapshotQuery)
  } catch { }

  if (result && result.length > 0) {
    const { callData, putData } = result.reduce((acc, cur) => {
      const key = cur.cp === 'P' ? 'putData' : 'callData';

      acc[key].flow_count += Number(cur.flow_count);
      acc[key].total_premium += Number(cur.total_premium);

      return acc;
    }, {
      callData: {
        flow_count: 0,
        total_premium: 0,
      },
      putData: {
        flow_count: 0,
        total_premium: 0,
      }
    });

    callSum = callData.flow_count;
    callPremTotal = callData.total_premium;

    putSum = putData.flow_count;
    putPremTotal = putData.total_premium;

    totalSum = Number((Number(callSum || 0) + Number(putSum || 0)).toFixed(2));

    putFlow = Number(putSum || 0).toFixed(2) / totalSum;
    callFlow = Number(callSum || 0).toFixed(2) / totalSum;
  }

  if (callSum && putSum) {
    putToCall = Number(putSum).toFixed(2) / Number(callSum).toFixed(2);
  } else if (callSum && !putSum) {
    putToCall = 0.0
  } else if (!callSum && putSum) {
    putToCall = -1.0 // infinite
  }

  // if (totalSum && putSum && putPremTotal && callPremTotal) {
  //
  //   let premTotal = (Number(putPremTotal) + Number(callPremTotal)).toFixed(2);
  //   let putPremToTotalPrem = Number(putPremTotal).toFixed(2) / premTotal;
  //
  //   let putSumToTotalSum = Number(putSum).toFixed(2) / totalSum;
  //
  //   let avg = (putSumToTotalSum > 0 && putPremToTotalPrem > 0) ? 2 : 1
  //   let avgRatios = (putSumToTotalSum + putPremToTotalPrem)/avg;
  //
  //   flowSentiment = 1 - (avgRatios || 0);
  // }

  if (totalSum) {
    let putSumToTotalSum = Number(putSum || 0).toFixed(2) / totalSum;
    flowSentiment = 1 - (putSumToTotalSum || 0);
  }


  return {
    call_count: Number(callSum || 0),
    call_flow: callFlow,
    call_total_prem: Number(callPremTotal),
    put_count: Number(putSum || 0),
    put_flow: putFlow,
    put_total_prem: Number(putPremTotal),
    put_to_call: putToCall,
    flow_sentiment: flowSentiment
  };
}

export async function helperGetSnapshot(ticker, date, exps) {
  let putSum,
    putFlow,
    putPremTotal,
    callSum,
    callFlow,
    callPremTotal,
    totalSum,
    putToCall,
    flowSentiment,
    reDate;

  const snapshotQuery = getSnapshotQuery({
    ticker,
    date: date,
    exps,
    allowHist: true
  });

  let result;
  try {
    result = await optionsDB(snapshotQuery)
  } catch (e) { console.log("err: ", e); }

  if (result && result.length > 0) {
    reDate = result[0].date || result[1].date;

    const { callData, putData } = result.reduce((acc, cur) => {
      const key = cur.cp === 'P' ? 'putData' : 'callData';

      acc[key].flow_count += Number(cur.flow_count);
      acc[key].total_premium += Number(cur.total_premium);

      return acc;
    }, {
      callData: {
        flow_count: 0,
        total_premium: 0,
      },
      putData: {
        flow_count: 0,
        total_premium: 0,
      }
    });

    callSum = callData.flow_count;
    callPremTotal = callData.total_premium;

    putSum = putData.flow_count;
    putPremTotal = putData.total_premium;

    totalSum = Number((Number(callSum || 0) + Number(putSum || 0)).toFixed(2));

    putFlow = Number(putSum || 0).toFixed(2) / totalSum;
    callFlow = Number(callSum || 0).toFixed(2) / totalSum;
  }

  if (callSum && putSum) {
    putToCall = Number(putSum).toFixed(2) / Number(callSum).toFixed(2);
  } else if (callSum && !putSum) {
    putToCall = 0.0
  } else if (!callSum && putSum) {
    putToCall = -1.0 // infinite
  }

  // if (totalSum && putSum && putPremTotal && callPremTotal) {
  //
  //   let premTotal = (Number(putPremTotal) + Number(callPremTotal)).toFixed(2);
  //   let putPremToTotalPrem = Number(putPremTotal).toFixed(2) / premTotal;
  //
  //   let putSumToTotalSum = Number(putSum).toFixed(2) / totalSum;
  //
  //   let avg = (putSumToTotalSum > 0 && putPremToTotalPrem > 0) ? 2 : 1
  //   let avgRatios = (putSumToTotalSum + putPremToTotalPrem)/avg;
  //
  //   flowSentiment = 1 - (avgRatios || 0);
  // }

  if (totalSum) {
    let putSumToTotalSum = Number(putSum || 0).toFixed(2) / totalSum;
    flowSentiment = 1 - (putSumToTotalSum || 0);
  }


  return {
    call_count: Number(callSum || 0),
    call_flow: callFlow,
    call_total_prem: Number(callPremTotal),
    put_count: Number(putSum || 0),
    put_flow: putFlow,
    put_total_prem: Number(putPremTotal),
    put_to_call: putToCall,
    flow_sentiment: flowSentiment,
    date: reDate,
  };
}

export async function getFlowSentiment(ticker, date, exps) {
  let putSum,
    putFlow,
    callSum,
    callFlow,
    totalSum,
    putToCall,
    flowSentiment,
    reDate;

  const sentimentQuery = getSentimentQuery({
    ticker,
    date: date,
    exps,
    allowHist: true
  });

  let result;
  try {
    result = await optionsDB(sentimentQuery)
  } catch (e) { console.log("err: ", e); }

  if (result && result.length > 0) {
    reDate = result[0].date || result[1].date;

    const { callData, putData } = result.reduce((acc, cur) => {
      const key = cur.cp === 'P' ? 'putData' : 'callData';

      acc[key].flow_count += Number(cur.flow_count);

      return acc;
    }, {
      callData: {
        flow_count: 0,
      },
      putData: {
        flow_count: 0,
      }
    });

    callSum = callData.flow_count;

    putSum = putData.flow_count;

    totalSum = Number((Number(callSum || 0) + Number(putSum || 0)).toFixed(2));

    putFlow = Number(putSum || 0).toFixed(2) / totalSum;
    callFlow = Number(callSum || 0).toFixed(2) / totalSum;
  }

  if (callSum && putSum) {
    putToCall = Number(putSum).toFixed(2) / Number(callSum).toFixed(2);
  } else if (callSum && !putSum) {
    putToCall = 0.0
  } else if (!callSum && putSum) {
    putToCall = -1.0 // infinite
  }

  if (totalSum) {
    let putSumToTotalSum = Number(putSum || 0).toFixed(2) / totalSum;
    flowSentiment = 1 - (putSumToTotalSum || 0);
  }

  return {
    call_count: Number(callSum || 0),
    call_flow: callFlow,
    put_count: Number(putSum || 0),
    put_flow: putFlow,
    put_to_call: putToCall,
    flow_sentiment: flowSentiment,
    date: reDate,
  };
}

export async function getSidebar() {
  let options = [];
  for (let i = 0; i < 20; i++) {
    options.push({
      time: "04:14:47 PM",
      ticker: "SPY",
      expiry: "12/31/2020",
      strike: "373",
      calls_puts: "Calls",
      spot: "369.77",
      details: "360 @ 0.75",
      type: "Block",
      prem: "$41k",
      sector: "Technology",
    });
  }
  return options;
}

export async function getOptions(req) {
  let limit,
    page = 1,
    ticker,
    last_time,
    order_direction,
    date,
    tablePostfix,
    exps,
    sort_column;

  if (req && req.query) {
    let query = req.query;

    if (query.limit) {
      limit = parseInt(query.limit);
    }

    if (query.page) {
      page = parseInt(query.page);
    }

    if (query.ticker && query.ticker.length > 0) {
      ticker = query.ticker.toLowerCase();
    }

    if (query.last_time) {
      last_time = parseInt(query.last_time);
    }

    if (query.sort_column && query.sort_column.length > 0) {
      sort_column = query.sort_column.toLowerCase();
    }

    if (sort_column) {
      order_direction = query.order_direction || 'DESC'
    }

    if (query.expiry && query.expiry.length > 0) {
      exps = query.expiry;
    }
  }

  if (page < 1) {
    page = 1;
  }

  let offset;
  if (limit) {
    offset = (page - 1) * limit;
  }

  let orderQuery = `${(sort_column && order_direction) ? `ORDER BY ${sort_column} ${order_direction}` : 'ORDER BY time DESC'}`;
  if (sort_column && sort_column != "time") {
    orderQuery = `ORDER BY ${sort_column} ${order_direction}, time DESC`
  }

  const limitQuery = `${limit ? `LIMIT ${limit} OFFSET ${offset}` : ''}`;

  const optionsQuery = getOptionsQuery({
    orderQuery,
    limitQuery,
    last_time,
    ticker,
    exps,
    date: req.query.date
  });

  let result;
  try {
    result = await optionsDB(optionsQuery);
  } catch {
    result = [];
  }
  return result;
};

function validateDate(date) {
  let formats = ['MM-DD-YYYY', 'MM-DD-YY', 'YYYY-MM-DD', 'YY-MM-DD'];
  for (let format of formats) {
    let validDate = moment(date, format, true).isValid();
    if (validDate) {
      return true;
    }
  }
  return false;
}

export async function getExpDates(req) {
  let searchClause, orderQuery, tickerClause;
  let { query } = req;

  if (query) {
    if (query.tickers && query.tickers.length > 0) {
      tickerClause = `AND ticker = ANY('{${query.tickers}}')`
    }

    if (query.search && query.search.length > 0) {
      let search = query.search.replace(new RegExp('/', 'g'), '-').toLowerCase(); // replace / for - to match timestamp format for the like match
      let validDate = validateDate(search);
      searchClause = `AND (exp::text like '%${search}%' OR LOWER(to_char(exp, 'Month')) like '%${search}%'`;
      searchClause += validDate ? ` OR exp = '${search}')` : `)`;
    }

    let orderDirection = query.order_direction || 'ASC';
    orderQuery = `ORDER BY exp ${orderDirection}`;
  }

  let expQuery = getExpDatesQuery({
    date: req.query.date,
    orderQuery,
    tickerClause,
    searchClause
  });

  let result;
  try {
    result = await optionsDB(expQuery);
  } catch {
    result = [];
  }

  return result;
};

export async function getOption(id) {
  const result = await optionsDB(`
        SELECT id, time, ticker, exp, strike, cp, spot, contract_quantity, price_per_contract, type, prem
        FROM options
        WHERE id=${id}
        `);
  return result;
}

export async function searchOptions(req) {
  let ticker = req.params.ticker;

  if (ticker && ticker.length > 0) {
    ticker = ticker.toLowerCase();
  } else {
    return [];
  }

  let optionsQuery = getSearchOptionsQuery({
    ticker,
    date: req.query.date
  });

  let result;
  try {
    result = await optionsDB(optionsQuery);
  } catch {
    result = [];
  }

  return result;
}

const getOptionsTablePostfix = (date) => {
  if (!date) {
    return null;
  }

  const currentDate = MTZ().tz('America/New_York').format('YYYY-MM');
  const selectedDate = moment(date).format('YYYY-MM');

  if (currentDate === selectedDate) {
    return null;
  }

  let tablePostfix = date.split('-');
  tablePostfix.length = 2;

  return `_${tablePostfix.join('_')}`;
};

const getSnapshotQuery = ({
  ticker,
  date,
  exps,
  allowHist
}) => {
  let startDate, endDate = null;

  if (date) {
    [startDate, endDate] = date.split(',');
  }

  let dateQuery = `AND to_timestamp(time)::date = (SELECT to_timestamp(MAX(time))::date FROM options${allowHist ? ` WHERE LOWER(ticker) = '${ticker}'` : ''})`;

  if (!endDate && startDate) {
    dateQuery = `AND to_timestamp(time)::date = '${startDate}'`;
  }

  if (!endDate) {
    const tablePostfix = getOptionsTablePostfix(startDate);

    return `${getSnapshotSubquery({
      tablePostfix,
      dateQuery,
      ticker,
      exps
    })} ORDER BY cp ASC`;
  }

  const unionQuery = enumerateDaysBetweenDates(startDate, endDate).map(date => {
    const tablePostfix = getOptionsTablePostfix(date);
    const dateQuery = `AND to_timestamp(time)::date = '${date}'`;

    return getSnapshotSubquery({
      tablePostfix,
      dateQuery,
      ticker,
      exps
    });
  }).join(' UNION ');

  return `SELECT * FROM (${unionQuery}) as results ORDER BY cp ASC`;
}

const getSnapshotSubquery = ({
  tablePostfix,
  dateQuery,
  ticker,
  exps
}) => {
  return `(
    SELECT SUM(contract_quantity) AS flow_count, SUM(prem) AS total_premium, 'C' AS cp, to_timestamp(MAX(time))::date AS date
    FROM options${tablePostfix ? tablePostfix : ''}
    WHERE cp = 'C'
    ${dateQuery}
    ${ticker ? `AND LOWER(ticker) = '${ticker}'` : ''}
    ${exps ? `AND exp = ANY('{${exps}}')` : ''}
  ) UNION (
    SELECT SUM(contract_quantity) AS flow_count, SUM(prem) AS total_premium, 'P' AS cp, to_timestamp(MAX(time))::date AS date
    FROM options${tablePostfix ? tablePostfix : ''}
    WHERE cp = 'P'
    ${dateQuery}
    ${ticker ? `AND LOWER(ticker) = '${ticker}'` : ''}
    ${exps ? `AND exp = ANY('{${exps}}')` : ''}
  )`;
}

const getSentimentQuery = ({
  ticker,
  date,
  exps,
  allowHist
}) => {
  let startDate, endDate = null;

  if (date) {
    [startDate, endDate] = date.split(',');
  }

  let dateQuery = `AND to_timestamp(time)::date = (SELECT to_timestamp(MAX(time))::date FROM options${allowHist ? ` WHERE LOWER(ticker) = '${ticker}'` : ''})`;

  if (!endDate && startDate) {
    dateQuery = `AND to_timestamp(time)::date = '${startDate}'`;
  }

  if (!endDate) {
    const tablePostfix = getOptionsTablePostfix(startDate);

    return `${getSentimentSubquery({
      tablePostfix,
      dateQuery,
      ticker,
      exps
    })} ORDER BY cp ASC`;
  }

  const unionQuery = enumerateDaysBetweenDates(startDate, endDate).map(date => {
    const tablePostfix = getOptionsTablePostfix(date);
    const dateQuery = `AND to_timestamp(time)::date = '${date}'`;

    return getSentimentSubquery({
      tablePostfix,
      dateQuery,
      ticker,
      exps
    });
  }).join(' UNION ');

  return `SELECT * FROM (${unionQuery}) as results ORDER BY cp ASC`;
}

const getSentimentSubquery = ({
  tablePostfix,
  dateQuery,
  ticker,
  exps
}) => {
  return `(
    SELECT SUM(contract_quantity) AS flow_count, 'C' AS cp, to_timestamp(MAX(time))::date AS date
    FROM options${tablePostfix ? tablePostfix : ''}
    WHERE cp = 'C'
    ${dateQuery}
    ${ticker ? `AND LOWER(ticker) = '${ticker}'` : ''}
    ${exps ? `AND exp = ANY('{${exps}}')` : ''}
  ) UNION (
    SELECT SUM(contract_quantity) AS flow_count, 'P' AS cp, to_timestamp(MAX(time))::date AS date
    FROM options${tablePostfix ? tablePostfix : ''}
    WHERE cp = 'P'
    ${dateQuery}
    ${ticker ? `AND LOWER(ticker) = '${ticker}'` : ''}
    ${exps ? `AND exp = ANY('{${exps}}')` : ''}
  )`;
}

const getOptionsQuery = ({
  orderQuery,
  limitQuery,
  last_time,
  ticker,
  date,
  exps
}) => {
  let startDate, endDate = null;

  if (date) {
    [startDate, endDate] = date.split(',');
  }

  let dateQuery = `WHERE to_timestamp(time)::date = (SELECT to_timestamp(time)::date FROM options ORDER BY time DESC LIMIT 1)`;

  if (!endDate && startDate) {
    dateQuery = `WHERE to_timestamp(time)::date = '${startDate}'`;
  }

  if (!endDate) {
    const tablePostfix = getOptionsTablePostfix(startDate);
    const optionsSubquery = getOptionsSubquery({
      tablePostfix,
      last_time,
      dateQuery,
      ticker,
      exps
    });

    return `
      SELECT * FROM ${optionsSubquery} as results
      ${orderQuery}
      ${limitQuery}
    `;
  }

  const unionQuery = enumerateDaysBetweenDates(startDate, endDate).map(date => {
    const tablePostfix = getOptionsTablePostfix(date);
    const dateQuery = `WHERE to_timestamp(time)::date = '${date}'`;

    return getOptionsSubquery({
      tablePostfix,
      last_time,
      dateQuery,
      ticker,
      exps
    });
  }).join(' UNION ');

  return `SELECT * FROM (${unionQuery}) as results ${orderQuery} ${limitQuery}`;
}

const getOptionsSubquery = ({
  tablePostfix,
  last_time,
  dateQuery,
  ticker,
  exps
}) => {
  return `(
    SELECT id, time, ticker, exp, strike, cp, spot, contract_quantity, price_per_contract, type, prem
    FROM options${tablePostfix ? tablePostfix : ''}
    ${dateQuery}
    ${ticker ? `AND LOWER(ticker) = '${ticker}'` : ''}
    ${last_time ? `AND time < ${last_time}` : ''}
    ${exps ? `AND exp = ANY('{${exps}}')` : ''}
  )`;
}

const getExpDatesQuery = ({
  date,
  orderQuery,
  searchClause,
  tickerClause
}) => {
  let startDate, endDate = null;

  if (date) {
    [startDate, endDate] = date.split(',');
  }

  let dateClause = `WHERE to_timestamp(time)::date = (SELECT to_timestamp(time)::date FROM options ORDER BY time DESC LIMIT 1)`;

  if (!endDate && startDate) {
    dateClause = `WHERE to_timestamp(time)::date = '${startDate}'`;
  }

  if (!endDate) {
    const tablePostfix = getOptionsTablePostfix(startDate);
    const optionsSubquery = getExpDatesSubquery({
      tablePostfix,
      dateClause,
      searchClause,
      tickerClause
    });

    return `
      SELECT distinct exp FROM ${optionsSubquery} as results
      ${orderQuery ? orderQuery : 'ORDER BY exp ASC'}
    `;
  }

  const unionQuery = enumerateDaysBetweenDates(startDate, endDate).map(date => {
    const tablePostfix = getOptionsTablePostfix(date);
    const dateClause = `WHERE to_timestamp(time)::date = '${date}'`;

    return getExpDatesSubquery({
      tablePostfix,
      dateClause,
      searchClause,
      tickerClause
    });
  }).join(' UNION ');

  return `SELECT distinct exp FROM (${unionQuery}) as results ${orderQuery ? orderQuery : 'ORDER BY exp ASC'}`;
}

const getExpDatesSubquery = ({
  tablePostfix,
  dateClause,
  searchClause,
  tickerClause
}) => {
  return `(
    SELECT distinct exp
    FROM options${tablePostfix ? tablePostfix : ''}
    ${dateClause}
    ${searchClause ? searchClause : ''}
    ${tickerClause ? tickerClause : ''}
  )`;
}

const getSearchOptionsQuery = ({
  date,
  ticker
}) => {
  let startDate, endDate = null;

  if (date) {
    [startDate, endDate] = date.split(',');
  }

  let dateClause = `WHERE to_timestamp(time)::date = (SELECT to_timestamp(time)::date FROM options ORDER BY time DESC LIMIT 1)`;

  if (!endDate && startDate) {
    dateClause = `WHERE to_timestamp(time)::date = '${startDate}'`;
  }

  if (!endDate) {
    const tablePostfix = getOptionsTablePostfix(startDate);
    const optionsSubquery = getSearchOptionsSubquery({
      tablePostfix,
      dateClause,
      ticker
    });

    return `
      SELECT ticker FROM ${optionsSubquery} as results
      GROUP BY ticker
      ORDER BY (LOWER(ticker) = '${ticker}') desc, length(ticker)
    `;
  }

  const unionQuery = enumerateDaysBetweenDates(startDate, endDate).map(date => {
    const tablePostfix = getOptionsTablePostfix(date);
    const dateClause = `WHERE to_timestamp(time)::date = '${date}'`;

    return getSearchOptionsSubquery({
      tablePostfix,
      dateClause,
      ticker
    });
  }).join(' UNION ');

  return `
    SELECT ticker FROM (${unionQuery}) as results
    GROUP BY ticker
    ORDER BY (LOWER(ticker) = '${ticker}') desc, length(ticker)
  `;
}

const getSearchOptionsSubquery = ({
  tablePostfix,
  dateClause,
  ticker
}) => {
  return `(
    SELECT ticker
    FROM options${tablePostfix ? tablePostfix : ''}
    ${dateClause}
    AND LOWER(ticker) LIKE '%${ticker}%'
    GROUP BY ticker
  )`;
}

const enumerateDaysBetweenDates = (startDate, endDate) => {
  let dates = [];
  let currDate = moment(startDate).startOf('day');
  let lastDate = moment(endDate).startOf('day');

  dates.push(startDate);

  while (currDate.add(1, 'days').diff(lastDate) < 0) {
    dates.push(currDate.clone().format('YYYY-MM-DD'));
  }

  dates.push(endDate);

  return dates;
};
