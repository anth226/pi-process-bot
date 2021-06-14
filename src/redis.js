import asyncRedis from "async-redis";
import redis from "redis";
import { getEnv } from "./env";

// import { reportError } from "./reporting";

let db;
let priceCache;
let priceCacheProd;
let atsCache;
let atsFallCache;
let chartdb;

export let KEY_NEWS_HEADLINES = "KEY_NEWS_HEADLINES";
export let KEY_NEWS_SOURCES = "KEY_NEWS_SOURCES";
export let KEY_FORBES_TITANS = "KEY_FORBES_TITANS";

export let KEY_CHART_DATA = "KEY_CHART_DATA";

//sharedCache keys
export let CACHED_SECURITY = "C_SEC:"; // security master
export let CACHED_PERF = "PERF:"; // current perf
export let CACHED_NOW = "NOW:"; // current price
export let CACHED_THEN = "THEN:"; // delayed price
export let CACHED_DAY = "DAY:"; // open and close

export const C_CHART = "CHART:";
export const C_CURRENT = "CURRENT:";
export const C_CHART_LD = "LD:CHART:";
export const C_CHART_DL = "DL:CHART:";
export const C_CHART_DL_LD = "LD:DL:CHART:";

//ats redis
export let ATS_CURRENT = "CURRENT:";
export let ATS_DAY = "DAY:";
export let ATS_ALL = "ALL";
export let ATS_DATES = "DATES";
export let ATS_LAST_TIME = "LAST_TIME";
export let ATS_SNAPSHOT = "SNAPSHOT:";
export let ATS_HIGH_DARK_FLOW = "HIGHDARKFLOW";
export let ATS_TRENDING_HIGH_DARK_FLOW = "TRENDINGHIGHDARKFLOW";
export let ATS_HISTORICAL_TRENDING_HIGH_DARK_FLOW = "HISTTRENDINGHIGHDARKFLOW";
export let ATS_TOP_TICKERS = "TOPTICKERS";

function connectDatabase() {
  let credentials = {
    host: getEnv("REDIS_HOST"),
    port: getEnv("REDIS_PORT"),
  };

  if (!db) {
    const client = redis.createClient(credentials);
    client.on("error", function (error) {
      //   reportError(error);
    });

    db = asyncRedis.decorate(client);
  }

  return db;
}

export default connectDatabase();

export const connectPriceCache = () => {
  let credentials = {
    host: getEnv("REDIS_HOST_PRICE_CACHE"),
    port: getEnv("REDIS_PORT"),
  };

  if (!priceCache) {
    const client = redis.createClient(credentials);
    client.on("error", function (error) {
      //   reportError(error);
    });

    priceCache = asyncRedis.decorate(client);
  }

  return priceCache;
};

export const connectPriceCacheProd = () => {
  let credentials = {
    host: getEnv("PROD_REDIS_HOST_PRICE_CACHE"),
    port: getEnv("REDIS_PORT"),
  };

  if (!priceCacheProd) {
    const client = redis.createClient(credentials);
    client.on("error", function (error) {
      //   reportError(error);
    });

    priceCacheProd = asyncRedis.decorate(client);
  }

  return priceCacheProd;
};

export const connectATSCache = () => {
  let credentials = {
    host: getEnv("REDIS_HOST_ATS_CACHE"),
    port: getEnv("REDIS_PORT"),
  };

  if (!atsCache) {
    const client = redis.createClient(credentials);
    client.on("error", function (error) {
      //   reportError(error);
    });

    atsCache = asyncRedis.decorate(client);
  }

  return atsCache;
};

export const connectATSFallbackCache = () => {
  let credentials = {
    host: getEnv("REDIS_HOST_ATS_FALL_CACHE"),
    port: getEnv("REDIS_PORT"),
  };

  if (!atsFallCache) {
    const client = redis.createClient(credentials);
    client.on("error", function (error) {
      //   reportError(error);
    });

    atsFallCache = asyncRedis.decorate(client);
  }

  return atsFallCache;
};

export const connectChartRedis = () => {
  let credentials = {
    host: getEnv("REDIS_HOST_CHART_CACHE"),
    port: getEnv("REDIS_PORT"),
  };

  if (!chartdb) {
    const client = redis.createClient(credentials);
    client.on("error", function (error) {
      //   reportError(error);
    });

    chartdb = asyncRedis.decorate(client);
  }
  return chartdb;
}


export const syncRedisData = async () => {
  const priceCache = connectPriceCache();
  const priceCacheProd = connectPriceCacheProd();
  const keys = await priceCacheProd.keys('*');

  for await (let key of keys) {
    try {
      const value = await priceCacheProd.get(key);

      await priceCache.set(key, value);
    } catch (e) { }
  }

  return true;
};
