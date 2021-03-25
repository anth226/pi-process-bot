import asyncRedis from "async-redis";
import redis from "redis";
import {getEnv} from "./env";

// import { reportError } from "./reporting";

let db;
let sharedCache;
let sharedCacheProd;

export let KEY_NEWS_HEADLINES = "KEY_NEWS_HEADLINES";
export let KEY_NEWS_SOURCES = "KEY_NEWS_SOURCES";
export let KEY_FORBES_TITANS = "KEY_FORBES_TITANS";

export let KEY_CHART_DATA = "KEY_CHART_DATA";

//sharedCache keys
export let CACHED_SYMBOL = "CS";
export let CACHED_PRICE_REALTIME = "C_R";
export let CACHED_PRICE_15MIN = "C_15";
export let CACHED_PRICE_OPEN = "C_O";
export let CACHED_SECURITY = "C_SEC";
export let KEY_SECURITY_PERFORMANCE = "KEY_SEC_PERF";

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

export const connectSharedCache = () => {
  let credentials = {
    host: getEnv("REDIS_HOST_SHARED_CACHE"),
    port: getEnv("REDIS_PORT_SHARED_CACHE"),
  };

  if (!sharedCache) {
    const client = redis.createClient(credentials);
    client.on("error", function (error) {
      //   reportError(error);
    });

    sharedCache = asyncRedis.decorate(client);
  }

  return sharedCache;
};

export const connectSharedCacheProd = () => {
  let credentials = {
    host: getEnv("PROD_REDIS_HOST_SHARED_CACHE"),
    port: getEnv("REDIS_PORT_SHARED_CACHE"),
  };

  if (!sharedCacheProd) {
    const client = redis.createClient(credentials);
    client.on("error", function (error) {
      //   reportError(error);
    });

    sharedCacheProd = asyncRedis.decorate(client);
  }

  return sharedCacheProd;
};

export const syncRedisData = async () => {
  const sharedCache = connectSharedCache();
  const sharedCacheProd = connectSharedCacheProd();
  const keys = await sharedCacheProd.keys('*');

  for await (let key of keys) {
    try {
      const value = await sharedCacheProd.get(key);

      await sharedCache.set(key, value);
    } catch(e) {}
  }

  return true;
}
