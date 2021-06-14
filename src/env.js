
export function getEnv(env) {
let release = process.env.RELEASE_STAGE;
let json = require(`../pi-process-bot-env-${release}.json`);
let value = json[env];
return value;
}