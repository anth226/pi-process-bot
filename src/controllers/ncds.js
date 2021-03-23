import db2 from "../db2";

//TODO: add time check for getRawData

export async function getRawData() {
  let result = await db2(`
        SELECT *
        FROM options_raw
        `);

  return result;
}

export async function getSmartTrade(optContract, time) {
  let result = await db2(`
    SELECT *
    FROM options
    WHERE option_contract = '${optContract}' AND time = ${time}
    `);

  return result[0];
}

export async function consolidate() {
  let smartTrades = new Map();

  let result = await getRawData();

  if (result && result.length > 0) {
    for (let i in result) {
      let time = result[i].time;
      let optContract = result[i].option_contract;
      let key = time + "-" + optContract;
      if (smartTrades.has(key)) {
        let trades = smartTrades.get(key);
        trades.push(result[i]);
        smartTrades.set(key, trades);
      } else {
        let trades = [];
        trades.push(result[i]);
        smartTrades.set(key, trades);
      }
    }

    smartTrades.forEach(async (value, key) => {
      //console.log("SMART TRADE");
      let ticker = value[0].ticker;
      let time = value[0].time;
      let exp = value[0].exp;
      let strike = value[0].strike;
      let cp = value[0].cp;
      let spot = value[0].spot;
      let type = value[0].type;
      let optContract = value[0].option_contract;
      let contract_quantity = 0;
      let total_amount = 0;

      for (let j in value) {
        contract_quantity += value[j].volume;
        total_amount += value[j].price * value[j].volume;
      }

      let price_per_contract = total_amount / contract_quantity;

      let prem = contract_quantity * price_per_contract * 100;

      let smTrade = await getSmartTrade(optContract, time);
      //console.log("smTrade", smTrade);

      if (smTrade) {
        //update
        let query = {
          text:
            "UPDATE options SET contract_quantity = $3, price_per_contract = $4, prem = $5 WHERE option_contract = $1 AND time = $2",
          values: [
            optContract,
            time,
            contract_quantity,
            price_per_contract,
            prem,
          ],
        };

        await db2(query);
        console.log("smart option trade updated");
      } else {
        //insert
        let query = {
          text:
            "INSERT INTO options (ticker, time, exp, strike, cp, spot, type, contract_quantity, price_per_contract, prem, option_contract) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING *",
          values: [
            ticker,
            time,
            exp,
            strike,
            cp,
            spot,
            type,
            contract_quantity,
            price_per_contract,
            prem,
            optContract,
          ],
        };
        console.log("hello");
        await db2(query);
        console.log("smart option trade added");
      }
    });
  }
}
//useless comment for commit
