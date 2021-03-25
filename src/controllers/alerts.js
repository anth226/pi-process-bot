import db from "../db";

import * as trades from "./trades";

export const createAlert = async (name, message, isDaily) => {
  let query = {
    text:
      "INSERT INTO alerts (name, message, daily, active) VALUES ($1, $2, $3, true)",
    values: [name, message, isDaily],
  };

  let result = await db(query);

  return result;
};

export async function getAlert(id) {
  const result = await db(`
        SELECT *
        FROM alerts
        WHERE id=${id}
        `);
  return result;
}

export async function getAlerts(req) {
  let name;
  if (req && req.query) {
    let query = req.query
    if (query.name && query.name.length > 0) {
      name = query.name;
    }
  }
  const result = await db(`
        SELECT *
        FROM alerts
        ${name ? `WHERE name='${name}'` : ''}
        `);
  return result;
}

export async function getAlertUsers(alertID) {
  const result = await db(`
        SELECT *
        FROM alert_users
        WHERE alert_id=${alertID}
        `);
  return result;
}

export async function getAlertUser(alertID, userID) {
  const result = await db(`
        SELECT *
        FROM alert_users
        WHERE alert_id=${alertID} AND user_id='${userID}'
        `);
  return result;
}

export async function getAlertActiveUsers(alertID) {
  const result = await db(`
        SELECT *
        FROM alert_users
        WHERE alert_id=${alertID} AND active = 'true'
        `);
  return result;
}

export async function activateAlert(id) {
  const result = await db(`
        UPDATE alerts 
        SET active = true
        WHERE id=${id}
        `);
  return result;
}

export async function deactivateAlert(id) {
  const result = await db(`
        UPDATE alerts 
        SET active = false
        WHERE id=${id}
        `);
  return result;
}


export const addAlertUser = async (userID, alertID, phoneNumber) => {
  let query = {
    text:
      "INSERT INTO alert_users (user_id, alertID, user_phone_number, created_at) VALUES ($1, $2, $3, now())",
    values: [userID, alertID, phoneNumber],
  };

  return await db(query);
};

export const subscribeAlert = async (phoneNumber, alertID) => {
  let query = {
    text:
      "UPDATE alert_users SET active = true WHERE user_phone_number=($1) AND alert_id=($2)",
    values: [phoneNumber, alertID],
  };

  let result = await db(query);

  await db(`
    UPDATE alerts
    SET subscriber_count = subscriber_count + 1
    WHERE id = '${alertID}'
  `);

  return result;
};

export const unsubscribeAlert = async (phoneNumber, alertID) => {
  let query = {
    text:
      "UPDATE alert_users SET active = false WHERE user_phone_number=($1) AND alert_id=($2)",
    values: [phoneNumber, alertID],
  };
  let result = await db(query);

  await db(`
    UPDATE alerts
    SET subscriber_count = subscriber_count - 1
    WHERE id = '${alertID}'
  `);

  return result;
};

export async function getDailyAlerts() {
 	return await db(`
        SELECT id, name, message
        FROM alerts
        WHERE daily = true AND active = true
		`);
}

// Update Cathie Wood Daily SMS Notification's Message
export async function updateCWDailyAlertMessage() {
  const buyResult = await trades.getTop3Buy();
  const sellResult = await trades.getTop3Sell();

  let message = "Portfolio Insider Prime Alerts\nCathie Woods - Ark Invest Daily Trades\n\n"+
      "Buys\nTicker | Shares | % of EFT";

  for(let i = 0; i < buyResult.length; i++) {
    message += "\n" + buyResult[i].ticker + " " + buyResult[i].shares + " " + buyResult[i].etf_percent + " ";
  }
  message += "\n\nSells\nTicker | Shares | % of EFT";

  for(let x = 0; x < sellResult.length; x++) {
    message += "\n" + sellResult[x].ticker + " " + sellResult[x].shares + " " + sellResult[x].etf_percent + " ";
  }
  message += "\n\nIf you no longer wish to receive these messages, please reply \"END ALERT\" to unsubscribe' WHERE name='CW Daily'";

  let query = {
    text:
      "UPDATE alerts SET message = '"+message+"",
    values: [],
  };

  let result = await db(query);

  return result;
}





