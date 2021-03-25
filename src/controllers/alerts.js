import db from "../db";

import * as trades from "./trades";

// Update Cathie Wood Daily SMS Notification's Message
export async function updateCWDailyAlertMessage() {
  const buyResult = await trades.getTop3Buy();
  const sellResult = await trades.getTop3Sell();

  let message = "Portfolio Insider Prime Alerts\nCathie Wood - Ark Invest Daily Trades\n\n"+
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

export async function getDailyAlerts() {
 	return await db(`
        SELECT id, name, message
        FROM alerts
        WHERE daily = true AND active = true
		`);
}

export async function getAlertActiveUsers(alertID) {
  const result = await db(`
        SELECT *
        FROM alert_users
        WHERE alert_id=${alertID} AND active = 'true'
        `);
  return result;
}

