import { scheduleJob } from 'node-schedule';
import moment from 'moment';
import { orderBy } from "lodash";

import db from "./db";
import db1 from "./db1";

// cron to update trending titans which are most viewed in last 24 hours
const updateTrendingTitans = async () => {
  const result = await db(`
    SELECT widget_instances.*, widget_data.*, widgets.*
    FROM widget_instances
    JOIN widget_data ON widget_data.id = widget_instances.widget_data_id 
    JOIN widgets ON widgets.id = widget_instances.widget_id
    WHERE widgets.type = 'TitansTrending' AND widget_instances.dashboard_id = 0
  `);

  if (result && result.length === 0) {
    return
  }

  const start = moment().startOf('day').format()
  const end = moment().endOf('day').format()

  const groupByTitans = await db1(`
    SELECT titan_uri FROM titans t2 WHERE created_at BETWEEN '${start}' AND '${end}' group by titan_uri
  `);

  let titans = []

  for (const titan of groupByTitans) {
    const { titan_uri } = titan
    const titanCount = await db1(`
      SELECT count(*) FROM titans t2 WHERE titan_uri = '${titan_uri}' and created_at BETWEEN '${start}' AND '${end}'
    `);
    const titanData = await db(`
      SELECT * FROM billionaires WHERE uri = '${titan_uri}'
    `);

    if (titanData && titanData.length > 0) {
      titans.push({
        id: titanData[0].id,
        name: titanData[0].name,
        photo_url: titanData[0].photo_url,
        uri: titanData[0].uri,
        views: titanCount[0].count
      })
    }
  }


  titans = orderBy(titans, 'views', 'desc');
  if (titans.length > 25) {
    titans.length = 25
  }

  if (titans && titans.length > 0) {
    const json = JSON.stringify(titans);
    const query = {
      text:
        "UPDATE widget_data SET output = $2, updated_at = now() WHERE id = $1",
      values: [result[0].widget_data_id, json],
    };
    await db(query);
  }

  return titans;
}

scheduleJob('00 01 * * *', () => { // Schedule time is 1:00 AM
  updateTrendingTitans()
})

