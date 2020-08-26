import db from "../db";
import * as queue from "../queue";

export async function getWidgets() {
  let result = await db(`
    SELECT widget_instances.*, widget_data.*, widgets.*
    FROM widget_instances
    JOIN widget_data ON widget_data.id = widget_instances.widget_data_id 
    JOIN widgets ON widgets.id = widget_instances.widget_id 
  `);

  return result;
}

export async function update() {
  let widgets = await getWidgets();

  for (let i = 0; i < widgets.length; i += 1) {
    let { type, dashboard_id } = widgets[0];

    console.log(widgets[0]);
    console.log(type);

    if (dashboard_id == 0) {
      await queue.publish_UpdateGlobalDashboard(widget);
    }
  }
}

export async function processWidget(widgetInstanceId) {
  // use type to decern what function to run ex. MutualFundsTopNDiscount
  // use input relevant input to customize ex. count = 10
  // load result into output for widget_data
}
