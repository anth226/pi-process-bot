import axios from "axios";
import { find } from "lodash";
// import cheerio from "cheerio";

import { fetchBillionaireList, getNetworth } from "../controllers/titans";

(async () => {
  let list = await fetchBillionaireList();
  let value = await getNetworth(list, "warren-buffett");

  console.log(value);
})();
