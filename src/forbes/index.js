import axios from "axios";
import { find } from "lodash";
// import cheerio from "cheerio";

(async () => {
  let list = await fetchBillionaireList();
  let value = await getNetworth(list, "mehmet-hattat");

  console.log(value);
})();

export async function fetchBillionaireList() {
  let billionaires = [];

  try {
    let url = `https://www.forbes.com/ajax/list/data?year=2020&uri=billionaires&type=person`;
    const result = await axios.get(url);
    billionaires = result.data;
  } catch (e) {
    console.error(e);
  }
  return billionaires;
}

export async function getNetworth(list, uri) {
  let value;

  //   try {
  //     let url = `https://www.forbes.com/profile/${uri}`;
  //     const result = await axios.get(url);
  //     const $ = await cheerio.load(result.data);
  //     value = $("div.profile-info__item-value").text();
  //   } catch (e) {
  //     console.log(e);
  //   }

  //   console.log("value", value);

  let billionaire = await find(list, { uri });

  console.log(billionaire);

  value = billionaire["realTimeWorth"];
  return value;
}
