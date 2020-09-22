import db from "../db";
import * as etfs from "./etfs";
import * as queue from "../queue";

const natural = require("natural");

const language = "EN";
const defaultCategory = "N";
const defaultCategoryCapitalized = "NNP";

let tokenizer = new natural.WordTokenizer();
let lexicon = new natural.Lexicon(
  language,
  defaultCategory,
  defaultCategoryCapitalized
);
let ruleSet = new natural.RuleSet("EN");
let tagger = new natural.BrillPOSTagger(lexicon, ruleSet);

const classifier = new natural.BayesClassifier();

//natural.PorterStemmer.attach();

export async function getKeywords() {
  let words = new Map();

  let result = await db(`
        SELECT sector_keyword.*, sector.*, keywords.*
        FROM sector_keyword
        JOIN sector ON sector.id = sector_keyword.sector_id 
        JOIN keywords ON keywords.id = sector_keyword.word_id 
      `);

  if (result && result.length > 0) {
    for (let i in result) {
      if (words.has(result[i].sector)) {
        //update
        let wordsList = words.get(result[i].sector);
        wordsList.push(result[i].word);
        words.set(result[i].sector, wordsList);
      } else {
        //add
        words.set(result[i].sector, [result[i].word]);
      }
    }

    return words;
  }
}

export async function trainClassifier() {
  let sectors = {};
  let words = await getKeywords();

  if (words) {
    words.forEach((word, sector) => {
      let docs = "";
      let doc = word;
      for (let d in doc) {
        docs += " " + doc[d];
      }
      sectors[sector] = docs;
    });
  }

  let keys = Object.keys(sectors);
  for (let i in keys) {
    let sector = keys[i];
    let keywords = sectors[keys[i]];
    classifier.addDocument(keywords, sector);
  }

  classifier.train();
  //console.log(sectors);
}

export async function classify(str) {
  let sectors = classifier.getClassifications(str);

  /*  recursive classifier training  */
  //   for (let i in sectors) {
  //     if (sectors[i].value > 0.05) {
  //       let tokens = tokenizer.tokenize(str);
  //       let tags = tagger.tag(tokens);
  //       let tagged = tags.taggedWords;

  //       for (let i in tagged) {
  //         if (
  //           tagged[i].tag &&
  //           (tagged[i].tag == "NN" || tagged[i].tag == "NNS")
  //         ) {
  //           //console.log(tagged[i]);
  //         }
  //       }
  //       //   let doc = "";
  //       //   for (let i in stem) {
  //       //     doc += " " + stem[i];
  //       //   }
  //       //   console.log(doc);
  //       //   classifier.addDocument(doc, sectors[i].label);
  //       //   classifier.train();
  //     }
  //   }

  if (sectors && sectors.length > 0) {
    return sectors;
  }
}

export async function getDescription(ticker, table) {
  let result = await db(`
        SELECT *
        FROM ${table}
        WHERE ticker = '${ticker}'
    `);

  if (result[0]) {
    switch (table) {
      case "companies":
        let comp = result[0];
        if (comp.json && comp.json.long_description) {
          return comp.json.long_description;
        }
      case "etfs":
        let etf = result[0];
        if (etf.json && etf.json.description) {
          return etf.json.description;
        }
    }
  }
}

export async function categorizeTicker(ticker, table) {
  let desc = await getDescription(ticker, table);
  let sectors = await classify(desc);
  let json = JSON.stringify(sectors);

  if (sectors) {
    let query = {
      text: "SELECT * FROM categorizations WHERE ticker = $1",
      values: [ticker],
    };
    let result = await db(query);

    if (result.length > 0) {
      let query = {
        text:
          "UPDATE categorizations SET json_categories = $1 WHERE ticker = $2",
        values: [json, ticker],
      };
      await db(query);
    } else {
      let query = {
        text:
          "INSERT INTO categorizations (json_categories, ticker ) VALUES ( $1, $2 ) RETURNING *",
        values: [json, ticker],
      };
      await db(query);
    }
  }
}

export async function categorizeTickers() {
  let result = await etfs.getDBETFs();
  let table = "etfs";

  if (result.length > 0) {
    for (let i = 0; i < result.length; i += 1) {
      let ticker = result[i].ticker;
      if (ticker) {
        await queue.publish_ProcessCategorization(ticker, table);
      }
    }
  }
}
