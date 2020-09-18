import db from "../db";

const natural = require("natural");

const classifier = new natural.BayesClassifier();

natural.PorterStemmer.attach();

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
  //       let stem = str.tokenizeAndStem();
  //       for (let i in stem) {
  //         if (!isNaN(stem[i])) {
  //           stem.splice(i, 1);
  //         }
  //       }
  //       let doc = "";
  //       for (let i in stem) {
  //         doc += " " + stem[i];
  //       }
  //       classifier.addDocument(doc, sectors[i].label);
  //       classifier.train();
  //     }
  //   }

  return sectors;
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
