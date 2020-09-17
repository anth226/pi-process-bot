const natural = require("natural");

var classifier = new natural.BayesClassifier();

export async function getKeywords() {
  let result = await db(`
        SELECT sector_keyword.*, sector.*, keywords.*
        FROM sector_keyword
        JOIN sector ON sector.id = sector_keyword.sector_id 
        JOIN keywords ON keywords.id = sector_keyword.word_id 
      `);

  if (result && result.length > 0) {
    return result;
  }
}
