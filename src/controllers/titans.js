import db from "../db";

export async function getTitans({ sort = [], page = 0, size = 100, ...query }) {
  return await db(`
    SELECT *
    FROM billionaires
    ORDER BY id DESC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}
