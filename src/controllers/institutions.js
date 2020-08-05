import db from "../db";

export async function getInstitutions({
  sort = [],
  page = 0,
  size = 100,
  ...query
}) {
  return await db(`
    SELECT *
    FROM institutions
    ORDER BY id DESC
    LIMIT ${size}
    OFFSET ${page * size}
  `);
}

export const getInstitutionByCIK = async cik =>
  db(`
    SELECT *
    FROM institutions
    WHERE cik = '${cik}'
  `);


  export async function getInstitutionsUpdated({
    sort = [],
    page = 0,
    size = 100,
    ...query
  }) {
    return await db(`
      SELECT *
      FROM institutions
      WHERE updated_at IS NOT NULL
      ORDER BY cik DESC
      LIMIT ${size}
      OFFSET ${page * size}
    `);
  }