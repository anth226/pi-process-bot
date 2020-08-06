import db from "../db";
import * as titans from "./titans";

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

export async function backfillInstitution_Billionaire(cik,id) {
  institution = getInstitutionByCIK(cik);
  if (institution.length > 0){
    let titan = await titans.getBillionaire(id);
    if (titan){
      let name = titan.name;
      let query = {
        text:
          "INSERT INTO institutions (name, cik) VALUES ( $1, $2 ) RETURNING *",
        values: [name, cik],
      };
      let result = await db(query);
    }
  }
}

    //  do i need json collumn somehow? or check if it's already there from cik in there?