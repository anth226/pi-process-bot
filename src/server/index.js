const express = require("express");
const os = require("os");

import * as institutions from "../controllers/institutions";

const app = express();

require("dotenv").config();

app.use(express.static("dist"));

app.get("/api/getUsername", (req, res) => {
  res.send({ username: os.userInfo().username });
})

export async function getInstData() {
  let result = await institutions.getInstitutionsUpdated({ size: 1000 });
  
  if (result.length > 0) {
    return result;
  }
  else {
    return "error";
  }
  
}

app.get("/bot/institutions/", async (req, res) => {
  let data = await getInstData()
  //console.log(data);
  res.send({ data });
});

app.listen(process.env.PORT || 8080, () =>
  console.log(`Listening on port ${process.env.PORT || 8080}!`)
);
