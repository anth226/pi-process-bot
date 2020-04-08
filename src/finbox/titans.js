import axios from "axios";

export async function getAll() {
  let portfolios = axios
    .get("https://makeshift.finbox.com/v4/ideas?limit=30&skip=0")
    .then(function(res) {
      return res;
    })
    .catch(function(err) {
      return err;
    });

  return portfolios.then(data => data.data);
}

// export async function getPortfolios(investorTypes, ...sectors) {
export async function getPortfolios({ investorTypes, sectors, gte, lte }) {
  let cleanInvestorType =
    investorTypes.charAt(0).toUpperCase() + investorTypes.substring(1);

  // let paramFilters = {"filters":{"investor_types":[cleanInvestorType], "sectors": []},"limit":30,"skip":0}
  let cleanSectors = [];
  let sectorsArr;

  // if (sectors.length > 0) {
  //     sectorsArr = sectors.split(",")
  //     for (let s of sectorsArr) {
  //         let capitalized = s.charAt(0).toUpperCase() + s.substring(1)
  //         cleanSectors.push(capitalized)
  //     }

  let paramFilters = {
    filters: {
      asset_price_return_1y: { $gte: gte, $lte: lte },
      investor_types: [cleanInvestorType],
      sectors: sectors
    },
    limit: 30,
    skip: 0
  };
  // let paramFilters = {"filters":{"investor_types":[cleanInvestorType],"sectors": sectors},"limit":30,"skip":0}

  // {"filters":{"asset_price_return_1y":{"$gte":-0.19999999999999993,"$lte":0.17999999999999983},"investor_types":["Billionaire"],"sectors":["Financials"]},"limit":30,"skip":0}

  let portfolios = axios
    .post("https://makeshift.finbox.com/v4/ideas/query", paramFilters)
    .then(function(res) {
      return res;
    })
    .catch(function(err) {
      console.log(err);
      return err;
    });

  // console.log(portfolios.then((data) => data.data))
  return portfolios.then(data => data.data);
}
// {"filters":{"investor_types":["Activist Investor"]},"limit":30,"skip":0}

export async function getSinglePortfolioData(portfolioName) {
  let portfolio = axios
    .get(`https://makeshift.finbox.com/v4/ideas/${portfolioName}`)
    .then(function(res) {
      return res;
    })
    .catch(function(err) {
      return err;
    });

  return portfolio.then(data => data.data);
}

export async function getTitans({ investorTypes, sectors, gte, lte }) {}
