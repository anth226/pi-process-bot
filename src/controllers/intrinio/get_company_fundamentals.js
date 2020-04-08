export default function lookupCompany(intrinioApi, ticker) {
  let res = intrinioApi
    .getCompany(ticker)
    .then(function(data) {
      return data;
    })
    .catch(function(error) {
      return error;
    });

  return res;
}
