export function getAllNews(intrinioApi) {
  let opts = {
    pageSize: 5, // Number | The number of results to return
    nextPage: null // String | Gets the next page of data from a previous API call
  };

  let res = intrinioApi
    .getAllCompanyNews(opts)
    .then(function(data) {
      return data;
    })
    .catch(function(error) {
      return error;
    });

  return res;
}
