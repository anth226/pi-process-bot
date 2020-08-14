import React from "react";

import Table from "../common/Table";
import {
  sortCaret,
  headerSortingClasses,
  nullFormatter,
} from "../common/Table/helpers";

const defaultSorted = [
  {
    dataField: "cik",
    order: "asc",
  },
];

const jsonFormatter = (cell, row) => {
  if (cell) {
    let data = cell[0];
    if (data && Object.keys(data).length > 0) {
      //let json = JSON.stringify(data);
      return "True";
    }
  }
  return "";
};

const columnStyle = (cell, row, rowIndex, colIndex) => {
  if (!cell) {
    return {
      // red
      backgroundColor: "#ff000094",
      color: "#fff",
    };
  }
};

/*
const linkFormatter = (cell, row) => {
  if (cell) {
    return <Link to={`/company/${cell}`}>{cell}</Link>;
  }
};


const numberFormatter = (cell, row) => {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD"
  }).format(parseInt(cell));
};

*/

const columns = [
  {
    dataField: "cik",
    text: "CIK",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "name",
    text: "Name",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "holdings_page_count",
    text: "Holdings Page Count",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "holdings_updated_at",
    text: "Holdings Updated At",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "json_allocations.allocations",
    text: "Allocations",
    formatter: jsonFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
    style: columnStyle,
  },
  {
    dataField: "json_top_10_holdings.top",
    text: "Top 10 Holdings",
    formatter: jsonFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
    style: columnStyle,
  },
  {
    dataField: "updated_at",
    text: "Updated At",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
];

const rowStyle = (row, rowIndex) => {
  const style = {};
  style.color = "#000";
  style.backgroundColor = "#f7faff";
  style.fontSize = "medium";
  return style;
};

const Portfolios = (props) => {
  const { data, items } = props;

  return (
    <div className="row">
      <h1>Portfolios</h1>

      <Table
        defaultSorted={defaultSorted}
        keyField={"cik"}
        columns={columns}
        data={data}
        rowStyle={rowStyle}
      />
    </div>
  );
};

export default Portfolios;
