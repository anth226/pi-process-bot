import React from "react";

import Table from "../common/Table";
import {
  sortCaret,
  headerSortingClasses,
  nullFormatter,
} from "../common/Table/helpers";

const defaultSorted = [
  {
    dataField: "position",
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
    dataField: "position",
    text: "Position",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "ticker",
    text: "Ticker",
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

const StrongBuys = (props) => {
  const { data, items } = props;

  return (
    <div className="row">
      <h1>Strong Long Buys</h1>

      <Table
        defaultSorted={defaultSorted}
        keyField={"position"}
        columns={columns}
        data={data}
        rowStyle={rowStyle}
      />
    </div>
  );
};

export default StrongBuys;
