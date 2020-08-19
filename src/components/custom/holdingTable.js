import React from "react";

import Table from "../common/Table";
import {
  sortCaret,
  headerSortingClasses,
  nullFormatter,
} from "../common/Table/helpers";

const defaultSorted = [
  {
    dataField: "batch_id",
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
      backgroundColor: "#87fff9",
      color: "#fff",
    };
  }
};

const isFormatter = (cell, row) => {
  if (cell) {
    return "True";
  }
  return "";
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
    currency: "USD",
  }).format(parseInt(cell));
};

*/

const columns = [
  {
    dataField: "cik",
    text: "CIK",
    sort: false,
    // sortCaret: sortCaret,
    // headerSortingClasses,
  },
  {
    dataField: "data_url",
    text: "Data URL",
    formatter: isFormatter,
    sort: false,
    // sortCaret: sortCaret,
    // headerSortingClasses,
  },
  {
    dataField: "id",
    text: "ID",
    sort: false,
    // sortCaret: sortCaret,
    // headerSortingClasses,
  },
  {
    dataField: "created_at",
    text: "Created At",
    sort: false,
    // sortCaret: sortCaret,
    // headerSortingClasses,
  },
  {
    dataField: "batch_id",
    text: "Latest Batch",
    sort: false,
    // sortCaret: sortCaret,
    // headerSortingClasses,
  },
];

const rowStyle = (row, rowIndex) => {
  const style = {};
  style.color = "#000";
  style.backgroundColor = "#f7faff";
  style.fontSize = "medium";
  return style;
};

const Holdings = (props) => {
  const { data, items } = props;

  return (
    <div className="row">
      <h1>Holdings</h1>

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

export default Holdings;
