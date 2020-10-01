import React from "react";

import Table from "../common/Table";
import {
  sortCaret,
  headerSortingClasses,
  nullFormatter,
} from "../common/Table/helpers";

const defaultSorted = [
  {
    dataField: "dashboard_id",
    order: "desc",
  },
];

const jsonFormatter = (cell, row) => {
  if (cell) {
    return JSON.stringify(cell);
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

const columns = [
  {
    dataField: "dashboard_id",
    text: "Dashboard ID",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "widget_instance_id",
    text: "Widget Instance ID",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "type",
    text: "Widget Type",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "input",
    text: "Input",
    formatter: jsonFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "output",
    text: "Output",
    formatter: jsonFormatter,
    style: columnStyle,
    sort: true,
    sortCaret: sortCaret,
    style: columnStyle,
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

const Widgets = (props) => {
  const { data, items } = props;

  return (
    <div className="row">
      <h1>Pinned Widgets</h1>

      <Table
        defaultSorted={defaultSorted}
        keyField={"dashboard_id"}
        columns={columns}
        data={data}
        rowStyle={rowStyle}
      />
    </div>
  );
};

export default Widgets;
