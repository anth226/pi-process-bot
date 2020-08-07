import React from "react";

import { Pagination } from "react-bootstrap";

import Table from "../components/common/Table";
import {
  sortCaret,
  headerSortingClasses,
  nullFormatter,
} from "../components/common/Table/helpers";

import { Link } from "react-router-dom";

const defaultSorted = [
  {
    dataField: "cik",
    order: "asc",
  },
];

const isFormatter = (cell, row) => {
  if (cell) {
    return "True";
  } else {
    return "False";
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
    dataField: "json_allocations",
    text: "Allocations",
    formatter: isFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "json_top_10_holdings",
    text: "Top 10 Holdings",
    formatter: isFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
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
  if (row.numbOfHits === 1) {
    if (!row.cik) {
      style.color = "#fff";
      style.backgroundColor = "#fed069";
    } else if (row.cik !== row.hitsCik) {
      style.backgroundColor = "#ff000094";
      style.color = "#fff";
    }
  }

  return style;
};

const Institutions = (props) => {
  const { institutions, items } = props;

  return (
    <div className="container">
      <div className="row">
        <h4>Institutions</h4>

        <Table
          defaultSorted={defaultSorted}
          keyField={"cik"}
          columns={columns}
          data={institutions}
        />
      </div>
    </div>
  );
};

export default Institutions;
