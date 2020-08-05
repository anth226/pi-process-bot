import React from "react";

import { Pagination } from "react-bootstrap";

import Table from "../components/common/Table";
import {
  sortCaret,
  headerSortingClasses,
  nullFormatter
} from "../components/common/Table/helpers";

import { Link } from "react-router-dom";




const defaultSorted = [
  {
    dataField: "cik",
    order: "asc"
  }
];


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
    text: "Name",
    sort: true,
    //formatter: linkFormatter,
    sortCaret: sortCaret,
    headerSortingClasses
  },
  {
    dataField: "holdings_page_count",
    text: "x",
    sort: true,
    // formatter: nullFormatter,
    sortCaret: sortCaret,
    headerSortingClasses
  },
  {
    dataField: "holdings_updated_at",
    text: "x",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses
  },/*
  {
    dataField: "json",
    text: "x",
    //formatter: nullFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses
  },
  {
    dataField: "json_allocations",
    text: "x",
    //formatter: numberFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses
  },
  {
    dataField: "json_top_10_holdings",
    text: "x",
    //formatter: numberFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses
  },*/
  {
    dataField: "name",
    text: "x",
    //formatter: numberFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses
  },
  {
    dataField: "updated_at",
    text: "updated_at",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses
  }
];

const Institutions = (props) => {
  const { institutions, items } = props;
  console.log("institutions");
  console.log(institutions);


  return (
    <div className="institutions">
      <h4>Institutions</h4>

      <Table
        defaultSorted={defaultSorted}
        keyField={"cik"}
        columns={columns}
        data={institutions}
      />
      
    </div>
  );
};

export default Institutions;
