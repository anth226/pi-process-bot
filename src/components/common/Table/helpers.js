import React from "react";

import { Link } from "react-router-dom";

import { ArrowUpIcon, ArrowDownIcon } from "../Icon";

const linkFormatter = (cell, row) => {
  return <Link to={`/company/${cell}`}>{cell}</Link>;
};

const sortCaret = (order, column) => {
  if (!order)
    return (
      <span className="hidden-sort">
        <ArrowDownIcon />
      </span>
    );

  if (order === "asc") return <ArrowUpIcon />;
  else if (order === "desc") return <ArrowDownIcon />;
  return null;
};

const headerSortingClasses = (sortOrder) => {
  return sortOrder && "sorting-active";
};

const nullFormatter = (cell, row) => {
  if (!cell) {
    return "-";
  }

  return cell.toFixed(2);
};

export { linkFormatter, sortCaret, headerSortingClasses, nullFormatter };
