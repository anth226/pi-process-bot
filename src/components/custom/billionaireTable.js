import React from "react";

import Table from "../common/Table";
import {
  sortCaret,
  headerSortingClasses,
  nullFormatter,
} from "../common/Table/helpers";

const defaultSorted = [
  {
    dataField: "ciks",
    order: "asc",
  },
];

const notesFormatter = (cell, row) => {
  if (cell) {
    let notes = [];
    for (let i = 0; i < cell.length; i++) {
      let note = cell[i].note;
      if (note != "") {
        notes.push(note + "\n");
      }
    }
    if (notes.length > 0) {
      return notes;
    }
  }
  return "";
};

const ciksFormatter = (cell, row) => {
  if (cell) {
    for (let i = 0; i < cell.length; i++) {
      let cik = cell[i].cik;
      let isPrimary = cell[i].is_primary;
      if (cik != "0000000000" && isPrimary == true) {
        return cik;
      } else if (cik == "0000000000" && isPrimary == true) {
        return cik;
      }
    }
  } else {
    return "No Entry in Ciks Table";
  }
};

const columnStyleNotes = (cell, row, rowIndex, colIndex) => {
  if (!cell) {
    return {
      // yellow
      backgroundColor: "#fed069",
      color: "#fff",
    };
  }
};

const columnStyleCiks = (cell, row, rowIndex, colIndex) => {
  if (!cell) {
    return {
      // red
      backgroundColor: "#ff000094",
      color: "#fff",
    };
  } else if (cell[0].cik == "0000000000") {
    return {
      // yellow
      backgroundColor: "#fed069",
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
    dataField: "name",
    text: "Name",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "net_worth",
    text: "Net Worth",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "uri",
    text: "URI",
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
  {
    dataField: "industry",
    text: "Industry",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "id",
    text: "Billionaire ID",
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
  },
  {
    dataField: "ciks",
    text: "Primary CIK",
    formatter: ciksFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
    style: columnStyleCiks,
  },
  {
    dataField: "notes",
    text: "Notes",
    formatter: notesFormatter,
    sort: true,
    sortCaret: sortCaret,
    headerSortingClasses,
    style: columnStyleNotes,
  },
];

const rowStyle = (row, rowIndex) => {
  const style = {};
  style.color = "#000";
  style.backgroundColor = "#f7faff";
  style.fontSize = "medium";
  return style;
};

const Billionaires = (props) => {
  const { data, items } = props;

  return (
    <div className="row">
      <h1>Billionaires</h1>

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

export default Billionaires;
