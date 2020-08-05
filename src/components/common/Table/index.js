import React from "react";

import BootstrapTable from "react-bootstrap-table-next";
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css";
import ToolkitProvider, { Search } from "react-bootstrap-table2-toolkit";
import filterFactory from "react-bootstrap-table2-filter";
import overlayFactory from "react-bootstrap-table2-overlay";

import NoDataAvailable from "../NoDataAvailable";

const { SearchBar } = Search;

const Table = (props) => {
  const {
    keyField,
    columns,
    data,
    defaultSorted,
    bordered,
    rowStyle,
    search,
    expandRow,
    loading,
    overlay
  } = props;

  return (
    <ToolkitProvider
      keyField={keyField}
      data={data}
      columns={columns}
      search={search}
    >
      {(props) => (
        <>
          {search && (
            <>
              <SearchBar {...props.searchProps} />
              <hr />
            </>
          )}

          <BootstrapTable
            {...props.baseProps}
            wrapperClasses="table-responsive"
            classes={"custom-react-table"}
            bordered={bordered}
            bootstrap4
            keyField={keyField}
            data={data}
            columns={columns}
            defaultSorted={defaultSorted}
            rowStyle={rowStyle}
            filter={filterFactory()}
            expandRow={expandRow}
            loading={loading}
            noDataIndication={
              !loading ? (
                <NoDataAvailable message="Sorry! There is not data available." />
              ) : (
                <div style={{ minHeight: "100px" }}></div>
              )
            }
            overlay={
              overlay &&
              overlayFactory({
                spinner: true
              })
            }
          />
        </>
      )}
    </ToolkitProvider>
  );
};

export default Table;

Table.defaultProps = {
  bordered: false,
  search: false,
  loading: false,
  overlay: null
};
