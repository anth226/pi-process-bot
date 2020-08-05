import React from "react";

import Spinner from "react-bootstrap/Spinner";

const Loader = (props) => {
  const { type, variant, size, children } = props;

  let element;
  if (type === "default") {
    element = (
      <Spinner animation="border" role="status" variant={variant} size={size}>
        <span className="sr-only">Loading...</span>
      </Spinner>
    );
  } else if (type === "screen") {
    element = (
      <div className="loader-view">
        <Spinner animation="border" role="status" variant={variant} size={size}>
          <span className="sr-only">Loading...</span>
        </Spinner>
        {children}
      </div>
    );
  } else {
    element = (
      <div className="spinner-box">
        <Spinner animation="grow" role="status">
          <span className="sr-only">Loading...</span>
        </Spinner>
        <Spinner animation="grow" role="status">
          <span className="sr-only">Loading...</span>
        </Spinner>
        <Spinner animation="grow" role="status">
          <span className="sr-only">Loading...</span>
        </Spinner>
      </div>
    );
  }

  return <>{element}</>;
};

Loader.defaultProps = {
  type: "multi",
  variant: "dark"
};

export default Loader;
