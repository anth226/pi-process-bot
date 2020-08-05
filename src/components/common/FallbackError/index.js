import React from "react";

import { Link } from "react-router-dom";

const FallbackError = (props) => {
  const { message } = props;
  return (
    <div className="fallback-error">
      <h4>500</h4>
      <p>{message}</p>
      <Link to="/" className="btn btn-primary">
        Back Home
      </Link>
    </div>
  );
};

export default FallbackError;

FallbackError.defaultProps = {
  message: "Sorry, something went wrong."
};
