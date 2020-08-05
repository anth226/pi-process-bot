import React from "react";

const NoDataAvailable = ({ children, type, message }) => {
  return (
    <div className="no-data">
      {type === "inline" ? (
        <p className="mb-0">{message}</p>
      ) : (
        <div className="no-data-block">
          {message && <p>{message}</p>}
          {children}
        </div>
      )}
    </div>
  );
};

export default NoDataAvailable;

NoDataAvailable.defaultProps = {
  type: "block",
  message: ""
};
