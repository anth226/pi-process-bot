import React from "react";

import { Link } from "react-router-dom";

const ToggleControl = React.forwardRef(({ children, onClick, icon }, ref) => {
  return (
    <Link
      className="drodown-trigger-label"
      to={""}
      ref={ref}
      onClick={(e) => {
        e.preventDefault();
        onClick(e);
      }}
    >
      {children}
      {icon ? icon : "&#x25bc"}
    </Link>
  );
});

export default ToggleControl;
