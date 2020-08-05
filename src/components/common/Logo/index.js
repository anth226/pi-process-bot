import React from "react";

import { Link } from "react-router-dom";
import { siteTitle } from "../../../appRedux/actions/helpers";

const Logo = (props) => {
  const { type, className, linkable, url, external } = props;

  let element;
  if (linkable && external) {
    element = (
      // <a
      //   href={`${url}`}
      //   className={className}
      //   // target="_blank"
      //   rel="noopener noreferrer"
      // >
      //   {siteTitle}
      // </a>

      <Link to={`${url}`} className={className}>
        {siteTitle}
      </Link>
    );
  } else if (linkable && !external) {
    element = (
      <Link to={`${url}`} className={className}>
        {siteTitle}
      </Link>
    );
  } else {
    element = <span className={className}>{siteTitle}</span>;
  }

  if (type === "h1") {
    return (
      <h1 className={`text-capitalize d-inline-block logo ${className}`}>
        {element}
      </h1>
    );
  }

  return (
    <h4 className={`text-capitalize d-inline-block logo ${className}`}>
      {element}
    </h4>
  );
};

export default Logo;

Logo.defaultProps = {
  className: "",
  url: "",
  external: false,
  linkable: true
};
