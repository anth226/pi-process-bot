import React from "react";

const Help = ({ helpClassname, phoneClassName }) => {
  return (
    <p className="mb-0">
      <span className={`text-large ${helpClassname}`}>Need Help?</span>{" "}
      <span>
        <a href="tel:8779600615" className={`text-large ${phoneClassName}`}>
          Call (877) 960-0615
        </a>
      </span>
    </p>
  );
};

export default Help;

Help.defaultProps = {
  helpClassname: "text-blue",
  phoneClassName: "text-black"
};
