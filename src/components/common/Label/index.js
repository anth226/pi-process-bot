import React from "react";

const Label = (props) => {
  const { text } = props;

  return <label className="label label-custom">{text}</label>;
};

export default Label;
