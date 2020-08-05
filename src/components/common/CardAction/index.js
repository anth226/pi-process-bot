import React from "react";

const CardAction = (props) => {
  const { text, style, onClick } = props;

  return (
    <div className={`custom-card-action ${style && style}`} onClick={onClick}>
      <h5>{text}</h5>
    </div>
  );
};

export default CardAction;
