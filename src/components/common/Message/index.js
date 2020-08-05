import React from "react";

const Message = (props) => {
  return <p>{props.text}</p>;
};

export default Message;

Message.defaultProps = {
  text: "Oops! Something went wrong, please refresh the page."
};
