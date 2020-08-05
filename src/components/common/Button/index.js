import React from "react";

import { Button as BootstrapButton } from "react-bootstrap";

const Button = (props) => {
  const {
    type,
    className,
    labelClassname,
    iconClassname,
    variant,
    size,
    block,
    label,
    onClick,
    icon,
    iconPosition,
    style,
    onMouseEnter,
    onMouseLeave
  } = props;

  return (
    <BootstrapButton
      type={type}
      className={`${className}`}
      variant={variant && variant}
      size={size}
      block={block}
      onClick={onClick}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      style={style}
    >
      {icon && iconPosition === "left" && (
        <>
          <span className={`btn-icon icon-left ${iconClassname}`}>{icon}</span>
          {label && (
            <span className={`btn-label btn-label-right ${labelClassname} `}>
              {label}
            </span>
          )}
        </>
      )}

      {label && !icon && (
        <>
          <span className={`btn-label ${labelClassname} `}>{label}</span>
        </>
      )}

      {icon && !iconPosition && (
        <>
          <span className={`btn-icon ${iconClassname}`}>{icon}</span>
        </>
      )}

      {icon && iconPosition === "right" && (
        <>
          {label && (
            <span className={`btn-label btn-label-left ${labelClassname} `}>
              {label}
            </span>
          )}
          <span className={`btn-icon icon-right ${iconClassname}`}>{icon}</span>
        </>
      )}
    </BootstrapButton>
  );
};

export default Button;

Button.defaultProps = {
  type: "button",
  className: "",
  labelClassname: "",
  iconClassname: "",
  size: "md",
  variant: "light",
  block: false,
  iconPosition: ""
};
