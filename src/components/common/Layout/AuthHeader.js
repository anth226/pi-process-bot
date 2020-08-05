import React from "react";

import Help from "../Help";
import Logo from "../Logo";

const AuthHeader = ({ headerClassName, textClassName, phoneClassName }) => {
  return (
    <header className={`header ${headerClassName}`}>
      <div className="container py-4 header-block">
        <div className="row align-items-center">
          <div className="col-12 col-md-6">
            <Logo className={textClassName} type="h1" />
          </div>
          <div className="col-12 col-md-6 col-xs-12 col-sm-12">
            <div className="text-lg-right text-md-right">
              <Help
                helpClassname={textClassName}
                phoneClassName={phoneClassName}
              />
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};

export default AuthHeader;
