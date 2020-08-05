import React, { useEffect, useState } from "react";

import { useLocation, useHistory } from "react-router-dom";

import Navbar from "./Navbar";
import { BurgerIcon } from "../Icon";
import Button from "../../common/Button";
import Logo from "../Logo";

const navItems = [
  {
    name: "About",
    url: "/about"
  },
  {
    name: "Contact",
    url: "/contact"
  }
];

const CustomHeader = () => {
  const [collapsed, setCollapsed] = useState(true);
  const location = useLocation();
  const history = useHistory();

  useEffect(() => {
    setCollapsed(true);
  }, [location]);

  const handleToggleNav = () => {
    setCollapsed(!collapsed);
  };

  return (
    <header className="header">
      <div className="container py-4 header-block">
        <div className="row align-items-center">
          <div className="col-12 col-lg-4">
            <div className="row align-items-center">
              <div className="col-lg-12 col-10">
                <Logo type="h1" />
              </div>
              <div className="d-lg-none col-2 text-right">
                <Button
                  variant="none"
                  className="navbar-toggler"
                  icon={<BurgerIcon />}
                  onClick={handleToggleNav}
                />
              </div>
            </div>
          </div>
          <div className="col-12 col-lg-7 offset-lg-1 col-xl-6 offset-xl-2 text-right d-none d-lg-block">
            <div className="d-flex justify-content-end align-items-center">
              <Navbar navItems={navItems} theme="navbar-secondary" />
              <div className="ml-3">
                <Button
                  variant="outline"
                  label="Sign In"
                  className="ml-3 animate-up"
                  labelClassname="text-bold"
                  size="lg"
                  onClick={() => history.push("/signin")}
                />
                <Button
                  variant="gradient-primary"
                  label="Subscribe Now"
                  className="ml-3 animate-up"
                  labelClassname="text-bold"
                  size="lg"
                  onClick={() => history.push("/signup")}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
      <div
        className={`custom-header-links d-block d-lg-none ${
          collapsed ? "nav-hidden" : "nav-open"
        }`}
      >
        <div className="container">
          <div className="user-links text-right pt-2">
            <Button
              variant="outline"
              label="Sign In"
              className="ml-3 animate-up"
              onClick={() => history.push("/signin")}
            />
            <Button
              variant="gradient-primary"
              label="Subscribe Now"
              className="ml-3 animate-up"
              onClick={() => history.push("/signup")}
            />
          </div>
          <Navbar navItems={navItems} theme="navbar-secondary" />
        </div>
      </div>
    </header>
  );
};

export default CustomHeader;
