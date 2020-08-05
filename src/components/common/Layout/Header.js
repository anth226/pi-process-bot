import React, { useEffect, useState } from "react";

import { NavLink, useLocation, useHistory } from "react-router-dom";
import { NavDropdown } from "react-bootstrap";

import SearchBar from "../../utilities/SearchBar";
import Navbar from "./Navbar";
import { BurgerIcon } from "../Icon";
import Button from "../../common/Button";
import Logo from "../Logo";

const navItems = [
  {
    name: "Titans",
    url: "/titans"
  },
  {
    name: "Insider Activity",
    url: "/insider"
  },
  {
    name: "Mutual Funds",
    url: "/mutual-funds"
  },
  {
    name: "Portfolios",
    url: "/portfolios"
  },
  {
    name: "Futures",
    url: "/futures"
  },
  {
    name: "Screener",
    url: "/screener"
  },
  {
    name: "Savings Accounts",
    url: "/savings"
  },
  {
    name: "Custom Reports",
    url: "/report-generator"
  },
  {
    name: "Markets",
    url: "/markets"
  },
  {
    name: "Market Research",
    url: "/research"
  }
  //   {
  //     name: "News",
  //     url: "/news"
  //   }
];

const MainHeader = () => {
  const [collapsed, setCollapsed] = useState(true);
  const location = useLocation();
  const history = useHistory();

  useEffect(() => {
    setCollapsed(true);
  }, [location]);

  const handleSearchSelect = (ticker) => {
    history.push(`/company/${ticker}`);
  };

  const handleToggleNav = () => {
    setCollapsed(!collapsed);
  };

  const handleSignOut = () => {
    history.push("/signout");
  };

  return (
    <header className="header">
      <div className="container py-4 header-block">
        <div className="row align-items-center main-header-flex-wrapper">
          <div className="col-lg-4 col-12">
            <div className="row align-items-center">
              <div className="col-lg-12 col-10">
                <Logo url="/titans" linkable type="h1" />
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

          <div className="col-lg-6 col-12 pt-2 pt-lg-0 main-searchbar-wrapper">
            <SearchBar
              placeholder="Search company symbol ie: 'AAPL'..."
              handleSelect={handleSearchSelect}
              isIcon
            />
          </div>
          <div className="col-lg-2 d-none d-lg-block text-lg-right main-header-buttons">
            <NavDropdown
              title="My Account"
              id="nav-dropdown"
              className="account-dropdown"
            >
              <NavDropdown.Item
                eventKey="4.1"
                onClick={() => history.push("/watchlist")}
              >
                Watchlist
              </NavDropdown.Item>
              <NavDropdown.Item
                eventKey="4.1"
                onClick={() => history.push("/account")}
              >
                Settings
              </NavDropdown.Item>
              <NavDropdown.Divider />
              <NavDropdown.Item eventKey="4.2" onClick={handleSignOut}>
                Sign Out
              </NavDropdown.Item>
            </NavDropdown>
          </div>
        </div>
      </div>

      <div className={`header-links ${collapsed ? "nav-hidden" : "nav-open"}`}>
        <div className="container">
          <div className="user-links text-right pt-2 d-block d-lg-none">
            <Button
              variant="none"
              label="Watchlist"
              className="ml-2"
              onClick={() => history.push("/watchlist")}
            />
            <Button
              variant="none"
              label="Settings"
              className="ml-2"
              onClick={() => history.push("/account")}
            />
            <Button
              variant="none"
              label=" Sign Out"
              className="ml-2"
              onClick={() => history.push("/signout")}
            />
          </div>

          <Navbar navItems={navItems} />
        </div>
      </div>
    </header>
  );
};

export default MainHeader;
