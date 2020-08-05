import React from "react";

import { NavLink } from "react-router-dom";

const Navbar = ({ navItems, theme }) => {
  return (
    <nav
      className={`navbar navbar-expand-lg navbar-custom ${
        theme ? "navbar-secondary" : "navbar-primary"
      }`}
    >
      <ul className="navbar-nav py-3 py-lg-0">
        {navItems.map((item, index) => (
          <li key={index} className="nav-item">
            <NavLink to={item.url} className="nav-link">
              {item.name}
            </NavLink>
          </li>
        ))}
      </ul>
    </nav>
  );
};

export default Navbar;
