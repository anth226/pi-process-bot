import React from "react";

import { Link } from "react-router-dom";

import Logo from "../Logo";
import { siteEmailSupport } from "../../../appRedux/actions/helpers";

const Footer = () => {
  return (
    <footer className="footer py-5">
      <div className="container">
        <div className="row mb-5 footer-top">
          <div className="col-12 col-lg-5 text-lg-center text-xl-right">
            <Logo className="text-white" url="/titans" linkable={false} />
          </div>
          <div className="col-12 col-lg-7 mt-3 mt-lg-0 text-lg-left">
            <p className="text-pale-blue text-bold text text-large mb-0">
              The Power Of An Investment Bank In Your Pocket
            </p>
          </div>
          <div className="col-12 mt-5">
            <div className="wave"></div>
          </div>
        </div>
        <div className="row footer-bottom">
          <div className="col-12 col-md-12 mb-3 mb-xl-0 col-xl-5">
            <p className="text-bold text-small">
              No commitment required. Cancel anytime.
            </p>
            <p className="text-pale-blue">
              Limited time offer. Your payment method will automatically be
              charged in advance every month. As indicated above in each offer,
              you will be charged the introductory offer rate for the
              introductory period, and thereafter will be charged the standard
              rate until you cancel. All subscriptions renew automatically. You
              can cancel anytime. These offers are not available for current
              subscribers. Other restrictions and taxes may apply. Offers and
              pricing are subject to change without notice.
            </p>
            <p className="text-pale-blue">
              *Your free book will be sent when you renew your membership after
              30 days.
            </p>
          </div>
          <div className="col-12 col-md-6 col-xl-3 offset-md-0 offset-xl-1">
            <h5 className="text-bold text-summer-sky">Company</h5>
            <ul>
              <li>
                <Link to="/about">About</Link>
              </li>
              <li>
                <Link to="/contact">Contact</Link>
              </li>
              <li>
                <Link to="/privacy"> Privacy Policy</Link>
              </li>
              <li>
                <Link to="/terms">Terms of Service</Link>
              </li>
              <li>
                <Link to="/cancel-membership">Membership</Link>
              </li>
            </ul>
          </div>
          <div className="col-12 col-md-6 col-xl-3 mt-3 mt-lg-0">
            <h5 className="text-bold text-summer-sky">Customer Service</h5>
            <ul>
              <li>
                <a href={`mailto:${siteEmailSupport}`}>{siteEmailSupport}</a>
              </li>
              <li>
                <a href="tel:8779600615">(877) 960-0615</a>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </footer>
  );
};

export default Footer;
