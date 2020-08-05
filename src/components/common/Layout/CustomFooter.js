import React from "react";

import { siteEmailSupport } from "../../../appRedux/actions/helpers";

const CustomFooter = ({ footerClassName, textClassName }) => {
  return (
    <footer className={`custom-footer ${footerClassName}`}>
      {/* <div className="container"> */}
      <div className={`row pb-3`}>
        <div className="col-12">
          <p className={`text-bold ${textClassName}`}>
            No commitment required. Cancel anytime.
          </p>
        </div>
        <div className="col-12 mt-3 mt-lg-0">
          <p className={`${textClassName}`}>
            Limited time offer. Your payment method will automatically be
            charged in advance every month. As indicated above in each offer,
            you will be charged the introductory offer rate for the introductory
            period, and thereafter will be charged the standard rate until you
            cancel. All subscriptions renew automatically. You can cancel
            anytime. These offers are not available for current subscribers.
            Other restrictions and taxes may apply. Offers and pricing are
            subject to change without notice.
          </p>
        </div>
        <div className="col-12">
          <p className={`${textClassName}`}>
            *Your free book will be sent when you renew your membership after 30
            days.
          </p>
        </div>
        <div className="col-12">
          <ul className="footer-links">
            <li className={`${textClassName}`}>Customer Service</li>
            <li>
              <a
                className={`${textClassName}`}
                href={`mailto:${siteEmailSupport}`}
              >
                {siteEmailSupport}
              </a>
            </li>
            <li>
              <a className={`${textClassName}`} href="tel:8779600615">
                (877) 960-0615
              </a>
            </li>
          </ul>
        </div>
      </div>
      {/* </div> */}
    </footer>
  );
};

export default CustomFooter;
