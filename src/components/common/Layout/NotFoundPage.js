import React from "react";

import { Link } from "react-router-dom";

const NotFoundPage = () => {
  return (
    <div className="not-found-page page-center">
      <div className="container">
        <div className="row justify-content-center">
          <div className="col-12 col-md-6">
            <div className="not-found-block">
              <h4>404</h4>
              <p>Sorry, the page you visited does not exist.</p>
              <Link to="/" className="btn btn-primary">
                Back Home
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default NotFoundPage;
