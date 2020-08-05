import React from "react";

import AuthHeader from "./AuthHeader";

const TrailThreeLayout = ({ children }) => {
  return (
    <>
      <AuthHeader
        headerClassName="bg-dark-blue"
        textClassName="text-white"
        phoneClassName="text-white"
      />
      <main className="main bg-dark-blue custom-layout-trial-three">
        {children}
      </main>
    </>
  );
};

export default TrailThreeLayout;
