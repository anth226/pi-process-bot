import React from "react";

import AuthHeader from "./AuthHeader";

const TrailTwoLayout = ({ children }) => {
  return (
    <>
      <AuthHeader
        headerClassName="bg-white"
        textClassName="text-blue"
        phoneClassName="text-black"
      />
      <main className="main bg-white custom-layout-trial-two">{children}</main>
    </>
  );
};

export default TrailTwoLayout;
