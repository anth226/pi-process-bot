import React from "react";

import AuthHeader from "./AuthHeader";
import Footer from "./Footer";

const PublicLayout = ({ children }) => {
  return (
    <>
      <AuthHeader
        headerClassName="bg-white"
        textClassName="text-blue"
        phoneClassName="text-black"
      />
      <main className="main public-layout">{children}</main>
      <Footer />
    </>
  );
};

export default PublicLayout;
