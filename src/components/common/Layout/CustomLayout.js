import React from "react";

import AuthHeader from "./AuthHeader";
import Header from "./Header";
import CustomHeader from "./CustomHeader";
import Footer from "./Footer";
import { useAuth } from "../../../contexts/Auth";

const CustomLayout = ({ children }) => {
  const { authUser, authToken } = useAuth();

  return (
    <>
      {authUser && authToken ? <Header /> : <CustomHeader />}
      <main className="main bg-white custom-layout">{children}</main>
      <Footer />
    </>
  );
};

export default CustomLayout;
