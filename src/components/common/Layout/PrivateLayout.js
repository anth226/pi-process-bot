import React, { useEffect, useRef } from "react";

import Header from "./Header";
import Footer from "./Footer";

import { useStores } from "../../../store/useStores";
import { useAuth } from "../../../contexts/Auth";
import useDimensions from "../../../hooks/useDimension";

const PrivateLayout = ({ children }) => {
  const { watchlistStore } = useStores();
  const { authUser, authToken, fetchUser } = useAuth();
  const targetRef = useRef();
  const size = useDimensions(targetRef);

  useEffect(() => {
    if (authUser && authToken) {
      fetchUser();
      // watchlistStore.fetchWatchlist();
    }
  }, []);

  // const getStyles = () => {
  //   if (size.height < 1600) {
  //     return { minHeight: "100vh" };
  //   }

  //   return { minHeight: "1000px" };
  // };

  // style={getStyles()}

  return (
    <>
      <Header />
      <main className="main main-layout" ref={targetRef}>
        {children}
      </main>
      <Footer />
    </>
  );
};

export default PrivateLayout;
