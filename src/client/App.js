import React, { useEffect, useState } from "react";
import "./app.css";
import { Nav, Tab, Tabs } from "react-bootstrap";
import "bootstrap/dist/css/bootstrap.min.css";
//import Tabs from "react-bootstrap/Tabs";
import Portfolios from "../components/custom/portfolioTable";
import Billionaires from "../components/custom/billionaireTable";
import Holdings from "../components/custom/holdingTable";

export default () => {
  const [portData, setPortData] = useState(null);
  const [billData, setBillData] = useState(null);
  const [holdData, setHoldData] = useState(null);
  useEffect(() => {
    const getBillData = async () => {
      const response = await fetch("/bot/billionaires");
      const json = await response.json();
      const rawData = json.data;
      setBillData(rawData);
    };
    getBillData();
  }, []);
  useEffect(() => {
    const getHoldData = async () => {
      const response = await fetch("/bot/holdings");
      const json = await response.json();
      const rawData = json.data;
      setHoldData(rawData);
    };
    getHoldData();
  }, []);
  useEffect(() => {
    const getPortData = async () => {
      const response = await fetch("/bot/institutions");
      const json = await response.json();
      const rawData = json.data;
      setPortData(rawData);
    };
    getPortData();
  }, []);

  return (
    <>
      {portData && billData && holdData && (
        <div className="container">
          <div className="row">
            <div className="col-12">
              <Tab.Container defaultActiveKey="portfolios">
                <Nav variant="tabs" justify className="custom-nav-tabs">
                  <Nav.Item>
                    <Nav.Link
                      eventKey="billionaires"
                      onMouseDown={(e) => e.preventDefault()}
                    >
                      Billionaires
                    </Nav.Link>
                  </Nav.Item>
                  <Nav.Item>
                    <Nav.Link
                      eventKey="holdings"
                      onMouseDown={(e) => e.preventDefault()}
                    >
                      Holdings
                    </Nav.Link>
                  </Nav.Item>
                  <Nav.Item>
                    <Nav.Link
                      eventKey="portfolios"
                      onMouseDown={(e) => e.preventDefault()}
                    >
                      Portfolios
                    </Nav.Link>
                  </Nav.Item>
                </Nav>
                <Tab.Content className="custom-tab-content bg-white py-4">
                  <Tab.Pane eventKey="billionaires">
                    <Billionaires data={billData} />
                  </Tab.Pane>

                  <Tab.Pane eventKey="holdings">
                    <Holdings data={holdData} />
                  </Tab.Pane>

                  <Tab.Pane eventKey="portfolios">
                    <Portfolios data={portData} />
                  </Tab.Pane>
                </Tab.Content>
              </Tab.Container>
            </div>
          </div>
        </div>
      )}
    </>
  );
};
