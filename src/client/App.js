import React, { useEffect, useState } from "react";
import "./app.css";
import { Nav, Tab, Tabs } from "react-bootstrap";
import "bootstrap/dist/css/bootstrap.min.css";
//import Tabs from "react-bootstrap/Tabs";
import StrongBuys from "../components/custom/securitiesTable";


export default () => {
  const [data, setData] = useState(null);
  useEffect(() => {
    const getData = async () => {
      const response = await fetch("/bot/strongbuys");
      const json = await response.json();
      const rawData = json.data;
      setData(rawData);
    };
    getData();
  }, []);
  return (
    <>
      {data && (
        <div className="container">
          <div className="row">
            <div className="col-12">
              <Tab.Container defaultActiveKey="sbuys">
                <Nav variant="tabs" justify className="custom-nav-tabs">
                  <Nav.Item>
                    <Nav.Link
                      eventKey="sbuys"
                      onMouseDown={(e) => e.preventDefault()}
                    >
                      Strong Long Buys
                    </Nav.Link>
                  </Nav.Item>
                </Nav>
                <Tab.Content className="custom-tab-content bg-white py-4">
                  <Tab.Pane eventKey="sbuys">
                    <StrongBuys data={data} />
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
