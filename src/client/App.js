import React, { useEffect, useState } from 'react';
import './app.css';
import Institutions from './Table'


export default () => {
  const [data, setData] = useState(null);
  useEffect( () => {
    const getData = async () => {
      const response = await fetch('/bot/institutions');
      const json = await response.json();
      const rawData = json.data;
      setData( rawData );
    }
    getData();
  },[])

  return (
    <>
      {
        data && 
          <div>
            <Institutions institutions={data} />
          </div>
      }
    </>
  );
}
