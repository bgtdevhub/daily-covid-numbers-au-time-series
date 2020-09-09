const axios = require('axios');
const moment = require('moment');
const qs = require('qs');

module.exports = function (config) {
  const recursiveGetData = async function (url, currentData) {
    const { data: rawData } = await axios.get(url);
    if (rawData.result.records.length === 0) {
      return currentData;
    }
  
    const todayDate = moment().utc();
    const postCodeData = rawData.result.records
      .map(({ postcode, active, data_date }) => ({
        attributes : {
          POA_CODE: postcode,
          TheDate: todayDate.format('YYYY-MM-DD 02:00:00'), // UTC 2am in 12pm in melbourne
          ActiveCases: active,
          data_date: moment(data_date, "DD/MM/YYYY").format()
        }
      }))
  
    return await recursiveGetData(`${config.dataSourceUrl}${rawData.result._links.next}`, currentData.concat(postCodeData))
  }  

  return async event => {
    try {
      // 0. Get token
      const { data: token } = await axios.post(config.arcgisServer, qs.stringify({
        client_id: config.clientId,
        client_secret: config.clientSecret,
        grant_type: 'client_credentials'
      }))

      // 1. Get & Massage data
      const postCodeData = await recursiveGetData(`${config.dataSourceUrl}/api/3/action/datastore_search?resource_id=b952711e-c667-4e4f-b028-ef8af645d06b`, [])

      // 3. Query previous daily data
      const todayDate = moment().utc();
      const { data: queryResults } = await axios.post(`${config.featureServiceUrlTimeSeries}/0/query`, qs.stringify({
        f: 'json',
        token: token.access_token,
        where: `TheDate = DATE '${todayDate.format('YYYY-MM-DD')} 02:00:00'`,
        outFields: 'OBJECTID, POA_CODE, ActiveCases'
        }), {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        })
      const dataToUpdate = queryResults && queryResults.features && queryResults.features.map(f => {
        const newData = postCodeData.find(d => d.attributes.POA_CODE === f.attributes.POA_CODE);
        if (newData) {
          f.attributes.ActiveCases = newData.attributes.ActiveCases;
        }
        return f
      })
      const applyEditsArgs = {
        f: 'json',
        token: token.access_token,
      }
      if (queryResults.features.length === 0) {
        applyEditsArgs.adds = JSON.stringify(postCodeData.map(p => ({
          attributes : {
            POA_CODE: p.attributes.POA_CODE,
            TheDate: p.attributes.TheDate,
            ActiveCases: p.attributes.ActiveCases
          }
        })));
      } else {
        applyEditsArgs.updates = JSON.stringify(dataToUpdate);
      }
  
      // 3. Remove previous data & Insert new data
      const { data: applyEditsResults } = await axios.post(`${config.featureServiceUrlTimeSeries}/0/applyEdits`, qs.stringify(applyEditsArgs), {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      })

      // 4. Update current daily data
      const { data: currentDataList } = await axios.post(`${config.featureServiceUrl}/0/query`, qs.stringify({
        f: 'json',
        token: token.access_token,
        where: '1=1',
        returnGeometry: false,
        outFields: 'OBJECTID, POA_CODE_2016, ActiveCases'
        }), {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        })
      const dataToUpdate2 = currentDataList && currentDataList.features && currentDataList.features.map(f => {
        const newData = postCodeData.find(d => d.attributes.POA_CODE === f.attributes.POA_CODE_2016);
        if (newData) {
          f.attributes.ActiveCases = newData.attributes.ActiveCases;
          f.attributes.data_date = newData.attributes.data_date;
        }
        return f
      })
      const applyEditsArgs2 = {
        f: 'json',
        token: token.access_token,
      }
      applyEditsArgs2.updates = JSON.stringify(dataToUpdate2);
      const { data: applyEditsResults2 } = await axios.post(`${config.featureServiceUrl}/0/applyEdits`, qs.stringify(applyEditsArgs2), {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      })


      return {
        statusCode: 200,
        body: JSON.stringify(
          {
            addResults: applyEditsResults.addResults.length,
            updateResults: applyEditsResults.updateResults.length,
            deleteResults: applyEditsResults.deleteResults.length,
            dailyUpdates: applyEditsResults2.updateResults.length
          },
          null,
          2
        ),
      };
    } catch(e) {
      console.log(e)
      return {
        statusCode: 500,
        body: e.message
      };
    }
  }
}