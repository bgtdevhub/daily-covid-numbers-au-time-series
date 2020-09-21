const axios = require('axios');
const moment = require('moment');
const qs = require('qs');
const csvToJson = require('convert-csv-to-json');

module.exports = function (config) {
  const getCsv = async function (url) {
    const todayDate = moment().utc();
    const { data: rawData } = await axios.get(url);
    return csvToJson.fieldDelimiter(',').csvStringToJson(rawData).map(({ postcode, active, data_date }) => ({
      attributes : {
        POA_CODE: postcode,
        TheDate: todayDate.format('YYYY-MM-DD 02:00:00'), // UTC 2am in 12pm in melbourne
        ActiveCases: active,
        data_date: moment(data_date, "DD/MM/YYYY").format()
      }
    }))
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
      const postCodeData = await getCsv('https://docs.google.com/spreadsheets/d/e/2PACX-1vTwXSqlP56q78lZKxc092o6UuIyi7VqOIQj6RM4QmlVPgtJZfbgzv0a3X7wQQkhNu8MFolhVwMy4VnF/pub?gid=0&single=true&output=csv');

      // 2. Update current daily data
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
      const dataToUpdate = currentDataList && currentDataList.features && currentDataList.features.map(f => {
        const newData = postCodeData.find(d => d.attributes.POA_CODE === f.attributes.POA_CODE_2016);
        if (newData) {
          f.attributes.ActiveCases = newData.attributes.ActiveCases;
          f.attributes.data_date = newData.attributes.data_date;
        }
        return f
      })
      const applyEditsArgs = {
        f: 'json',
        token: token.access_token,
      }
      applyEditsArgs.updates = JSON.stringify(dataToUpdate);
      const { data: applyEditsResults } = await axios.post(`${config.featureServiceUrl}/0/applyEdits`, qs.stringify(applyEditsArgs), {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      })

      return {
        statusCode: 200,
        body: JSON.stringify(
          {
            dailyUpdates: applyEditsResults.updateResults.length
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