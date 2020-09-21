const axios = require('axios');
const moment = require('moment');
const qs = require('qs');
const csvToJson = require('convert-csv-to-json');

module.exports = function (config) {
  const getCsv = async function (url) {
    const { data: rawData } = await axios.get(url);
    return csvToJson.fieldDelimiter(',').csvStringToJson(rawData).map(({ postcode, active, data_date }) => ({
      attributes : {
        POA_CODE: postcode,
        ActiveCases: active,
        TheDate: moment(data_date, "DD/MM/YYYY").format('YYYY-MM-DD 02:00:00')
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

      // 2. For each postcode, check if the postcode and date already exist
      // if yes, update
      // if no, add
      const adds = [];
      const updates = [];
      for (let x = 0; x < postCodeData.length - 1; x++) {
        const { data: queryResultByPostCodeAndDate } = await axios.post(`${config.featureServiceUrlTimeSeries}/0/query`, qs.stringify({
          f: 'json',
          token: token.access_token,
          where: `TheDate = DATE '${postCodeData[x].attributes.TheDate}' and POA_CODE = '${postCodeData[x].attributes.POA_CODE}'`,
          outFields: 'OBJECTID, POA_CODE, ActiveCases'
        }), {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        })

        if (queryResultByPostCodeAndDate.error) {
          continue;
        }

        if (queryResultByPostCodeAndDate.features && queryResultByPostCodeAndDate.features.length === 0) {
          adds.push({
            attributes : {
              POA_CODE: postCodeData[x].attributes.POA_CODE,
              TheDate: postCodeData[x].attributes.TheDate,
              ActiveCases: postCodeData[x].attributes.ActiveCases
            }
          })
        } else {
          updates.push({
            attributes : {
              OBJECTID: queryResultByPostCodeAndDate.features[0].attributes.OBJECTID,
              ActiveCases: postCodeData[x].attributes.ActiveCases
            }
          })
        }
      }

      const applyEditsArgs = {
        f: 'json',
        token: token.access_token,
      }
      applyEditsArgs.adds = JSON.stringify(adds);
      applyEditsArgs.updates = JSON.stringify(updates);
      const { data: applyEditsResults } = await axios.post(`${config.featureServiceUrlTimeSeries}/0/applyEdits`, qs.stringify(applyEditsArgs), {
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