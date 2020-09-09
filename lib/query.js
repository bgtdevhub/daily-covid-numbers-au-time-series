const axios = require('axios');
const moment = require('moment');
const qs = require('qs');

module.exports = function (config) {
  return async event => {
    try {
      // 0. Get token
      const { data: token } = await axios.post(config.arcgisServer, qs.stringify({
        client_id: config.clientId,
        client_secret: config.clientSecret,
        grant_type: 'client_credentials'
      }))
  
      // 1. Get data
      const postData = {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"d","Entity":"dimLGA","Type":0},{"Name":"l","Entity":"Linelist","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"LGAName"},"Name":"dimLGA.LGAName"},{"Measure":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"Cases"},"Name":"Linelist.Cases"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"clin_status_n"}}],"Values":[[{"Literal":{"Value":"'Admitted to ICU'"}}],[{"Literal":{"Value":"'Admitted, not known to be in ICU'"}}],[{"Literal":{"Value":"'Home isolation'"}}],[{"Literal":{"Value":"'Hotel detention'"}}],[{"Literal":{"Value":"'Hospital in the home'"}}],[{"Literal":{"Value":"'Under investigation'"}}]]}}}],"OrderBy":[{"Direction":2,"Expression":{"Measure":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"Cases"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1]}]},"DataReduction":{"DataVolume":4,"Primary":{"Top":{}}},"Aggregates":[{"Select":1,"Aggregations":[{"Min":{}},{"Max":{}}]}],"Version":1}}}]},"CacheKey":"{\"Commands\":[{\"SemanticQueryDataShapeCommand\":{\"Query\":{\"Version\":2,\"From\":[{\"Name\":\"d\",\"Entity\":\"dimLGA\",\"Type\":0},{\"Name\":\"l\",\"Entity\":\"Linelist\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"LGAName\"},\"Name\":\"dimLGA.LGAName\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"l\"}},\"Property\":\"Cases\"},\"Name\":\"Linelist.Cases\"}],\"Where\":[{\"Condition\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"l\"}},\"Property\":\"clin_status_n\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"'Admitted to ICU'\"}}],[{\"Literal\":{\"Value\":\"'Admitted, not known to be in ICU'\"}}],[{\"Literal\":{\"Value\":\"'Home isolation'\"}}],[{\"Literal\":{\"Value\":\"'Hotel detention'\"}}],[{\"Literal\":{\"Value\":\"'Hospital in the home'\"}}],[{\"Literal\":{\"Value\":\"'Under investigation'\"}}]]}}}],\"OrderBy\":[{\"Direction\":2,\"Expression\":{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"l\"}},\"Property\":\"Cases\"}}}]},\"Binding\":{\"Primary\":{\"Groupings\":[{\"Projections\":[0,1]}]},\"DataReduction\":{\"DataVolume\":4,\"Primary\":{\"Top\":{}}},\"Aggregates\":[{\"Select\":1,\"Aggregations\":[{\"Min\":{}},{\"Max\":{}}]}],\"Version\":1}}}]}","QueryId":"","ApplicationContext":{"DatasetId":"5b547437-24c9-4b22-92de-900b3b3f4785","Sources":[{"ReportId":"964ef513-8ff4-407c-8068-ade1e7f64ca5"}]}}],"cancelQueries":[],"modelId":1959902}
      const { data: rawData } = await axios.post(config.dataSourceUrl, postData)
  
      // 2. Massage data
      const todayDate = moment().utc();
      const lgaData = rawData.results[0].result.data.dsr.DS[0].PH[0]['DM0']
        .map(d => d.C)
        .filter(([lgaName]) => !+lgaName)
        .map(([lgaName, count]) => ({
          attributes : {
            LGA_Name: lgaName,
            TheDate: todayDate.format('YYYY-MM-DD 02:00:00'), // UTC 2am in 12pm in melbourne
            ActiveCases: count || 1 // In the UI, even though there is no value, the UI still shows 1.
          }
        }))
  
      // 3. Query previous daily data
      const { data: queryResults } = await axios.post(`${config.featureServiceUrl}/0/query`, qs.stringify({
        f: 'json',
        token: token.access_token,
        // where: `CreationDate >= DATE '${todayDate.format('YYYY-MM-DD')} 00:00:00' AND CreationDate < DATE '${todayDate.add(1, 'd').format('YYYY-MM-DD')} 00:00:00'`,
        where: `TheDate = DATE '${todayDate.format('YYYY-MM-DD')} 02:00:00'`,
        outFields: 'OBJECTID, LGA_Name, ActiveCases'
        }), {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        })
      const dataToUpdate = queryResults && queryResults.features && queryResults.features.map(f => {
        const newData = lgaData.find(d => d.attributes.LGA_Name === f.attributes.LGA_Name);
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
        applyEditsArgs.adds = JSON.stringify(lgaData);
      } else {
        applyEditsArgs.updates = JSON.stringify(dataToUpdate);
      }
  
      // 3. Remove previous data & Insert new data
      const { data: applyEditsResults } = await axios.post(`${config.featureServiceUrl}/0/applyEdits`, qs.stringify(applyEditsArgs), {
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
            deleteResults: applyEditsResults.deleteResults.length
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