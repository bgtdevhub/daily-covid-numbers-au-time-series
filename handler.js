'use strict';

const config = {
  clientId: process.env.CLIENT_ID,
  clientSecret: process.env.CLIENT_SECRET,
  arcgisServer: 'https://www.arcgis.com/sharing/rest/oauth2/token',

  // For query.js
  // dataSourceUrl: 'https://wabi-australia-southeast-api.analysis.windows.net/public/reports/querydata?synchronous=true',
  // featureServiceUrl: 'https://services1.arcgis.com/vHnIGBHHqDR6y0CR/arcgis/rest/services/Historical_Active_Cases_by_LGA/FeatureServer'
  
  // For queryPostCode.js
  dataSourceUrl: 'https://discover.data.vic.gov.au',
  featureServiceUrlTimeSeries: 'https://services1.arcgis.com/vHnIGBHHqDR6y0CR/arcgis/rest/services/VIC_POA_COVID19_TimeSeries/FeatureServer',
  featureServiceUrl: 'https://services1.arcgis.com/vHnIGBHHqDR6y0CR/arcgis/rest/services/POA_2016_VIC/FeatureServer'
}

module.exports = {
  query: require('./lib/query')(config),
  queryPostCode: require('./lib/queryPostCode')(config)
}
