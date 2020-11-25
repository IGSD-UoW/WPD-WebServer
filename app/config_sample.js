var config = module.exports
var PRODUCTION = process.env.NODE_ENV === 'production'

config.express = {
  port: process.env.EXPRESS_PORT || 9090,
  ip: '127.0.0.1'
}

config.postgresdb = {
  port: process.env.POSTGRESDB_PORT || 25432,
  host: process.env.POSTGRESDB_HOST || 'localhost',
  databaseName: 'wpdWiki',
  schema: 'wpdapi'
}

config.wiki = {
  port: process.env.WIKI_PORT || 8080,
  host: process.env.WIKI_HOST || 'localhost',
  groupId: 4
}

config.wikiApi = {
  url: 'http://'+ config.wiki.host +':'+ config.wiki.port +'/graphql'
}

// config.mongodb = {
//   port: process.env.MONGODB_PORT || 27017,
//   host: process.env.MONGODB_HOST || 'localhost'
// }

config.wikiApiUserCredentials = {
  username: "api@wpd.com",
  password: "api@wpd.com"
}

if (PRODUCTION) {
  // for example
  config.express.ip = '0.0.0.0';
  config.postgresdb.host = 'waterproofing.geog.uni-heidelberg.de';
  config.wiki.host = 'waterproofing.geog.uni-heidelberg.de';
}
// config.db same deal
// config.email etc
// config.log
