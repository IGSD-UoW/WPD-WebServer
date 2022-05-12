var express = require('express')
var expressWs = require('@small-tech/express-ws')
var join = require('path').join
// var cors           = require('cors')
// var createSubscriber  = require('pg-listen');
var pgp = require('pg-promise')(/* options */)
var bole = require('bole');
var dbSubscriber = require('../initPglisten')
var sql_query = require('../dashboard/sql_query')
var config = require('../config')
// const WebSocket     = require('ws');

// var expressWs = require('express-ws')(express());

var router = new express.Router()
var expressWebSocket = expressWs(express());
var log = bole('wpd-server')

// global variable of WebSocket Server
let globalWss = null;

// DB CONNECTION - USES DEV_WPDAPI **
var db = pgp(config.azurePostgresdbDEV)

// const dbSchema = 'datalake';
const dbSchema = 'public';
const userSchema = 'auth';
// let dbSubscriber = null;
const channel = `datalake-data-change`;

let resTemplate = {
  'responseTimestamp': null,
  'responseData': null,
  'success': true
}

// ===============
// SEARCH
// ===============

function search(req, res) {
  log.info('>>> dashboard >>> search')

  // EXAMPLE QUERY STRING - ws://localhost:9090/dashboard/search?value={xxx}

  let searchValue = req.query.value

  tempSql_Query = sql_query.search(searchValue)

  db.one(tempSql_Query)
      .then( data => {
        // check if form type doesn't exist in DB
        if(!data.array_to_json) {
          log.error('No data')
          resTemplate.success = false
          resTemplate.responseData = 'No data'
          resTemplate.responseTimestamp = new Date().toISOString()
          res.send(JSON.stringify(resTemplate))
          return
        }
        resTemplate.success = true
        resTemplate.responseTimestamp = new Date().toISOString()
        resTemplate.responseData = data
        log.info('sent response at ', resTemplate.responseTimestamp)
        res.send(JSON.stringify(resTemplate))
      })
      .catch( error => {
        log.error('ERROR while executing DB-query to fetch data :'+ error.message)
        resTemplate.success = false
        resTemplate.responseData = 'ERROR while executing DB-query to fetch data :'+ error.message
        resTemplate.responseTimestamp = new Date().toISOString()
        res.send(JSON.stringify(resTemplate))
      })
}

// ===============
// SIMPLE GEOMETRY - AFTER USER DEFINES LOCATION
// ===============

function simpleGeometry(req, res) {
  log.info('>>> dashboard >>> simple geometry')

    // EXAMPLE QUERY STRING - http://localhost:9090/dashboard/simplegeometry?id={xxx}

  let locationID = req.query.id
  tempSql_Query = sql_query.simpleGeometry(locationID)

  db.one(tempSql_Query)
      .then( data => {
        // check if form type doesn't exist in DB
        if(!data.array_to_json) {
          log.error('No data')
          resTemplate.success = false
          resTemplate.responseData = 'No data'
          resTemplate.responseTimestamp = new Date().toISOString()
          res.send(JSON.stringify(resTemplate))
          return
        }
        resTemplate.success = true
        resTemplate.responseTimestamp = new Date().toISOString()
        resTemplate.responseData = data
        // console.log('sent response at ', resTemplate.responseTimestamp)
        log.info('sent response at ', resTemplate.responseTimestamp)
        res.send(JSON.stringify(resTemplate))
      })
      .catch( error => {
        // console.log('ERROR while executing DB-query to fetch data :', error)
        log.error('ERROR while executing DB-query to fetch data :'+ error.message)
        resTemplate.success = false
        resTemplate.responseData = 'ERROR while executing DB-query to fetch data :'+ error.message
        resTemplate.responseTimestamp = new Date().toISOString()
        res.send(JSON.stringify(resTemplate))
      })
}

// ===============
// PLUVIOMETER RECORDS
// ===============

function pluviometerRecords(req, res) {
  log.info('>>> dashboard >>> pluviometer records (official + citizen)')

    // EXAMPLE QUERY STRING - http://localhost:9090/dashboard/pluviometers?id={xxx}&startDate={xxx}&endDate={xxx}

  let locationID = req.query.id;
  let startDate = req.query.startDate;
  let endDate = req.query.endDate;

  tempSql_Query = sql_query.pluviometerRecords(locationID, startDate, endDate)

  db.one(tempSql_Query)
      .then( data => {
        // check if form type doesn't exist in DB
        if(!data.array_to_json) {
          log.error('No data')
          resTemplate.success = false
          resTemplate.responseData = 'No data'
          resTemplate.responseTimestamp = new Date().toISOString()
          res.send(JSON.stringify(resTemplate))
          return
        }
        resTemplate.success = true
        resTemplate.responseTimestamp = new Date().toISOString()
        resTemplate.responseData = data
        // console.log('sent response at ', resTemplate.responseTimestamp)
        log.info('sent response at ', resTemplate.responseTimestamp)
        res.send(JSON.stringify(resTemplate))
      })
      .catch( error => {
        // console.log('ERROR while executing DB-query to fetch data :', error)
        log.error('ERROR while executing DB-query to fetch data :'+ error.message)
        resTemplate.success = false
        resTemplate.responseData = 'ERROR while executing DB-query to fetch data :'+ error.message
        resTemplate.responseTimestamp = new Date().toISOString()
        res.send(JSON.stringify(resTemplate))
      })
}



// ======================
// OLD CODE STARTS HERE
// ======================

function requestHandler(client, request) {

  client.room = this.setRoom(request);
  if(Object.keys(request.query).length === 0){
    // console.log('Object.keys(request.query).length ', client)
    resTemplate.success = false
    resTemplate.responseData = 'This endpoint needs a \'type\' param'
    resTemplate.responseTimestamp = new Date().toISOString()
    client.send(JSON.stringify(resTemplate))
    client.close()
    return
  }
  // console.log(`New client connected to ${client.room}`);
  log.info(`New client ${request.socket.remoteAddress} connected to ${client.room}`);
  globalWss = this.getWss()

  // timeout after 15 min = 900000 milliseconds
  // since we dont want unlimited time ws
  setTimeout(() => {
    closeOnTimeout(client)
  }, config.websocketTimeout)

  client.on('close', () => {
    log.info(`client ${request.socket.remoteAddress} disconnected which was connected to ${client.room}`)
    dbSubscriber.notifications.off(channel, () => {
      console.log('db notifications turned off ')
    })
  });

}

function closeOnTimeout(client) {
  if(client.readyState === client.OPEN) {
    log.info(`Server Timeout, disconnecting... ${client.room}`);
    resTemplate.success = false
    resTemplate.responseData = 'Server Timeout, disconnecting...'
    resTemplate.responseTimestamp = new Date().toISOString()
    client.send(JSON.stringify(resTemplate))
    client.close()
  }
}

router.use(express.static(join(__dirname, '../../wwwroot')))
// router.use(cors())

// New Endpoints
// Search
router.ws('/search', requestHandler)
router.get('/search', search)

// Simple Geometry
router.ws('/simplegeometry', requestHandler)
router.get('/simplegeometry', simpleGeometry)

// Pluviometer Records
router.ws('/pluviometers', requestHandler)
router.get('/pluviometers', pluviometerRecords)


module.exports = router
