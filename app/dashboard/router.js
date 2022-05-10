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

function search(req, res) {
  log.info('>>> dashboard >>> search')
  console.log(req.query)
  // sample url params
  // ws://localhost:9090/hot/databybbox?type=PLUVIOMETERS_OFFICIAL&bbox=-67.98451956245826,-10.09049971309554,-67.69796501946632,-9.900096285440455

  let searchValue = req.query.value

  // check what type of data requested based on params received
  tempSql_Query = sql_query.search(searchValue, dbSchema, userSchema)

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


// SIMPLE GEOMETRY - AFTER USER DEFINES LOCATION

function simpleGeometry(req, res) {
  log.info('>>> dashboard >>> simple geometry')
  console.log(req.query)

  let searchValue = req.query.id

  // check what type of data requested based on params received
  tempSql_Query = sql_query.simpleGeometry(searchValue, dbSchema, userSchema)

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


function getData(ws, req) {
  log.info('>>> hot >>> getData')
  // sample url params
  // ws://localhost:9090/hot/data?type=stacao_pluviometricas&time=2021-07-15/2021-07-16&lat=-9.98132&lon=-67.81544&buffer=2000&limit=5

  let tempFormType = req.query.type
  // ISO 8601 time interval string eg: 2015-01-17T09:50:04/2015-04-17T08:29:55
  let timeRange = req.query.time
  let lat = req.query.lat
  let lon = req.query.lon
  let buffer = req.query.buffer
  let limit = req.query.limit
  let timeStart = null
  let timeEnd = null
  // let bbox = req.query.bbox
  if(timeRange) {
    timeStart = timeRange.substring(0, timeRange.indexOf('/'))
    timeEnd = timeRange.substring(timeRange.indexOf('/') + 1, timeRange.length)
  }

  // check what type of data requested based on params received
  // console.log(tempFormType, lat, lon, buffer, timeStart, timeEnd, limit, dbSchema)
  let tempSql_Query = ''
  let tempSql_QueryById = ''
  // spatial with point and buffer
  if((lat && lon && buffer) && (!timeStart && !timeEnd)){
    tempSql_Query = sql_query.dataQuery(tempFormType, lat, lon, buffer, timeStart, timeEnd, limit, dbSchema, userSchema)
    // no fcode required as we check fcode with tempFormType befreo firing query
    // no limit required as its query by ID which should return just 1 row
    tempSql_QueryById = sql_query.notifybyIdWithFia(tempFormType, lat, lon, buffer, timeStart, timeEnd, dbSchema, userSchema)
  }
  // temporal
  else if((!lat && !lon && !buffer) && (timeStart && timeEnd)){
    tempSql_Query = sql_query.dataQuery(tempFormType, lat, lon, buffer, timeStart, timeEnd, limit, dbSchema, userSchema)
    // if request is for temporal then newly inserted values
    tempSql_QueryById = sql_query.notifybyIdWithFia(tempFormType, lat, lon, buffer, timeStart, timeEnd, dbSchema, userSchema)
  }
  // only bbox
  // else if(bbox && (!lat && !lon && !buffer))
  //   tempSql_Query = sql_query.dataByBbox(tempFormType, bbox, dbSchema)

  // spatio-temporal with point and buffer
  else if((lat && lon && buffer) && (timeStart && timeEnd)){
    tempSql_Query = sql_query.dataQuery(tempFormType, lat, lon, buffer, timeStart, timeEnd, limit, dbSchema, userSchema)
    tempSql_QueryById = sql_query.notifybyIdWithFia(tempFormType, lat, lon, buffer, timeStart, timeEnd, dbSchema, userSchema)
  }
  // only form name
  else if((!lat && !lon && !buffer) && (!timeStart && !timeEnd) && (tempFormType)){
    tempSql_Query = sql_query.dataQuery(tempFormType, lat, lon, buffer, timeStart, timeEnd, limit, dbSchema, userSchema)
    tempSql_QueryById = sql_query.notifybyIdWithFia(tempFormType, lat, lon, buffer, timeStart, timeEnd, dbSchema, userSchema)
  }
  else {
    log.error('Invalid Parameter passed')
    resTemplate.success = false
    resTemplate.responseData = 'Invalid Parameter passed'
    resTemplate.responseTimestamp = new Date().toISOString()
    ws.send(JSON.stringify(resTemplate))
    ws.close()
    return
  }
  // console.log('tempSql_Query = ', tempSql_Query)

  db.one(tempSql_Query)
  .then( data => {
    // check if form type doesn't exist in DB
    if(!data.array_to_json) {
      log.error('No data for type :'+ tempFormType)
      resTemplate.success = false
      resTemplate.responseData = 'No data for type :'+ tempFormType
      resTemplate.responseTimestamp = new Date().toISOString()
      ws.send(JSON.stringify(resTemplate))
      // ws.close()
      // return
    } else {
      resTemplate.success = true
      resTemplate.responseTimestamp = new Date().toISOString()
      resTemplate.responseData = data
      // console.log('sent response at ', resTemplate.responseTimestamp)
      log.info('sent response at ', resTemplate.responseTimestamp)
      ws.send(JSON.stringify(resTemplate))
    }
    // send any delta changes/newly added data to all clients listening to it
    // get all clients listening to ws
    if(tempSql_QueryById !== '')
    dbSubscriber.notifications.on(channel, (payload) => {
      // Payload as passed to dbSubscriber.notify() (see below)

      if(ws.readyState === ws.OPEN)
      if(payload.formtypecode)
        if(payload.formtypecode === tempFormType)
          if(payload.formsanswersid) {
            log.info("Received notification in 'datalake-data-change':", payload)
            // db.one(sql_query.byId(payload.formsanswersid, dbSchema))
            db.oneOrNone(tempSql_QueryById, [payload.formsanswersid])
              .then( data => {
                if(data && data.array_to_json){
                  resTemplate.success = true
                  resTemplate.responseTimestamp = new Date().toISOString()
                  resTemplate.responseData = data
                  ws.send(JSON.stringify(resTemplate))
                  log.info("New data send ", payload)
                }
              })
              .catch( error => {
                // console.log('ERROR:', error)
                // console.log('ERROR while executing DB-query to fetch data :', error)
                log.error('ERROR while executing DB-query to fetch data :'+ error.message)
                resTemplate.success = false
                resTemplate.responseData = 'ERROR while executing DB-query to fetch data :'+ error.message
                resTemplate.responseTimestamp = new Date().toISOString()
                ws.send(JSON.stringify(resTemplate))
                // ws.close()
              })
          }
    })

  })
  .catch( error => {
    // console.log('ERROR while executing DB-query to fetch data :', error)
    log.error('ERROR while executing DB-query to fetch data :'+ error.message)
    resTemplate.success = false
    resTemplate.responseData = 'ERROR while executing DB-query to fetch data :'+ error.message
    resTemplate.responseTimestamp = new Date().toISOString()
    ws.send(JSON.stringify(resTemplate))
    ws.close()
  })

}

function getDataByBbox(ws, req) {
  log.info('>>> hot >>> getDataByBbox')
  console.log(req)
  // sample url params
  // ws://localhost:9090/hot/databybbox?type=PLUVIOMETERS_OFFICIAL&bbox=-67.98451956245826,-10.09049971309554,-67.69796501946632,-9.900096285440455

  let tempFormType = req.query.type
  let limit = req.query.limit
  let bbox = req.query.bbox

  // check what type of data requested based on params received
  let tempSql_Query = ''
  // only bbox
  if(bbox)
    tempSql_Query = sql_query.dataByBbox(tempFormType, bbox, dbSchema, userSchema)

  db.one(tempSql_Query)
  .then( data => {
    // check if form type doesn't exist in DB
    if(!data.array_to_json) {
      log.error('No data for type :'+ tempFormType)
      resTemplate.success = false
      resTemplate.responseData = 'No data for type :'+ tempFormType
      resTemplate.responseTimestamp = new Date().toISOString()
      ws.send(JSON.stringify(resTemplate))
      // ws.close()
      // return
    } else {
      resTemplate.success = true
      resTemplate.responseTimestamp = new Date().toISOString()
      resTemplate.responseData = data
      // console.log('sent response at ', resTemplate.responseTimestamp)
      log.info('sent response at ', resTemplate.responseTimestamp)
      ws.send(JSON.stringify(resTemplate))
    }
    // send any delta changes/newly added data to all clients listening to it
    // get all clients listening to ws
    dbSubscriber.notifications.on(channel, (payload) => {
      // Payload as passed to dbSubscriber.notify() (see below)

      if(ws.readyState === ws.OPEN)
      if(payload.formtypecode)
        if(payload.formtypecode === tempFormType)
          if(payload.formsanswersid) {
            log.info("Received notification in 'datalake-data-change':", payload)

            db.oneOrNone(sql_query.byIdWithBbox(payload.formsanswersid, bbox, dbSchema, userSchema))
              .then( data => {
                if(data && data.array_to_json){
                  resTemplate.success = true
                  resTemplate.responseTimestamp = new Date().toISOString()
                  resTemplate.responseData = data
                  ws.send(JSON.stringify(resTemplate))
                  // broadcastToAllClients(ws, resTemplate)
                  log.info("New data send ", payload)
                }
              })
              .catch( error => {
                // console.log('ERROR:', error)
                // console.log('ERROR while executing DB-query to fetch data :', error)
                log.error('ERROR while executing DB-query to fetch data :'+ error.message)
                resTemplate.success = false
                resTemplate.responseData = 'ERROR while executing DB-query to fetch data :'+ error.message
                resTemplate.responseTimestamp = new Date().toISOString()
                ws.send(JSON.stringify(resTemplate))
                // ws.close()
              })
          }
    })
  })
  .catch( error => {
    // console.log('ERROR while executing DB-query to fetch data :', error)
    log.error('ERROR while executing DB-query to fetch data :'+ error.message)
    resTemplate.success = false
    resTemplate.responseData = 'ERROR while executing DB-query to fetch data :'+ error.message
    resTemplate.responseTimestamp = new Date().toISOString()
    ws.send(JSON.stringify(resTemplate))
    ws.close()
  })



}

function getHttpDataByBbox(req, res) {
  log.info('>>> hot >>> getDataByBbox')
  console.log(req.query)
  // sample url params
  // ws://localhost:9090/hot/databybbox?type=PLUVIOMETERS_OFFICIAL&bbox=-67.98451956245826,-10.09049971309554,-67.69796501946632,-9.900096285440455

  let tempFormType = req.query.type
  let limit = req.query.limit
  let bbox = req.query.bbox

  // check what type of data requested based on params received
  let tempSql_Query = ''
  // only bbox
  if(bbox)
    tempSql_Query = sql_query.dataByBbox(tempFormType, bbox, dbSchema, userSchema)

  db.one(tempSql_Query)
  .then( data => {
    // check if form type doesn't exist in DB
    if(!data.array_to_json) {
      log.error('No data for type :'+ tempFormType)
      resTemplate.success = false
      resTemplate.responseData = 'No data for type :'+ tempFormType
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

/**
 * Gets only Forms data. Can have Spatial filter params
 *
 * @param {*} ws
 * @param {*} req
 */
function getFormData(ws, req) {
  log.info('>>> hot >>> getFormData')
  // sample url params
  // ws://localhost:9090/hot/formsanswers?type=RAIN_FORM&lat=-23.623&lon=-46.5637&buffer=50000&limit=30
  // wss://waterproofing.geog.uni-heidelberg.de/wss/hot/formsanswers?type=RAIN_FORM&lat=-23.623&lon=-46.5637&buffer=50000&time=2021-09-13/2021-09-17
  // ws://localhost:9090/hot/formsanswers?type=PLUVIOMETERS_OFFICIAL&bbox=-67.98451956245826,-10.09049971309554,-67.69796501946632,-9.900096285440455

  let tempFormType = req.query.type
  let lat = req.query.lat
  let lon = req.query.lon
  let buffer = req.query.buffer
  let limit = req.query.limit
  let bbox = req.query.bbox
  // ISO 8601 time interval string eg: 2015-01-17T09:50:04/2015-04-17T08:29:55
  let timeRange = req.query.time
  let timeStart = null
  let timeEnd = null
  let fiaAttribute = null
  let user = req.query.user

  if(timeRange) {
    timeStart = timeRange.substring(0, timeRange.indexOf('/'))
    timeEnd = timeRange.substring(timeRange.indexOf('/') + 1, timeRange.length)
  }

  // check what type of data requested based on params received
  // console.log(tempFormType, lat, lon, buffer, timeStart, timeEnd, limit, dbSchema)
  let tempSql_Query = ''
  let tempSql_QueryById = ''

  // check if the tempFormType is citizen generated data. TIP: _FORM ending
  if(tempFormType.indexOf('_FORM') !== -1) {
    fiaAttribute = 'situation'
  }
  // spatial with point and buffer
  if((lat && lon && buffer) && (tempFormType) && (!bbox) && (!timeStart && !timeEnd)) {
    tempSql_Query = sql_query.formsAnswersData(tempFormType, lat, lon, buffer, null, null, limit, fiaAttribute, user, dbSchema, userSchema)
    // no limit required as its query by ID which should return just 1 row
    tempSql_QueryById = sql_query.notifybyId(lat, lon, buffer, null, null, fiaAttribute, user, dbSchema, userSchema)
  }
  // only bbox
  else if(bbox && tempFormType && (!timeStart && !timeEnd)){
    tempSql_Query = sql_query.formAnswersByBbox(tempFormType, bbox, dbSchema, userSchema)
    // tempSql_QueryById = sql_query.notifybyId(lat, lon, buffer, dbSchema)
    tempSql_QueryById = sql_query.byIdWithBbox(tempFormType, bbox, dbSchema, userSchema)
  }
  // spatio-temporal with point and buffer
  else if((lat && lon && buffer) && (timeStart && timeEnd) && (tempFormType) && (!bbox)){
    tempSql_Query = sql_query.formsAnswersData(tempFormType, lat, lon, buffer, timeStart, timeEnd, limit, fiaAttribute, user, dbSchema, userSchema)
    tempSql_QueryById = sql_query.notifybyId(lat, lon, buffer, timeStart, timeEnd, fiaAttribute, user, dbSchema, userSchema)
  }
  // only form name
  else if((!lat && !lon && !buffer) && (tempFormType) && (!bbox) && (!timeStart && !timeEnd)){
    tempSql_Query = sql_query.formsAnswersData(tempFormType, lat, lon, buffer, null, null, limit, fiaAttribute, user, dbSchema, userSchema)
    tempSql_QueryById = sql_query.notifybyId(lat, lon, buffer, null, null, fiaAttribute, user, dbSchema, userSchema)
  }
  else {
    log.error('Invalid Parameter passed')
    resTemplate.success = false
    resTemplate.responseData = 'Invalid Parameter passed'
    resTemplate.responseTimestamp = new Date().toISOString()
    ws.send(JSON.stringify(resTemplate))
    ws.close()
    return
  }
  // console.log('tempSql_Query = ', tempSql_Query)
  // console.log('tempSql_QueryById = ', tempSql_QueryById)

  if(tempSql_Query)
  db.one(tempSql_Query)
  .then( data => {
    // check if form type doesn't exist in DB
    if(!data.array_to_json) {
      log.error('No data for type :'+ tempFormType)
      resTemplate.success = false
      resTemplate.responseData = 'No data for type :'+ tempFormType
      resTemplate.responseTimestamp = new Date().toISOString()
      ws.send(JSON.stringify(resTemplate))
      // ws.close()
      // return
    } else {
      resTemplate.success = true
      resTemplate.responseTimestamp = new Date().toISOString()
      resTemplate.responseData = data
      // console.log('sent response at ', resTemplate.responseTimestamp)
      log.info('sent response at ', resTemplate.responseTimestamp)
      ws.send(JSON.stringify(resTemplate))
    }
    // send any delta changes/newly added data to all clients listening to it
    // get all clients listening to ws
    if(tempSql_QueryById !== '')
    dbSubscriber.notifications.on(channel, (payload) => {
      // Payload as passed to dbSubscriber.notify() (see below)

      if(ws.readyState === ws.OPEN)
      if(payload.formtypecode)
        if(payload.formtypecode === tempFormType)
          if(payload.formsanswersid) {
            log.info("Received notification in 'datalake-data-change':", payload)
            // db.one(sql_query.byId(payload.formsanswersid, dbSchema))
            db.oneOrNone(tempSql_QueryById, [payload.formsanswersid])
              .then( data => {
                if(data && data.array_to_json){
                  resTemplate.success = true
                  resTemplate.responseTimestamp = new Date().toISOString()
                  resTemplate.responseData = data
                  ws.send(JSON.stringify(resTemplate))
                  log.info("New data send ", payload)
                }
              })
              .catch( error => {
                // console.log('ERROR:', error)
                // console.log('ERROR while executing DB-query to fetch data :', error)
                log.error('ERROR while executing DB-query to fetch data :'+ error.message)
                resTemplate.success = false
                resTemplate.responseData = 'ERROR while executing DB-query to fetch data :'+ error.message
                resTemplate.responseTimestamp = new Date().toISOString()
                ws.send(JSON.stringify(resTemplate))
                // ws.close()
              })
          }
    })

  })
  .catch( error => {
    // console.log('ERROR while executing DB-query to fetch data :', error)
    log.error('ERROR while executing DB-query to fetch data :'+ error.message)
    resTemplate.success = false
    resTemplate.responseData = 'ERROR while executing DB-query to fetch data :'+ error.message
    resTemplate.responseTimestamp = new Date().toISOString()
    ws.send(JSON.stringify(resTemplate))
    ws.close()
  })

}

function requestHandler(client, request) {
  // console.log('>>> requestHandler ', request.query)

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

  if(request.url.indexOf('databybbox') !== -1 && request.url.indexOf('formsanswers') === -1)
    getDataByBbox(client, request)
  else if(request.url.indexOf('databybbox') === -1 && request.url.indexOf('formsanswers') === -1)
    getData(client, request)
  else if(request.url.indexOf('formsanswers') !== -1 && request.url.indexOf('databybbox') === -1)
    getFormData(client, request)

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


// Old Endpoints
// router.ws('/echo', echo)
// router.ws('/broadcast', broadcast)
// router.ws('/data', requestHandler)
//
// router.ws('/databybbox', requestHandler)
// router.get('/databybbox', getHttpDataByBbox)
//
// router.ws('/formsanswers', requestHandler)
// router.get('/formsanswers', getHttpFormData)
// // router.post('/capability', getCapability)
// router.get('/capability', getCapability)
// router.get('/fieldsanswers', getFieldAnswersData)

module.exports = router
