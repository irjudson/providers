var assert = require('assert')
  , AzureArchiveProvider = require('../../lib/archive')
  , moment = require('moment')
  , log = require('winston')
  , core = require('nitrogen-core');

log.remove(log.transports.Console);
log.add(log.transports.Console, { colorize: true, timestamp: true, level: 'info' });

var obj = {
  ts: "Mon Feb 02 2015 14:59:48 GMT-0800 (PST)",
  body: {
    longitude: "-122.3331", 
    latitude: "48.2332"
   },
  from: "54cffafea09ef731a1c09682",
  to: "54cffafea09ef731a1c09682",
  type: 'location',
  index_until: "Mon Feb 09 2015 14:59:48 GMT-0800 (PST)",
  expires: "Thu Dec 31 2499 16:00:00 GMT-0800 (PST)",
  tags: ['involves:54cffafea09ef731a1c09682'],
  response_to: [],
  ver: 0.2,
  updated_at: '2015-02-02T22:59:48.387Z',
  created_at: '2015-02-02T22:59:48.387Z',
  id: '54d00164509ef69fa13cb99d'
};

var obj2 = {
  ts: "Mon Feb 14 2015 14:59:48 GMT-0800 (PST)",
  body: {
    gps : {
      longitude: "-122.3331", 
      latitude: "48.2332"  
    },
    id: "54cffafea09ef731a1c09699",
    sensor_tree: {
      steering: {
        wheel: "0",
        column: "1"
      },
      brakes: {
        pedal: "0",
        lines: "1",
        rotors: "2",
        pads: "3"
      }
    }
   },
  from: "54cffafea09ef731a1c09682",
  to: "54cffafea09ef731a1c09682",
  type: 'location',
  index_until: "Mon Feb 09 2015 14:59:48 GMT-0800 (PST)",
  expires: "Thu Dec 31 2499 16:00:00 GMT-0800 (PST)",
  tags: ['involves:54cffafea09ef731a1c09682'],
  response_to: [],
  ver: 0.2,
  updated_at: '2015-02-02T22:59:48.387Z',
  created_at: '2015-02-02T22:59:48.387Z',
  id: '54d00164509ef69fa13cb99d'
};

var config = {
    "azure_table_name" : "testtable",
    "azure_storage_account": process.env.AZURE_STORAGE_ACCOUNT,
    "azure_storage_key":  process.env.AZURE_STORAGE_KEY
};

var tableService = null;
describe('archive', function() {

  before(function(done) {
    tableService = new AzureArchiveProvider(config, log, function (createError, created, response) {
      if (createError) {
          log.error(createError);
          assert(false);
          done();
      } else {
        if (created) {
          // New
          assert(true);
          done();
        } else {
          // Existed
          assert(true);
          done();
        }
      }
    });    
  });
  
  after(function(done) { 
//    tableService.azureTableService.deleteTable(config.azure_table_name, function(error, successful, response) { 
//      if (error) {
//        log.error(error);
//        assert(false);
//        done();
//      } else if (successful) {
//        assert(true);
//        done();
//      }
//      assert(false);
//      done();
//    })
  done();
  });
  
  it('should be able to create an archive (table) store.', function(done) {
    tableService.azureTableService.queryTables(function(error, queryTablesResult, resultContinuation, response) {
      if(!error) {
        for (var result in queryTablesResult) {
          if(JSON.stringify(queryTablesResult[result].TableName) == config.azure_table_name) {
            assert(true);
            done();
          }
        }
      } else {
        log.error(error);
        assert(false);
      }
      done();          
    });
  });
  
  it('should be able to store a message.', function(done) {
    var message = new core.models.Message(obj);
    message.ts = moment().format();
    this.timeout(5000);
    setTimeout(done, 4000);
    tableService.archive(message, function(error, entity) {
      if (error) {
        assert(false);
      }
      done();
    });
  });

  it('should be able to store a flattened message.', function(done) {
    var message = new core.models.Message(obj2);
    message.ts = moment().format();
    this.timeout(5000);
    setTimeout(done, 4000);
    tableService.archive(message, {flatten: true}, function(error, entity) {
      if (error) {
        assert(false);
      }
      done();
    });
  });

});