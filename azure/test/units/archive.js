var assert = require('assert')
  , AzureArchiveProvider = require('../../lib/archive')
  , moment = require('moment')
  , log = require('winston')
  , core = require('nitrogen-core')
  , uuid = require('node-uuid');

log.remove(log.transports.Console);
log.add(log.transports.Console, { colorize: true, timestamp: true, level: 'info' });

var msg = {
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
  from: "54d00164509ef69fa13cb99d",
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
var msgId = null;
var message = null;

describe('An archive', function() {
  before(function(done) {
    tableService = new AzureArchiveProvider(config, log, function (createError, created, response) {
      if (createError) {
          log.error(createError);
          assert(false);
      } else {
        if (created) {
          // New
          assert(true);
        } else {
          // Existed
          assert(true);
        }
      }
      done();
    });
  });
  
  beforeEach(function () {
    msgId = uuid.v4();
    message = new core.models.Message(msg);
    message.ts = moment().valueOf();
    message.from = msgId;
    message.id = msgId;
  });
  
  afterEach(function () {
    msgId = null;
    message = null;
  });
  
// This is asynchronous and takes *way* too long to leave in here for a single sub-suite.
//  after(function(done) { 
//    tableService.azureTableService.deleteTable(config.azure_table_name, function (error, successful, response) {
//      if (error) {
//        log.error(error);
//        assert(false);
//      } else if (successful) {
//        assert(true);
//      }
//      done();
//    });
//  });
  
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
  
  it('should be able to store a message.', function (done) {
    this.timeout(10000);
    setTimeout(done, 9000);
    tableService.archive(message, function(error, entity) {
      assert.ifError(error);
      tableService.azureTableService.queryEntity(config.azure_table_name, entity.PartitionKey, entity.RowKey, function (error, msg) {
        assert.ifError(error);
        assert(true);
        // delete the entity
        tableService.azureTableService.deleteEntity(config.azure_table_name, entity, function (error, successful, response) {
          assert.ifError(error);
          assert(successful, true);
        });          
      });
      done();
    });
  });

  it('should be able to store a flattened message.', function (done) {
    this.timeout(10000);
    setTimeout(done, 9000);
    tableService.archive(message, {flatten: true}, function(error, entity) {
      assert.ifError(error);
      tableService.azureTableService.queryEntity(config.azure_table_name, entity.PartitionKey, entity.RowKey, function (error, msg) {
        assert.ifError(error);
        if ("sensor_tree__steering__wheel" in msg) {
          assert(true);
          // delete the entity
          tableService.azureTableService.deleteEntity(config.azure_table_name, entity, function (error, successful, response) {
            assert.ifError(error);
            assert(successful, true);
          });
        }
      });
      done();
    });
  });

});

describe('A flat archive', function () {

// Done before suite 1; why do it again if we're not tearing down. Maybe we should clean up messages (TODO).
  before(function (done) {
    config.flatten_messages = true;
    tableService = new AzureArchiveProvider(config, log, function (error, created, response) {
      if (error) {
        assert(false);
      } else {
        if (created) {
          // New
          assert(true);
        } else {
          // Existed
          assert(true);
        }
      }
      done();
    });
  });

  beforeEach(function () {
    msgId = uuid.v4();
    message = new core.models.Message(msg);
    message.ts = moment().valueOf();
    message.from = msgId;
    message.id = msgId;
  });
  
  afterEach(function () {
    msgId = null;
    message = null;
  });
 
// This is asynchronous and takes *way* too long to leave in here for a single sub-suite. We should do it at the end.
//  after(function (done) {
//    tableService.azureTableService.deleteTable(config.azure_table_name, function (error, successful, response) {
//      if (!error && successful) {
//        assert(true);
//      } else {
//        assert(false);
//      }
//      done();
//    });
//  });


  it('should be able to store a message, that will be flattened automatically.', function (done) {
    this.timeout(10000);
    setTimeout(done, 9000);
    tableService.archive(message, function (error, entity) {
      assert.ifError(error);      
      tableService.azureTableService.queryEntity(config.azure_table_name, entity.PartitionKey, entity.RowKey, function (error, msg) {
        assert.ifError(error);
        if ("sensor_tree__steering__wheel" in msg) {
          assert(true);
        }
        // delete the entity
        tableService.azureTableService.deleteEntity(config.azure_table_name, entity, function (error, successful, response) {
          assert.ifError(error);
          assert(successful, true);
        });
      });
    done();
    });
  });
});
