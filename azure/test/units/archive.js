var assert = require('assert')
  , AzureArchiveProvider = require('../../lib/archive')
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

var config = {
    "azure_table_name" : "messages",
    "azure_storage_account": process.env.AZURE_STORAGE_ACCOUNT,
    "azure_storage_key":  process.env.AZURE_STORAGE_KEY
};

describe('archive', function() {

  it('should be able to create an archive (table) store.', function(done) {
    var ats = new AzureArchiveProvider(config, log);
    done();
  });
  
  it('should be able to store a message.', function(done) {
    var ats = new AzureArchiveProvider(config, log);
    var message = new core.models.Message(obj);
    ats.archive(message, function(err, data) {
      assert(err, null);
    });
    done();
  });

});