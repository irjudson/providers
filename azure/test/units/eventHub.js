var assert = require('assert')
  , moment = require('moment')
  , log = require('winston')
  , uuid = require('node-uuid')
  , core = require('nitrogen-core')  
  , EventHubProvider = require('../../lib/eventhub');

log.remove(log.transports.Console);
log.add(log.transports.Console, { colorize: true, timestamp: true, level: 'info' });

var config = {
    "servicebus": process.env.AZURE_SERVICE_BUS,
    "sas_key_name": process.env.AZURE_SAS_KEY_NAME,
    "sas_key":         process.env.AZURE_SAS_KEY,
    "azure_eventhub_name":   process.env.AZURE_EVENTHUB_NAME
};

var consumerGroup = '$Default';

function validConfig(config) {
    return (config.servicebus && config.sas_key_name && config.sas_key && config.azure_eventhub_name);
}

var msg = {
    ts: "Mon Feb 02 2015 14:59:48 GMT-0800 (PST)",
    body: {
        longitude: "-122.3331", latitude: "48.2332"
    },
    from: "54cffafea09ef731a1c09682",
    type: 'location',
    index_until: "Mon Feb 09 2015 14:59:48 GMT-0800 (PST)",
    expires: "Thu Dec 31 2499 16:00:00 GMT-0800 (PST)",
    tags: [ 'involves:54cffafea09ef731a1c09682' ],
    response_to: [],
    ver: 0.2,
    updated_at: '2015-02-02T22:59:48.387Z',
    created_at: '2015-02-02T22:59:48.387Z',
    id: '54d00164509ef69fa13cb99d'
};

describe('The eventhub', function() {

  beforeEach(function () {
    msgId = uuid.v4();
    message = new core.models.Message(msg);
    message.ts = moment().valueOf();
    message.from = msgId;
    message.id = msgId;
  });

// Next version of mocha
//    before(function () {
//        if (!validConfig(config)) {
//            this.skip();
//        }
//    });
        
  it('should be able to send a message.', function (done) {
        // This allows the test to run asynchronously so the message can get all 
        // the way through eventhub and get caught by the reciever (which calls done());
        this.timeout(10000);
        setTimeout(done, 9000);
        
        assert(!validConfig(config));


        var ehp = new EventHubProvider(config, log);
        
        ehp.eventHub.getEventProcessor(consumerGroup, function (conn_err, processor) {
            assert.ifError(conn_err);
            processor.init(function (rx_err, partition, payload) {
                                assert(!rx_err);
                                assert(payload.id, message.id);
                                processor.teardown(function () { done(); });
                        }, function (init_err) {
                                assert(!init_err);
                                processor.receive();
                        });
        });
        
        ehp.archive(message, function (err) {
            assert(!err);
        });
    });
});