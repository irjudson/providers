var assert = require('assert')
  , EventHubMessageHub = require('../../lib/messageHub')
  , Q = require('q');

var config = {
    "serviceBusHost": process.env.SERVICE_BUS_HOST,
    "SASKeyName":     process.env.SAS_KEY_NAME,
    "SASKey":         process.env.SAS_KEY,
    "eventHubName":   process.env.EVENT_HUB_NAME
};

function validConfig(config) {
    return (config.serviceBusHost && config.SASKey && config.SASKeyName && config.eventHubName); 
}

describe('The eventHubMessageHub', function() {

// Next version of mocha
//    before(function () {
//        if (!validConfig(config)) {
//            this.skip();
//        }
//    });
        
    it('should be able to send a message.', function(done) {

        if (!validConfig(config)) {
            assert(false);
            done();
        }
        
        var obj = {
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

        var ehp = new EventHubMessageHub(config);
        ehp.sendAsync(obj).then(function() {
            assert(true);
        }).then(function() {
            ehp.send(obj, function (err) {
                if (!err) {
                    assert(true);
                } else {
                    assert(false);
                }
            });
        }).fail(function (err) {
            assert(false);
        });
        done();
    });
});