var EventHubSender = require('./eventHubSender');
var Q = require('q');

main();

function main() {
    var config = {
        "serviceBusHost": "",
        "SASKeyName":"",
        "SASKey":"",
        "eventHubName":"",
        "tableStoreName":"",
        "tableStoreKey":""
    };

    var obj1 = {
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


    var ehp = new EventHubSender(config);
    ehp.sendAsync(obj1)
        .then(function() {
            console.log('SendAsync ok. sent')
        })
        .then(function() {
            ehp.send(obj1, function (err) {
                if (!err)
                    console.log('Send ok. sent');
                else
                    console.log('Error', err);

                process.exit(0);
            });
        })
        .fail(function(err) {
            console.log('Error', err);
            process.exit(0);
        });
}