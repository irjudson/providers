var Sbus = require('node-sbus-amqp10');
var Q = require('q');

function EventhubSender(config, log) {
    var self = this;

    self.log = log !== undefined ? log : { log: function() {} };

    self.config = config;
    self.group = config.consumerGroup || '$Default';
    self.hub = Sbus.EventHubClient(
        config.serviceBusHost,
        config.eventHubName,
        config.SASKeyName,
        config.SASKey
    );

    var initDeferred = Q.defer();
    self.initPromise = initDeferred.promise;

    self.hub.getEventProcessor(self.group, function(err, processor) {
        if (err) {
            initDeferred.reject(err);
        } else {
            processor.set_storage(self.config.tableStoreName, self.config.tableStoreKey);
            processor.init(
                function () {
                }, // receiver function
                function (err) {
                    if (err) {
                        initDeferred.reject(err);
                    } else {
                        initDeferred.resolve(processor);
                    }
                });
        }
    });
}

// not using partitions yet
EventhubSender.prototype.sendAsync = function(message) {
    var self = this;

    var deferred = Q.defer();

    self.initPromise.then(
        function (processor) {
            processor.send(message, undefined, function (err) {
                if (err)
                    deferred.reject(err);
                else
                    deferred.resolve();
            })
        }
    ).fail(function(err) {
        deferred.reject(err);
    });

    return deferred.promise;
};

EventhubSender.prototype.send = function(message, callback) {
    var self = this;

    callback = callback || function () {};

    self.initPromise.then(
        function (processor) {
            processor.send(message, undefined, function (err) {
                callback(err);
            })
        }
    ).fail(function(err) {
        callback(err);
    });
};

module.exports = EventhubSender;