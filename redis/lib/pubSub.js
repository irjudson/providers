var async = require('async')
  , redis = require('redis')
  , sift = require('sift');

function RedisPubSubProvider(config, log) {
    if (!config.redis_servers) log.error('RedisPubSubProvider: no redis server configuration information found.');

    this.config = config;
    this.log = log;
    this.clients = {};
    this.SUPPORTS_PERMANENT_SUBSCRIPTIONS = true;
}

RedisPubSubProvider.SUBSCRIPTIONS_KEY = 'pubsub.subscriptions';
RedisPubSubProvider.RECEIVE_TIMEOUT_SECONDS = 5 * 60;

RedisPubSubProvider.redisifySubscription = function(subscription) {
    return JSON.stringify({
        id: subscription.id,
        type: subscription.type,
        filter: subscription.filter,
        principal: subscription.principal
    });
};

RedisPubSubProvider.subscriptionKey = function(subscription) {
    return subscription.id;
};

RedisPubSubProvider.prototype.clientForServer = function(serverId) {
    if (!this.clients[serverId] ) {
        this.log.warn('RedisPubSubProvider: creating redis client for serverId: ' + serverId);
        var client = this.createClient(serverId);

        this.clients[serverId] = client;
    }

    return this.clients[serverId];
};

RedisPubSubProvider.prototype.createClient = function(serverId) {
    var server = this.config.redis_servers[serverId];
    var client = redis.createClient(server.port, server.host);
    var self = this;

    client.on('error', function (err) {
        self.log.error('RedisPubSubProvider: client error: ' + err);
    });

    return client;
};

RedisPubSubProvider.prototype.createSubscription = function(subscription, callback) {
    // TODO: choose server based on subscription load not randomly.
    var serverIds = Object.keys(this.config.redis_servers);
    var serverAssignmentIdx = Math.floor(serverIds.length * Math.random());

    subscription.assignment = serverIds[serverAssignmentIdx];

    var client = this.clientForServer(subscription.assignment);

    client.sadd(RedisPubSubProvider.SUBSCRIPTIONS_KEY, RedisPubSubProvider.redisifySubscription(subscription), callback);
};

// TODO: Use straw.js to queue the item with the subscription system?

RedisPubSubProvider.prototype.publish = function(type, item, callback) {
    this.log.debug("RedisPubSubProvider: publishing " + type + ": " + item.id + ": " + JSON.stringify(item));
    var self = this;

    // iterate over each redis server

    async.each(Object.keys(this.config.redis_servers), function(serverId, serverCallback) {

        var client = self.clientForServer(serverId);

        // find all of the subscriptions for this server
        self.subscriptionsForServer(serverId, function(err, subscriptions) {
            if (err) return serverCallback(err);

            // for each subscription, see if the filter matches this item
            self.log.debug("RedisPubSubProvider: CHECKING " + subscriptions.length + " subscriptions.");
            async.each(subscriptions, function(subscriptionJson, subscriptionCallback) {
                var subscription = JSON.parse(subscriptionJson);

                self.log.debug("RedisPubSubProvider: CHECKING subscription: name: " + subscription.name + " type: " + subscription.type + " filter: " + JSON.stringify(subscription.filter));

                if (subscription.type !== type) return subscriptionCallback();
                if (item.visible_to.indexOf(subscription.principal) === -1) return subscriptionCallback();

                var unfilteredItems = sift(subscription.filter, [item]);

                if (unfilteredItems.length === 0) return subscriptionCallback();

                self.log.debug("RedisPubSubProvider: MATCHED subscription: name: " + subscription.name + " type: " + subscription.type + " filter: " + JSON.stringify(subscription.filter));
                client.rpush(RedisPubSubProvider.subscriptionKey(subscription), JSON.stringify(unfilteredItems[0]), subscriptionCallback);

            }, serverCallback);
        });

    }, callback);
};

RedisPubSubProvider.prototype.receive = function(subscription, callback) {
    if (!subscription.assignment) return callback('subscription has no Redis server assignment.');

    var client = this.createClient(subscription.assignment);
    var self = this;

    var subscriptionKey = RedisPubSubProvider.subscriptionKey(subscription);
    this.log.debug('RedisPubSubProvider: RECEIVING on subscription key: ' + subscriptionKey + ' filter: ' + JSON.stringify(subscription.filter));

    client.blpop(subscriptionKey, RedisPubSubProvider.RECEIVE_TIMEOUT_SECONDS, function(err, reply) {
        client.quit();

        if (err) return callback(err);
        if (!reply) return callback(null, null);

        // redis returns an 2 element array with [key, value], so decode this
        var item = JSON.parse(reply[1]);

        self.log.debug("RedisPubSubProvider: RECEIVED on subscription: name: " + subscription.name + " type: " + subscription.type + " filter: " + JSON.stringify(subscription.filter) + " item: " + JSON.stringify(item));

        var ref = {
            subscription: subscription,
            item: item
        };

        return callback(null, item, ref);
    });
};

RedisPubSubProvider.prototype.ackReceive = function(ref, success, callback) {
    if (success) {
        if (callback) callback();
    } else if (ref) {
        var client = this.createClient(ref.subscription.assignment);
        var subscriptionKey = RedisPubSubProvider.subscriptionKey(ref.subscription);

        client.lpush(subscriptionKey, JSON.stringify(ref.item), callback);
    }
};

RedisPubSubProvider.prototype.removeSubscription = function(subscription, callback) {
    if (!subscription.assignment) return callback('Subscription not assigned to Redis server.');

    var subscriptionJson = RedisPubSubProvider.redisifySubscription(subscription);

    this.log.debug("RedisPubSubProvider: removing subscription: " + subscriptionJson);

    var client = this.clientForServer(subscription.assignment);

    // remove subscription from the set of subscriptions
    client.srem(RedisPubSubProvider.SUBSCRIPTIONS_KEY, subscriptionJson, function(err) {
        if (err) return callback(err);

        client.del(RedisPubSubProvider.subscriptionKey(subscription), callback);
    });
};

RedisPubSubProvider.prototype.subscriptionsForServer = function(serverId, callback) {
    var client = this.clientForServer(serverId);

    client.smembers(RedisPubSubProvider.SUBSCRIPTIONS_KEY, callback);
};

RedisPubSubProvider.prototype.staleSubscriptionCutoff = function() {
    return new Date(new Date().getTime() + -4 * 1000 * RedisPubSubProvider.RECEIVE_TIMEOUT_SECONDS);
};

//// TESTING ONLY METHODS BELOW THIS LINE

RedisPubSubProvider.prototype.displaySubscriptions = function(callback) {
    var self = this;

    async.eachSeries(Object.keys(this.config.redis_servers), function(serverId, serverCallback) {
        self.log.info("RedisPubSubProvider: SUBSCRIPTIONS FOR SERVER ID: " + serverId);

        var client = self.clientForServer(serverId);

        client.smembers(RedisPubSubProvider.SUBSCRIPTIONS_KEY, function(err, subscriptions) {
            if (err) return serverCallback(err);

            subscriptions.forEach(function(subscription) {
                self.log.info("RedisPubSubProvider: subscription: " + subscription);
            });

            return serverCallback();
        });

    }, callback);
};

RedisPubSubProvider.prototype.resetForTest = function(callback) {
    if (process.env.NODE_ENV === "production") return callback();

    this.log.info('RedisPubSubProvider: resetting Redis store completely for test');

    var client = this.clientForServer(Object.keys(this.config.redis_servers)[0]);

    client.flushdb(callback);
};

module.exports = RedisPubSubProvider;
