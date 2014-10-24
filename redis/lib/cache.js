var redis = require('redis');

function RedisCacheProvider(config, log) {
    this.client = redis.createClient(config.redis_server.port, config.redis_server.host);
    this.log = log;
}

RedisCacheProvider.buildCompositeKey = function(namespace, key) {
    return namespace + '_' + key;
}

RedisCacheProvider.prototype.del = function(namespace, key, callback) {
    this.client.del(RedisCacheProvider.buildCompositeKey(namespace, key), callback);
};

RedisCacheProvider.prototype.get = function(namespace, key, callback) {
    var compositeKey = RedisCacheProvider.buildCompositeKey(namespace, key);
    var self = this;

    this.client.get(compositeKey, function(err, entryJson) {
        if (err) {
            self.log.error('error fetching cache entry: ' + compositeKey + ' :' + err);
            return callback(err);
        }

        if (!entryJson) {
            self.log.debug('cache entry not found: ' + compositeKey);
            return callback();
        }

        var entry = JSON.parse(entryJson);

        if (entry.expiration < new Date()) {
            self.log.debug('cache entry expired: ' + compositeKey);
            return this.del(namespace, key, callback);
        }

        return callback(null, entry.value);
    });
};

RedisCacheProvider.prototype.set = function(namespace, key, value, expiration, callback) {
    var entry = {
        expiration: expiration,
        value: value
    };

    this.client.set(RedisCacheProvider.buildCompositeKey(namespace, key), JSON.stringify(entry), callback);
};

module.exports = RedisCacheProvider;