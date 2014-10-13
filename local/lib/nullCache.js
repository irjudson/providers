function NullCacheProvider(config, log) {
    this.config = config;
    this.log = log;
}

NullCacheProvider.prototype.del = function(namespace, key, callback) {
    if (callback) return callback();
};

NullCacheProvider.prototype.get = function(namespace, key, callback) {
    return callback();
};

NullCacheProvider.prototype.set = function(namespace, key, value, expiration, callback) {
    if (callback) return callback();
};

module.exports = NullCacheProvider;
