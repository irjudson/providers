function MemoryCacheProvider(config, log) {
    this.cache = {};

    this.config = config;
    this.log = log;
}

MemoryCacheProvider.buildCompositeKey = function(namespace, key) {
    return namespace + '_' + key;
}

MemoryCacheProvider.prototype.del = function(namespace, key, callback) {
    delete this.cache[MemoryCacheProvider.buildCompositeKey(namespace, key)];
    if (callback) return callback();
};

MemoryCacheProvider.prototype.get = function(namespace, key, callback) {
    var compositeKey = MemoryCacheProvider.buildCompositeKey(namespace, key);
    if (!this.cache[compositeKey]) return callback();
    if (this.cache[compositeKey].expiration < new Date()) {
        return this.del(namespace, key, callback);
    }

    return callback(null, this.cache[compositeKey].value);
};

MemoryCacheProvider.prototype.set = function(namespace, key, value, expiration, callback) {
    this.cache[MemoryCacheProvider.buildCompositeKey(namespace, key)] = { value: value, expires: expiration };

    if (callback) return callback();
};

module.exports = MemoryCacheProvider;
