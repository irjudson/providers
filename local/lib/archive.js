function NullArchiveProvider(config, log) {
}

NullArchiveProvider.prototype.archive = function(message, callback) {
    if (callback) return callback();
};

NullArchiveProvider.prototype.initialize = function(callback) {
    if (callback) return callback();
};

NullArchiveProvider.prototype.remove = function(message, callback) {
    if (callback) return callback();
};

module.exports = NullArchiveProvider;