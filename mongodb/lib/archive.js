function MongoDBArchiveProvider(core) {
    this.core = core;
}

MongoDBArchiveProvider.prototype.archive = function(message, callback) {
    message.tags.push('involves:' + message.id);
    if (message.to) message.tags.push('involves:' + message.to);

    if (message.index_until.getTime() > new Date().getTime()) {
        message.save(callback);
    } else {
        return callback();
    }
};

MongoDBArchiveProvider.prototype.find = function(principal, filter, options, callback) {
    if (!this.core.models.Message.filterHasIndex(filter)) return callback(this.core.utils.badRequestError("filter: " + JSON.stringify(filter) + " does not have an index."));
    var translatedFilter = this.core.utils.translateQuery(filter, this.core.models.Message.fieldTranslationSpec);
    var filter = this.core.services.principals.filterForPrincipal(principal, translatedFilter);

    this.core.models.Message.find(filter, null, options, callback);
};

MongoDBArchiveProvider.prototype.remove = function(principal, filter, callback) {
    if (!principal || !principal.is('service')) return callback(this.core.utils.authorizationError());

    this.core.models.Message.remove(filter, callback);
};

module.exports = MongoDBArchiveProvider;
