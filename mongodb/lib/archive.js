var core = require('nitrogen-core');

function MongoDBArchiveProvider(config, log, callback) {
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
    if (!core.models.Message.filterHasIndex(filter)) return callback(core.utils.badRequestError("filter: " + JSON.stringify(filter) + " does not have an index."));
    var translatedFilter = core.utils.translateQuery(filter, core.models.Message.fieldTranslationSpec);
    var filter = core.services.principals.filterForPrincipal(principal, translatedFilter);

    core.models.Message.find(filter, null, options, callback);
};

MongoDBArchiveProvider.prototype.remove = function(principal, filter, callback) {
    if (!principal || !principal.is('service')) return callback(core.utils.authorizationError());

    core.models.Message.remove(filter, callback);
};

module.exports = MongoDBArchiveProvider;
