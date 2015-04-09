var async = require('async')
  , azure = require('azure')
  , crypto = require('crypto')
  , moment = require('moment')
  , core = require('nitrogen-core');

function TableStorageProvider(config, log, callback) {
    var self = this;

    this.flatten = false;
    this.log = log;
    if ('flatten_messages' in config) {
        this.flatten_messages = config.flatten_messages;
    }

    this.ascending_table_name = config.azure_table_name || "messages";
    this.descending_table_name = this.ascending_table_name + "Descending";

    var azure_storage_account = config.azure_storage_account || process.env.AZURE_STORAGE_ACCOUNT;
    var azure_storage_key = config.azure_storage_key || process.env.AZURE_STORAGE_KEY;

    if (!azure_storage_account || !azure_storage_key) {
        this.log.warn("WARNING: Azure storage account or key not configured.  Set AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY as environment variables to configure the azure blob provider.");
        return;
    }

    var retryOperations = new azure.ExponentialRetryPolicyFilter();

    this.azureTableService = azure.createTableService(
        azure_storage_account,
        azure_storage_key
    ).withFilter(retryOperations);

    callback = callback || function(err) {
        if (err) log.error("Error creating Azure messages table: " + err);
    }

    this.azureTableService.createTableIfNotExists(this.ascending_table_name, function(err, created, response) {
        if (err) return callback(err);

        self.azureTableService.createTableIfNotExists(self.descending_table_name, callback);
    });
}

TableStorageProvider.ASCENDING_SORT = 1;
TableStorageProvider.DESCENDING_SORT = -1;

TableStorageProvider.MAX_DATE_TIMESTAMP = 8640000000000000;
TableStorageProvider.ID_KEY_LENGTH = TableStorageProvider.MAX_DATE_TIMESTAMP.toString().length;

TableStorageProvider.DEFAULT_MAX_ROWS = 200;

TableStorageProvider.prototype.archive = function(message, optionsOrCallback, callback) {
    var options = {};
    var self = this;

    if (typeof(optionsOrCallback) == 'function' && !callback) {
        callback = optionsOrCallback;
    } else if (optionsOrCallback) {
        options = optionsOrCallback;
    }

    var messageObject = message.toObject();

    if (options.flatten || this.flatten_messages) {
        var flatBody = core.services.messages.flatten(messageObject.body);
        for (var key in flatBody) {
            messageObject[key] = flatBody[key];
        }
    }

    messageObject.body = JSON.stringify(messageObject.body);
    messageObject.tags = JSON.stringify(messageObject.tags);
    messageObject.response_to = JSON.stringify(messageObject.response_to);

    var messageHash = TableStorageProvider.hashMessage(message).toString();

    async.each(messageObject.visible_to, function(visibleToId, visibleToCallback) {

        var clonedMessageObject = JSON.parse(JSON.stringify(messageObject));

        clonedMessageObject.PartitionKey = visibleToId.toString();
        clonedMessageObject.visible_to = [ visibleToId ];
        clonedMessageObject.RowKey = TableStorageProvider.formatTimeStamp(message.ts.getTime()) + "-" + messageHash;

        self.azureTableService.insertOrReplaceEntity(self.ascending_table_name, clonedMessageObject, { echoContent: false }, function(err) {
            if (err) return visibleToCallback(err);

            var invertedTimestamp = TableStorageProvider.MAX_DATE_TIMESTAMP - new Date(message.ts).getTime();
            clonedMessageObject.RowKey = TableStorageProvider.formatTimeStamp(invertedTimestamp) + "-" + messageHash;

            self.azureTableService.insertEntity(self.descending_table_name, clonedMessageObject, { echoContent: false }, visibleToCallback);
        });
    }, function(err) {
        if (err) self.log.error('Azure TableStorageProvider: insertOrReplaceEntity failed: ' + err);

        return callback(err);
    });
};

TableStorageProvider.formatTimeStamp = function(ts) {
    var tsString = ts.toString();
    while (tsString.length < TableStorageProvider.ID_KEY_LENGTH) {
        tsString = "0" + tsString;
    }

    return tsString;
};

TableStorageProvider.hashMessage = function(message) {
    var hashMessageObject = JSON.parse(JSON.stringify(message));;
    delete hashMessageObject.id;

    var messageHashBuf = new Buffer(JSON.stringify(hashMessageObject), 'base64');

    var md5 = crypto.createHash('md5');
    md5.update(messageHashBuf.toString('binary'), 'binary');

    var messageHash = md5.digest('hex');
    return messageHash;
};

TableStorageProvider.prototype.remove = function(principal, filter, callback) {
    // We do not delete from table storage.
    return callback();
};

TableStorageProvider.prototype.find = function(principal, filter, options, callback) {

    var table = this.descending_table_name;
    if (options.sort) {
        if (options.sort.ts) {
            if (options.sort.ts !== TableStorageProvider.DESCENDING_SORT && options.sort.ts !== TableStorageProvider.ASCENDING_SORT)
                return callback (new Error("invalid sort option: " + JSON.stringify(options.sort)));

            if (options.sort.ts === TableStorageProvider.ASCENDING_SORT)
                table = this.ascending_table_name;
        }
    }

    var limit = options.limit || TableStorageProvider.DEFAULT_MAX_ROWS;
    var partitionKey = principal.id.toString();

    var query = new azure.TableQuery()
        .from(table)
        .top(limit)
        .where('PartitionKey eq ?', partitionKey);

    // TODO: super basic two mode query mechanism (OR or AND) - replace with real query builder
    var mode = "and";
    if (filter.$or) {
        mode = "or";
        filter = filter.$or;
    }

    for (var key in filter) {
        if (mode === "and")
            query = query.and(key + " eq ?", filter[key]);
        else
            query = query.or(key + " eq ?", filter[key]);
    }

    this.azureTableService.queryEntities(query, function(err, messages) {
        if (err) return callback(err);

        var hydratedMessages = messages.map(function(message) {
            delete message._;
            delete message.RowKey;
            delete message.PartitionKey;
            delete message.Timestamp;

            message.body = JSON.parse(message.body);

            return new core.models.Message(message);
        });

        return callback(null, hydratedMessages);
    });
};

module.exports = TableStorageProvider;
