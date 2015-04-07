var async = require('async')
  , azure = require('azure')
  , crypto = require('crypto')
  , moment = require('moment')
  , uuid = require('node-uuid')
  , core = require('nitrogen-core');

function TableStorageProvider(config, log, callback) {
    var self = this;

    this.flatten = false;
    this.log = log;
    if ('flatten_messages' in config) {
        this.flatten_messages = config.flatten_messages;
    }

    this.ascending_table_name = config.azure_table_name || "messages";
    this.descending_table_name = this.ascending_table_name + "Descending" || "messagesDescending";

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

TableStorageProvider.DEFAULT_MAX_ROWS = 1000;

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

    var messageHash = uuid.v4() // TableStorageProvider.hashMessage(message).toString();

    console.dir(messageObject.visible_to);

    async.each(messageObject.visible_to, function(visibleToId, visibleToCallback) {
        messageObject.PartitionKey = visibleToId.toString();
        messageObject.visible_to = JSON.stringify([ visibleToId ]);

        messageObject.RowKey = moment(message.ts).utc().format() + "-" + messageHash;
        console.log('ascending table entry ' + visibleToId);
        console.log('PartitionKey: ' + messageObject.PartitionKey);
        console.log('RowKey: ' + messageObject.RowKey);

        self.azureTableService.insertOrReplaceEntity(self.ascending_table_name, messageObject, function(err) {
            if (err) return visibleToCallback(err);

            var invertedRowKey = TableStorageProvider.MAX_DATE_TIMESTAMP - new Date(message.ts).getTime()
            messageObject.RowKey = invertedRowKey + "-" + messageHash;
            console.log('descending table entry ' + visibleToId + ' RowKey: ' + messageObject.RowKey);

            self.azureTableService.insertOrReplaceEntity(self.descending_table_name, messageObject, visibleToCallback);
        });
    }, callback);
};

TableStorageProvider.hashMessage = function(message) {
    var hashMessageObject = JSON.parse(JSON.stringify(message));;
    delete hashMessageObject.id;

    console.log('hashing message: ' + JSON.stringify(hashMessageObject));

    var messageHashBuf = new Buffer(JSON.stringify(hashMessageObject), 'base64');

    var sha256 = crypto.createHash('sha1');
    sha256.update(messageHashBuf.toString('binary'), 'binary');

    var messageHash = sha256.digest('base64');

    console.log('hash: ' + messageHash);
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
            if (options.sort.ts !== TableStorageProvider.DESCENDING_SORT || options.sort.ts !== TableStorageProvider.ASCENDING_SORT)
                return callback (new Error("invalid sort option: " + JSON.stringify(options.sort)));

            if (options.sort.ts === TableStorageProvider.ASCENDING_SORT)
                table = this.ascending_table_name;
        }
    }

    var query = new azure.TableQuery()
        .from(table)
        .top(options.limit || TableStorageProvider.DEFAULT_MAX_ROWS)
        .where('PartitionKey eq ?', principal.id);

    this.azureTableService.queryEntities(query, callback);
};

module.exports = TableStorageProvider;
