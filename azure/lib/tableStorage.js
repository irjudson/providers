var azure = require('azure')
  , moment = require('moment')
  , uuid = require('node-uuid')
  , core = require('nitrogen-core');

function TableStorageProvider(config, log, callback) {
    this.flatten = false;
    this.log = log;
    if ('flatten_messages' in config) {
        this.flatten_messages = config.flatten_messages;
    }

    this.ascending_table_name = config.azure_table_name || "messages";
    this.descending_table_name = config.azure_table_name + "_descending" || "messages_descending";

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

        this.azureTableService.createTableIfNotExists(this.descending_table_name, callback);
    });
}

TableStorageProvider.prototype.archive = function(message, optionsOrCallback, callback) {
    var options = {};
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

    var ascendingBatch = new azure.TableBatch();
    var descendingBatch = new azure.TableBatch();

    var correspondanceId = uuid.v4();
    messageObject.visible_to.forEach(function(visibleToId) {
        messageObject.PartitionKey = visibleToId;
        messageObject.visible_to = JSON.stringify([ visibleToId ]);

        messageObject.RowKey = moment(message.ts).utc().format() + "-" + correspondanceId;
        ascendingBatch.insertEntity(messageObject, { echoContent: false });

        var invertedRowKey = 8640000000000000 - new Date(message.ts).getTime()
        messageObject.RowKey = invertedRowKey + "-" + correspondanceId;
        descendingBatch.insertEntity(messageObject, { echoContent: false });
    });

    this.azureTableService.executeBatch(this.ascending_table_name, batch, function(err) {
        if (err) return callback(err);

        this.azureTableService.executeBatch(this.descending_table_name, batch, callback);
    });
};

TableStorageProvider.prototype.remove = function(principal, filter, callback) {
    // Not Supported

    return callback();
};

TableStorageProvider.prototype.find = function(principal, filter, options, callback) {
    var query = new azure.TableQuery()
        .top(options.limit || TableStorageProvider.DEFAULT_MAX_ROWS)
        .where('PartitionKey eq ?', principal.id);
};

module.exports = TableStorageProvider;
