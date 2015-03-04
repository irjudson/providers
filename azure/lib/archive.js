var azure = require('azure')
  , moment = require('moment')
  , core = require('nitrogen-core');

function AzureArchiveProvider(config, log, callback) {
    this.flatten = false;
    this.log = log;
    if ('flatten_messages' in config) {
        this.flatten_messages = config.flatten_messages;
    }

    this.azure_table_name = config.azure_table_name || "messages";
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

    this.azureTableService.createTableIfNotExists(this.azure_table_name, callback ||
        function (err, created, response) {
            if (err) {
                log.error("Error creating Azure messages table: " + err);
            }
    });
}

AzureArchiveProvider.prototype.archive = function(message, optionsOrCallback, callback) {
    var options = {};
    if (typeof(optionsOrCallback) == 'function' && !callback) {
        callback = optionsOrCallback;
    } else if (optionsOrCallback) {
        options = optionsOrCallback;
    }

    var messageObject = message.toObject();

    messageObject.PartitionKey = messageObject.from;
    messageObject.RowKey = moment(message.ts).utc().format();

    if (options.flatten || this.flatten_messages) {
        var flatBody = core.services.messages.flatten(messageObject.body);
        for (var key in flatBody) {
          messageObject[key] = flatBody[key];
        }
    }

    messageObject.body = JSON.stringify(messageObject.body);
    messageObject.tags = JSON.stringify(messageObject.tags);
    messageObject.response_to = JSON.stringify(messageObject.response_to);
    messageObject.visible_to = JSON.stringify(messageObject.visible_to);

    this.azureTableService.insertEntity(this.azure_table_name, messageObject, callback);
};

module.exports = AzureArchiveProvider;
