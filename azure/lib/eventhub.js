var sbus = require('node-sbus-amqp10')
  , core = require('nitrogen-core');

function EventHubProvider(config, log, callback) {
    this.flatten = false;
    this.log = log;
    if ('flatten_messages' in config) {
        this.flatten_messages = config.flatten_messages;
    }
    
    var servicebus = config.servicebus || process.env.AZURE_SERVICE_BUS;
    var sas_key_name = config.sas_key_name || process.env.AZURE_SAS_KEY_NAME;
    var sas_key = config.sas_key || process.env.AZURE_SAS_KEY;
    var azure_eventhub_name = config.azure_eventhub_name || process.env.AZURE_EVENTHUB_NAME;

    if (!sas_key_name || !sas_key) {
        this.log.error("Error: Azure SAS Key not configured.  Set AZURE_SAS_KEY_NAME and AZURE_SAS_KEY as environment variables to configure the azure sas key.");
        return;
    }
    
    if (!azure_eventhub_name) {
        this.log.error("Error: Azure eventhub name not configured. Set AZURE_EVENTHUB_NAME as environment variables to configure the azure eventhub.");
    }
    
    this.eventHub = sbus.EventHubClient(servicebus, azure_eventhub_name, sas_key_name, sas_key);
}

EventHubProvider.prototype.archive = function(message, optionsOrCallback, callback) {
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
    
    this.eventHub.send(message, message.from, callback);
};

module.exports = EventHubProvider;
