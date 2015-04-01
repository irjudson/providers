var async = require('async')
  , azure = require('azure')
  , redis = require('redis')
  , sift = require('sift');

function QueuesPubSubProvider(config, log) {
    this.queueService = azure.createQueueService(
         config.azure_storage_account || process.env.AZURE_STORAGE_ACCOUNT,
         config.azure_storage_key || process.env.AZURE_STORAGE_KEY
    );

    this.config = config;
    this.log = log;
    this.clients = {};

    this.SUPPORTS_PERMANENT_SUBSCRIPTIONS = true;
}

QueuesPubSubProvider.NO_MESSAGES_WAIT_MILLISECONDS = 1000;

QueuesPubSubProvider.subscriptionKey = function(subscription) {
    return subscription.id;
};

QueuesPubSubProvider.prototype.createSubscription = function(subscription, callback) {
    var self = this;
    this.log.info('QueuesPubSubProvider: CREATING subscription for type: ' + subscription.type + ' with id: ' + subscription.id + ' with filter: ' + JSON.stringify(subscription.filter));

    var subscriptionKey = QueuesPubSubProvider.subscriptionKey(subscription);

    this.queueService.createQueueIfNotExists(subscriptionKey, callback);
};

QueuesPubSubProvider.prototype.publish = function(type, item, callback) {
    var self = this;

    // for each principal this item is visible_to
    self.log.debug("QueuesPubSubProvider: ITEM: " + JSON.stringify(item));
    self.log.debug("QueuesPubSubProvider: ITEM VISIBLE_TO: " + JSON.stringify(item.visible_to));

    async.each(item.visible_to, function(visibleToId, visibleToCallback) {

        // query the subscriptions that principal has
        self.services.subscriptions.find(self.services.principals.servicePrincipal, { principal: visibleToId }, {}, function(err, subscriptions) {
            if (err) return visibleToCallback(err);

            self.log.debug("QueuesPubSubProvider: principal: " + visibleToId + " subscriptions: " + JSON.stringify(subscriptions));

            async.each(subscriptions, function(subscription, subscriptionCallback) {
                self.log.debug("QueuesPubSubProvider: CHECKING subscription: name: " + subscription.id + " type: " + subscription.type + " filter: " + JSON.stringify(subscription.filter));
                if (subscription.type !== type) return subscriptionCallback();

                //self.log.info("message: " + JSON.stringify(item));

                var unfilteredItems = sift(subscription.filter, [item]);
                if (unfilteredItems.length === 0) return subscriptionCallback();

                self.log.debug("QueuesPubSubProvider: MATCHED subscription: name: " + subscription.id + " type: " + subscription.type + " filter: " + JSON.stringify(subscription.filter));

                var messageString = JSON.stringify(item);
                var subscriptionKey = QueuesPubSubProvider.subscriptionKey(subscription);

                self.log.debug("QueuesPubSubProvider: PUBLISHING TO SUBSCRIPTION: " + subscriptionKey);

                self.queueService.createMessage(subscriptionKey, messageString, function(err) {
                    self.log.debug("QueuesPubSubProvider: FINISHED PUBLISHING TO SUBSCRIPTION: " + subscriptionKey);

                    return subscriptionCallback(err);
                });
            }, visibleToCallback);
        });
    }, callback);
};

QueuesPubSubProvider.prototype.receive = function(subscription, callback) {
    var subscriptionKey = QueuesPubSubProvider.subscriptionKey(subscription);
    var self = this;

    self.log.debug("QueuesPubSubProvider: CHECKING FOR MESSAGES.");

    this.queueService.getMessages(subscriptionKey, { numOfMessages: 1 }, function(err, messages, response) {
        if (err) return callback(err);
        if (messages.length === 0) {
            self.log.debug("QueuesPubSubProvider: NO MESSAGES: PAUSING.");
            return setTimeout(callback, QueuesPubSubProvider.NO_MESSAGES_WAIT_MILLISECONDS);
        }

        var item = JSON.parse(messages[0].messagetext);

        var ref = {
            subscription: subscription,
            item: messages[0]
        };

        self.log.debug("QueuesPubSubProvider: RECEIVED on subscription: name: " + subscription.name + " type: " + subscription.type + " filter: " + JSON.stringify(subscription.filter) + " item: " + JSON.stringify(item));

        return callback(null, item, ref);
    });
};

QueuesPubSubProvider.prototype.ackReceive = function(ref, success) {
    if (!ref || !ref.subscription || !ref.item) return;

    var subscriptionKey = QueuesPubSubProvider.subscriptionKey(ref.subscription);

    if (success) {
        this.queueService.deleteMessage(subscriptionKey, ref.item.messageid, ref.item.popreceipt, function() {});
    }
};

QueuesPubSubProvider.prototype.removeSubscription = function(subscription, callback) {
    this.log.info("QueuesPubSubProvider: REMOVING subscription: " + subscription.id);

    var subscriptionKey = QueuesPubSubProvider.subscriptionKey(subscription);
    this.queueService.deleteQueue(subscriptionKey, callback);
};

QueuesPubSubProvider.prototype.staleSubscriptionCutoff = function() {
    return new Date(new Date().getTime() + -4 * QueuesPubSubProvider.NO_MESSAGES_WAIT_MILLISECONDS);
};

//// TESTING ONLY METHODS BELOW THIS LINE

QueuesPubSubProvider.prototype.displaySubscriptions = function(callback) {
    return callback();
};

QueuesPubSubProvider.prototype.resetForTest = function(callback) {
    return callback();
};

module.exports = QueuesPubSubProvider;
