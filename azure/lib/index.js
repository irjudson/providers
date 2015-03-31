module.exports = {
    AzureArchiveProvider:   		require('./archive'),
    AzureBlobProvider:      		require('./blob'),
    AzurePubSubProvider:    		require('./pubSub'),
    AzureQueuesPubSubProvider:  	require('./queuesPubSub'),
    AzureEventHubProvider:  		require('./eventhub')
};