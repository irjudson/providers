module.exports = {
    AzureBlobProvider:      		require('./blob'),
    AzureEventHubProvider:  		require('./eventhub'),
    AzurePubSubProvider:    		require('./pubSub'),
    AzureQueuesPubSubProvider:  	require('./queuesPubSub'),
    AzureTableStorageProvider:      require('./tableStorage')
};
