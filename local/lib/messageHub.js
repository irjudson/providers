var log = require('winston')
  , request = require('request');

function ProxyMessageHub(config) {
    this.config = config;
}

ProxyMessageHub.prototype.send = function(context, messages, callback) {
    request.post(this.config.messages_endpoint, {
        json: messages,
        headers: {
            Authorization: 'Bearer ' + context.rawJwtToken
        }
    }, function(err, resp, body) {
        if (err) return callback(err);
        if (resp.statusCode !== 200) return callback(resp.statusCode);

        return callback(null, body.messages);
    });
};

module.exports = ProxyMessageHub;