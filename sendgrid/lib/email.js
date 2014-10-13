var async = require('async')
  , sendgrid = require('sendgrid')
  , sift = require('sift');

function SendgridEmailProvider(config, log) {
    if (!process.env.SENDGRID_API_USER || !process.env.SENDGRID_API_KEY) {
        return log.error('sendgrid email provider:  environmental variables SENDGRID_API_USER and SENDGRID_API_KEY not both set.');
    }

    this.config = config;
    this.log = log;
    this.client = sendgrid(process.env.SENDGRID_API_USER, process.env.SENDGRID_API_KEY);
}

SendgridEmailProvider.prototype.send = function(email, callback) {
    this.client.send(email, callback);
};

module.exports = SendgridEmailProvider;
