function NullEmailProvider(config, log) {
}

NullEmailProvider.prototype.send = function(email, callback) {
	return callback();
};

module.exports = NullEmailProvider;