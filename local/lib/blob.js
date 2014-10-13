var fs = require('fs')
  , path = require('path');

function LocalBlobProvider(config, log) {
    if (!config.blob_storage_path) {
        log.warn("Local storage path not configured.");
        return;
    }

    this.config = config;
    this.log = log;
}

LocalBlobProvider.prototype.create = function(blob, readStream, callback) {
    var fileStream = fs.createWriteStream(this.makePath(blob));

    readStream.pipe(fileStream);

    fileStream.on('finish', function() {
        blob.content_length = fileStream.bytesWritten;

        return callback(null, blob);
    });
};

LocalBlobProvider.prototype.makePath = function(blob) {
    return path.join(this.config.blob_storage_path, blob.id);
};

LocalBlobProvider.prototype.initialize = function(callback) {
    var self = this;
    fs.exists(this.config.blob_storage_path, function(exists) {
        if (!exists) {
            fs.mkdir(self.config.blob_storage_path, callback);
        } else {
            callback();
        }
    });
};

LocalBlobProvider.prototype.stream = function(blob, stream, callback) {
    var blobPath = this.makePath(blob);
    var self = this;

    fs.exists(blobPath, function(exists) {
        if (!exists) return callback(null, null);

        var fileStream = fs.createReadStream(self.makePath(blob));
        fileStream.pipe(stream);
        // TODO: handle errors in pipe
        fileStream.on('end', function() {
            callback(null, blob);
        });
    });
};

LocalBlobProvider.prototype.remove = function(blob, callback) {
    fs.unlink(this.makePath(blob), callback);
};

module.exports = LocalBlobProvider;
