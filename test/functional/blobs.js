var app = require('../../server')
  , assert = require('assert')
  , config = require('../../config')
  , fixtures = require('../fixtures')
  ,	fs = require('fs')
  , request = require('request');

describe('blobs REST endpoint', function() {
	it('should be able to create and then fetch a blob', function(done) {

		var fixture_path = 'test/fixtures/images/image.jpg';

		fs.stat(fixture_path, function(err, stats) {
                assert.ifError(err);

				fs.createReadStream(fixture_path).
				pipe(
					request.post({ url: config.base_url + '/blobs',
						           headers: { 'Content-Type': 'image/jpeg', 'Content-Length': stats.size,
                                              'Authorization': fixtures.authHeaders.device } },
                        function (err, resp, body) {
						    assert.ifError(err);

                            console.log("body: " + body);
						    var body_json = JSON.parse(body);
					        assert.equal(resp.statusCode, 200);
                            assert.equal(body_json.blob._id, undefined);
					        assert.notEqual(body_json.blob.id, undefined);

					        var blob_url = config.base_url + '/blobs/' + body_json.blob.id;

  					        request.get(blob_url, { headers: { 'Authorization': fixtures.authHeaders.device } }, function(err,resp,body) {
  					      	    assert.ifError(err);
					    	    assert.equal(resp.statusCode, 200);
					    	    assert.equal(resp.body.length, 28014);

					    	    done();
					        });
						}
					)
				);
		});

    });

    it('should return 404 for unknown blobs', function(done) {
    	request(config.base_url + '/blobs/51195d5f11600000deadbeef',
                { headers: { 'Authorization': fixtures.authHeaders.device } }, function(err,resp,body) {
	    	  assert.equal(resp.statusCode, 404);

	    	  done();
	      });
    });

    it('should not allow unauthenticated access to blobs', function(done) {
        request(config.base_url + '/blobs/51195d5f11600000deadbeef', function(err,resp,body) {
            assert.equal(resp.statusCode, 401);

            done();
        });
    });


});