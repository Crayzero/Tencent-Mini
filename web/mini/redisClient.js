var redis = require('redis');
var config = require('./config');

redisClient = redis.createClient(config.redis.port,
                                config.redis.host, config.redis.options);

redisClient.on('error', function (err) {
    console.log('Error ' + err);
    throw(err);
});

redisClient.on('connect', function (err) {
    redisClient.get_key = function(key, callback) {
        redisClient.get(key, function (err, reply) {
            if (err) {
                callback(err, null);
            }
            else {
                callback(null, reply);
            }
        });
    }
});

redisClient.on('end', function (err, res) {
    console.log("redis closed!");
});

module.exports = redisClient;
