var express = require('express');
var router = express.Router();
var receive = require('../receiver');
var redisClient = require('../redisClient.js')

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/prov', function (req, res, next) {
    var result = null;
    redisClient.get('result', function (err, reply) {
        if (err) {
            console.log(err);
            next(err);
        }
        else {
            var result = JSON.parse(reply);
            reply = null;
            for (var i in result) {
                if (i == "processed") {
                    continue;
                }
                if (i == 'count_per_five_second') {
                    continue;
                }
                if (i == 'source') {
                    continue;
                }
            }
            res.type('application/json');
            res.json(result);
        }
    });
});

router.get('/flow', function (req, res, next) {
    redisClient.get('result', function(err, reply) {
        if (err) {
            console.log(err)
            next(err);
        }
        else {
            var result = JSON.parse(reply);
            reply = null;
            if (result != null) {
                result = result['source'];
                for (var city in result) {
                    //console.log(city, result[city]['total']);
                }
            }
            res.type('application/json');
            res.json(result);
        }
    });
});

module.exports = router;
