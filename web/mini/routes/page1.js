var express = require('express');
var router = express.Router();
var receive = require('../receiver');
var redisClient = require('../redisClient.js');
var Heap = require('heap');
var fs = require('fs');

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/prov/all', function (req, res, next) {
    var result = null;
    redisClient.get('result', function (err, reply) {
        if (err) {
            console.log(err);
            next(err);
        }
        else {
            var result = JSON.parse(reply);
            reply = null;
            var data = [];

            for (var i in result) {
                if (i == 'count_per_five_second') {
                    continue;
                }
                if (i == 'source') {
                    continue;
                }
                if (i == 'time') {
                    continue;
                }
                data.push({name: i, value: result[i]['total']});
            }
            console.log(data);
            var top10 = Heap.nlargest(data, 10, function (a,b) {
                return a.value - b.value;
            });
            var top10_result = {prov: [], value: []};
            top10.forEach(function(item) {
                top10_result.prov.push(item.name);
                top10_result.value.push(item.value);
            });
            var start_time = new Date(result.time.start_time);
            var times = [];
            var time_counts = result.count_per_five_second;
            for(var i = 0; i < time_counts.length; i++) {
                times.push(start_time.toLocaleTimeString());
                start_time.setSeconds(start_time.getSeconds() + 5);
            }
            var time_count = {};
            time_count.time = times;
            time_count.count = time_counts;

            var page1 = {};
            page1.prov = data;
            page1.top10 = top10_result;
            page1.time_count = time_count;
            console.log(top10_result);
            res.type('application/json');
            res.json(page1);
            fs.open('../routes/page1.json', 'w', function(err, fd) {
                if (err) {
                    console.log(err);
                }
                else {
                    fs.write(fd, JSON.stringify(page1, null, '\t'), function(err, written, string) {
                        if (err) {
                            console.log(err);
                        }
                        else {
                            console.log("written ", written, " bytes");
                        }
                    });
                }
            });
        }
    });
});

module.exports = router;
