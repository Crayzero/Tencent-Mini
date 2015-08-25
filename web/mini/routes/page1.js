var express = require('express');
var router = express.Router();
var redisClient = require('../redisClient.js');
var Heap = require('heap');
var fs = require('fs');

/* GET home page. */

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
            var total = 0;

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
                if (i == 'top10') {
                    continue;
                }
                if (i == 'extra') {
                    continue;
                }
                data.push({name: i, value: result[i]['total']});
                total += result[i]['total'];
            }
            if (data.length == 0) {
                res.type('application/json');
                res.json({});
                res.end();
                return ;
            }
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
            for(var i = 0; i < Math.min(time_counts.length,60); i++) {
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
            page1.total = total;
            res.type('application/json');
            res.json(page1);

            /* write the result to file
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
            */
        }
    });
});

module.exports = router;
