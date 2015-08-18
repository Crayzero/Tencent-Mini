var mysql = require('mysql');
var fs = require('fs');

var connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '111111',
    database: 'tencent'
});

connection.connect();

connection.query("select * from location", function (err, rows, fields) {
    var result = {};
    rows.forEach(function (item) {
        console.log(city);
        if (city[city.length-1] != '市') {
            city += "市";
        }
        console.log(city);
        result[city] = {prov: item.prov,
                            lnt: item.lnt,
                            lat: item.lat
                    };
    });
    fs.open("location.js", 'w', function(err, fd) {
        if (err) {
            console.log(err);
            throw(err);
        }
        else {
            fs.write(fd, JSON.stringify(result, null, '\t'), 0, 'utf-8', function(err, written, string) {
                if (err) {
                    console.log(err);
                    throw(err);
                }
                else {
                    console.log("write ", written, " bytes");
                }
            });
        }
    });
    connection.end();
});

