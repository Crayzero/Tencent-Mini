CREATE TABLE logs(
    id INT(32) PRIMARY KEY AUTO_INCREMENT,
    datetime DATETIME,
    some VARCHAR(100),
    IP VARCHAR(32),
    name VARCHAR(100),
    ISP VARCHAR(40),
    prov VARCHAR(40),
    city VARCHAR(40),
    url VARCHAR(100),
    server VARCHAR(20),
    extra VARCHAR(20)
)
CHARACTER SET utf8;

