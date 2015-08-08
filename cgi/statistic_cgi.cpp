#include <iostream>
#include <hdf.h>
#include "json.h"
#include "mysql++.h"

class Statistic {
public:
    Statistic();
    ~Statistic();
    string getconfig();
    int run();
private:
    Json::Value config;
    mysqlpp::Connection conn;
}
int main(int argc, char *argv[])
{

    return 0;
}
