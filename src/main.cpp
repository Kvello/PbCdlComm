/**
 * @file main.cpp
 * Contains the entry point to the pbcdl_comm application.
 */

#include <iostream>
#include <cstdlib>
#include "collection_process.h"
#include "utils.h"
#include <log4cpp/Category.hh>
using namespace std;
using namespace log4cpp;

std::auto_ptr<PB5CollectionProcess> proc{ new PB5CollectionProcess() };

void atExit(int signum)
{
    static int caught(0);
    caught++;
    if (caught > 1) {
        Category::getInstance("SignalHandler")
                 .warn("Exiting on multiple signal reception");
        printSigInfo(signum);
        exit(1);
    }
    printSigInfo(signum);
    printStackTrace();
    if (NULL != proc.get()) {
        try {
            delete proc.get();
        } catch(exception& e) {
            cout << e.what() << endl;
        }
    }
    exit(1);
}

int main (int argc, char *argv[])
{
    int stat;
    setSignalHandler(atExit);
    try {
        proc->init(argc,argv);
        proc->run();
    } 
    catch (exception& e) {
        cout << e.what() << endl;
        stat = 1;
    }
    catch (...) {
        cout << "Exception caught while executing collection process." << endl;
        stat = 1;
    }
    return stat;
}
