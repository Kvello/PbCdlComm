/**
 * @file collection_process.h
 * Contains classes for modeling data collection processes.
 */

#ifndef COLLECTION_PROCESS_H
#define COLLECTION_PROCESS_H

#include <stdexcept>
#include <iostream>
#include <memory>
#include <exception>
#include <string>
#include <sstream>
#include <log4cpp/Category.hh>
#include "pb5.h"
#include "init_comm.h"

using namespace std;

/**
 * DataCollectionProcess is an interface for implementing a generic process.
 */
class DataCollectionProcess {
public:
    virtual ~DataCollectionProcess() throw() {}
    /*
     * Initialize class members using command line arguments prior to execution.
     */
    virtual void init(int argc, char* argv[]) throw (exception) = 0;
    /*
     * Execute the process.
     */
    virtual void run() throw (exception) = 0;
    /*
     * This would provide cleanup functions while exiting on a signal receipt.
     */
    virtual void onExit() throw (exception) = 0;
    /* 
     * Print usage information to standard output.
     */
    virtual void printHelp() throw () = 0;
    /*
     * Print version information to standard output.
     */
    virtual void printVersion() throw () = 0;
};

/**
 * Implementation of the DataCollectionProcess interface for collectinng data from
 * a PakBus (2005) protocol based datalogger.
 */
class PB5CollectionProcess : public DataCollectionProcess {
public:
    PB5CollectionProcess();
    ~PB5CollectionProcess() throw();
    virtual void init(int argc, char* argv[]) throw (exception);
    virtual void run() throw (exception);
    virtual void onExit() throw ();
    virtual void printHelp() throw ();
    virtual void printVersion() throw ();

protected :
    void parseCommandLineArgs(int argc, char* argv[]) throw (exception);
    void configure() throw (AppException);
    void checkLoggerTime() throw (AppException);
    void initSession(int nTry) throw (AppException);
    void collect() throw (AppException);
    void closeSession() throw ();
    void exitHandler(int signum) throw ();

private:
    auto_ptr<DataSource> dataSource__;
    CommInpCfg       appConfig__;
    pakbuf           IObuf__;
    TableDataManager tblDataMgr__;
    PakCtrlObj       pakCtrlImplObj__;
    BMP5Obj          bmp5ImplObj__;

    string           lockFilePath__;
    bool             optDebug__;
    bool             optCleanAppCache__;
    bool             executionComplete__;
    bool             loggerTimeCheckComplete__;
    stringstream     msgstrm;
};

#define PB5_APP_NAME "PbCdlComm"
// #define PB5_APP_VERS "1.3.5 (2008/07/28)"
// #define PB5_APP_VERS "1.3.6 (2009/07/16)"
// #define PB5_APP_VERS "1.3.8 (2010/05/12)" 
// 1.3.8 Lock file creation will happen in /tmp instead of /var/tmp

#define PB5_APP_VERS "1.3.9 (2010/08/31)" 
#endif
