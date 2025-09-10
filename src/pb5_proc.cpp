/**
 * @file pb5_proc.cpp
 * Implementation of the collection process for collecting data from a PakBus5 logger.
 */

#include "collection_process.h"
#include <log4cpp/Category.hh>
#include <getopt.h>
#include <unistd.h>
#include <cmath>
using namespace std;
using namespace log4cpp;

// TODO - Set vtime through configuration file 
// TODO - Persist connection settings 
PB5CollectionProcess::PB5CollectionProcess(
    string separator,
    bool& executionComplete,
    bool& optDebug,
    CommInpCfg& appConfig,
    string connection_string,
    const int next_record_number
) :
    appConfig__(appConfig),              // 2nd
    IObuf__(8192, 512),                  // 3rd
    pakCtrlImplObj__(),                  // 4th (default-initialized)
    bmp5ImplObj__(separator,appConfig.getDataOutputConfig().WorkingPath), // 5th
    lockFilePath__(),                    // 6th (default-initialized)
    optDebug__(optDebug),                // 7th
    optCleanAppCache__(false),           // 8th
    executionComplete__(executionComplete), // 9th
    loggerTimeCheckComplete__(false),    // 10th (default-initialized)
    msgstrm(),                           // 11th (default-initialized)
    has_table_spec(false)                // 12th

{
    // mkfifo(pipe_name.c_str(),S_IWUSR|S_IRUSR); // Make pipe. Read/write for owner
    dataSource__ = appConfig__.getDataSource(connection_string);
}

PB5CollectionProcess :: ~PB5CollectionProcess() throw ()
{
    this->onExit();
    cout << "============================================================"
         << endl;
}

/**
 * The two-step intialization process involves parsing inputs provided in
 * the standard command-line argument form and then configuring various
 * class memebers based on the inputs.
 */
void PB5CollectionProcess :: init(int argc, char* argv[])
    throw (exception)
{
    executionComplete__ = false;
    configure();
}


bool compareSampleInts(const Table& lhs, const Table& rhs){
    return lhs.TblTimeInterval<rhs.TblTimeInterval;
}
/**
 * Gets the smallest sample interval of all the configured tables(seconds)
 */
int PB5CollectionProcess :: smallestTableInt(){
    const vector<Table>& tables = bmp5ImplObj__.getTableDefinitions();
    vector<Table>::const_iterator it = min_element(tables.begin(),tables.end(),compareSampleInts);
    return static_cast<int>(it->TblTimeInterval.sec);
}

/**
 * This function is responsible for wiring together different class members
 * based on their dependencies.
 */
void PB5CollectionProcess :: configure() throw (AppException)
{
    if (executionComplete__) {
        return;
    }

    appConfig__.dirSetup();

    cout<<"Getting Lock File name"<<endl;

    string lockFilePath__ = dataSource__->getLockFileName(PB5_APP_NAME);

    int pid = is_running ((char *)lockFilePath__.c_str());

    if (pid) {
        msgstrm << PB5_APP_NAME << " is already connected to " 
                << dataSource__->getConnInfo() << " (PID : " << pid << ")";
        Category::getInstance("Init")
                 .warn(msgstrm.str());
        executionComplete__ = true;
        return;
    }
    else {
        if (open_lockfile ((char *)lockFilePath__.c_str(), PB5_APP_NAME)) {
             msgstrm << "Failed to open lock file : " << lockFilePath__;
             throw AppException(__FILE__, __LINE__, msgstrm.str().c_str());
        }
        else {
            Category::getInstance("Init").debug("Opened lock file : " + 
                    lockFilePath__);
        }
    }

    // Wire various objects
    const DataOutputConfig& dataOpt = appConfig__.getDataOutputConfig();
    const PBAddr& pbAddr = appConfig__.getPakbusAddr();

    if (optDebug__ || (Category::getRoot().getPriority() == Priority::DEBUG)) {
        Category::getInstance("Init").debug("Enabling low-level logging");
        IObuf__.setHexLogDir(dataOpt.WorkingPath);
    }

    bmp5ImplObj__.setDataOutputConfig(dataOpt);

    pakCtrlImplObj__.setPakBusAddr(pbAddr);
    pakCtrlImplObj__.setIOBuf(&IObuf__);
   
    bmp5ImplObj__.setPakBusAddr(pbAddr);
    bmp5ImplObj__.setIOBuf(&IObuf__);

    return;
}

/**
 * This function attempts to initiate a connection to the data logger and 
 * retrieve data definitions prior to starting the data download process.
 */
void PB5CollectionProcess :: initSession(int nTry) throw (AppException)
{
    int fd;

    try {
        cout << endl;
        Category::getInstance("InitSession")
                 .info("Trying to establish PakBus session => " + 
                       dataSource__->getConnInfo());
        fd = dataSource__->connect();
        IObuf__.setFd(fd);
        pakCtrlImplObj__.InitComm();
        pakCtrlImplObj__.HelloTransaction();
        pakCtrlImplObj__.HandShake(SERPKT_RING);

        if(!has_table_spec){
            try {
                checkLoggerTime();
                bmp5ImplObj__.getDataDefinitions();
            } 
            catch (IOException& ioe) {
                throw;
            }
            catch (AppException& e) {
                pakCtrlImplObj__.HandShake(SERPKT_FINISHED);
                throw;
            }
            has_table_spec=true;
        }
        pakCtrlImplObj__.HandShake(SERPKT_FINISHED);

    } 
    catch (IOException& ioe) {
        throw;
    }
    catch (AppException& appException) {
        Category::getInstance("InitSession")
                .debug("Failed to establish session, disconnecting from device");
        dataSource__->disconnect();
        throw;
    }

    /* if (!dataSource__->isOpen()) {
        throw AppException(__FILE__, __LINE__, 
               "Failed to establish PakBus session with datalogger");
    } */
}

void PB5CollectionProcess :: closeSession() throw ()
{
    pakCtrlImplObj__.Bye();
}
    
void PB5CollectionProcess :: run() throw (exception)
{
    if (executionComplete__) {
        return;
    }
    loggerTimeCheckComplete__ = false;

    int ntry = 0;

    do {
        try {
            initSession(ntry);
            cout << endl;
            Category::getInstance("InitSession")
                     .notice("Established PakBus session with datalogger at "
                          + dataSource__->getConnInfo());
            collect();
            closeSession();
            break;
        } 
        catch (IOException& ioe) {
            break;  
        } 
        catch (AppException& appe) {
            // Category::getInstance("run").info(appe.what());
            ntry++;
        }
        catch (exception& e) {
            if (e.what() != NULL) {
                Category::getInstance("run").error(e.what());
            }
            else {
                Category::getInstance("run").error("Caught exception");
            }
            break;
        }
    } while (dataSource__->retryOnFail()); 

    this->onExit();
}


void PB5CollectionProcess :: collect() throw (AppException)
{
    bmp5ImplObj__.writeData();
}

void PB5CollectionProcess :: onExit() throw ()
{
    if (dataSource__.get() && dataSource__->isOpen()) {
        dataSource__->disconnect();
    }
    unlink (lockFilePath__.c_str());
}





void PB5CollectionProcess :: checkLoggerTime() throw (AppException)
{
    if (loggerTimeCheckComplete__) {
        return;
    }
    stringstream msgstrm;

    time_t logger_t = (time_t)bmp5ImplObj__.ClockTransaction (0, 0);
    if (!logger_t) {
        throw AppException(__FILE__, __LINE__, "Invalid logger time !");
    }

    time_t host_t = time (NULL);
    time_t time_offset = host_t - logger_t;
   
    cout << "CDL Time Check:" << endl;
    cout << "Local:     localhost " << host_t << " " << ctime(&host_t) << endl;
    cout << "Reference: localhost " << host_t << " " << ctime(&host_t) << endl;
	cout << "System:    " << dataSource__->getAddress() << " " << logger_t  << 
            " " << ctime(&logger_t) << endl;
	cout << "Offset:    " << time_offset << " seconds" << endl;

    if (abs(time_offset) > MAX_TIME_OFFSET) {

        Category::getInstance("TimeCheck")
                 .notice(msgstrm.str());
        logger_t = (time_t)bmp5ImplObj__.ClockTransaction (time_offset, 0);

        if (logger_t) {
            Category::getInstance("TimeCheck")
                     .error("Failed to update logger time.");
            throw AppException(__FILE__, __LINE__,
                    "Failed to set logger time !");
        }
        else {
            Category::getInstance("TimeCheck")
                     .notice("Successfully updated logger time.");
        }
    }
    loggerTimeCheckComplete__ = true;
    return;
}

