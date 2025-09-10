/**
 * @file main.cpp
 * Contains the entry point to the pbcdl_comm application.
 */

#ifdef _WIN32
#include <windows.h>
void portable_sleep_s(unsigned int s) {
    Sleep(s*1000);
}
#else
#include <unistd.h>
void portable_sleep_s(unsigned int s) {
    sleep(s);
}
#endif
#include <iostream>
#include <cstdlib>
#include <unistd.h>
#include "collection_process.h"
#include "utils.h"

#include <log4cpp/Category.hh>
#include <log4cpp/OstreamAppender.hh>
using namespace std;
using namespace log4cpp;

void printVersion() 
{
   cout << " " << PB5_APP_NAME << " Version : " << PB5_APP_VERS << endl;
   return;
}
void printHelp() 
{
    cout << endl;
    printVersion();
    cout << "  Data Collection Software for PakBus Loggers                " << endl;
    cout << "  Usage : " << PB5_APP_NAME;
    cout << "  Options :                                                  " << endl;
    cout << "     -c Complete path of the collection configuration file   " << endl;
    cout << "     -d Turn on debugging to print packet level errors       " << endl;
    cout << "     -o Path to output file or pipe                            " << endl;
    // cout << "     -e Erase application cache                              " << endl;
    cout << "     -w Override the working path mentioned in config file   " << endl;
    cout << "     -r Redirect log msgs to a file instead of stdout. The   " << endl;
    cout << "        logs will be stored in the <workingPath> directory   " << endl;
    cout << "     -h Print this help message                              " << endl;
    cout << "     -v Print version information                            " << endl;
    cout << endl;

    return;

}
/**
 * Function to parse the command line inputs and appropriately update/initialize
 * various class members and set logging destination.
 */
void parseCommandLineArgs(
    int argc, 
    char* argv[], 
    bool& executionComplete,
    bool& optDebug,
    CommInpCfg& appConfig,
    string& separator,
    string& connectionString
)
{
    char optstring[] = "c:p:w:s:drvh";
    // --- defaults here ---
    std::string configFilePath   = "./config.xml";
    std::string workingPath      = "";
    separator    = ", ";
    optDebug     = false;
    int         cmd_opt;
    bool        optDisplayHelp = false;
    bool        optDisplayVersion = false;
    bool        optRedirectLog(false);

    if (argc == 1) {
        printHelp();
        executionComplete = true;
        return;
    }


    while((cmd_opt = getopt(argc, argv, optstring)) != -1) {
        switch(cmd_opt) {
            case 'c' : configFilePath = optarg;  break;
            case 'd' : optDebug = true;       break;
            case 'p' : connectionString = optarg;        break;
            // case 'e' : optCleanAppCache = true;  break;
                       // TODO implement the clean app cache option
            case 'r' : optRedirectLog = true;     break;
            case 'w' : workingPath = optarg;     break;
            case 'h' : optDisplayHelp = true;  break;
            case 'v' : optDisplayVersion = true;  break;
            case 's' : separator = optarg; break;
            case '?' : throw invalid_argument("Invalid argument provided for initialization");
        }
    }

    if (optDisplayHelp) {
        printHelp();
        executionComplete = true;
        return;
    }

    if (optDisplayVersion) {
        printVersion();
        executionComplete = true;
        return;
    }

    if (optDebug) {
        cout << "Enabling debug mode ..." << endl;
        Category::getRoot().setPriority(Priority::DEBUG);
    }
    else {
        Category::getRoot().setPriority(Priority::INFO);
    }

    try {
        if (optRedirectLog) {
            appConfig.redirectLog();
        }
        cout << "============================================================" << endl;
        printVersion();
        cout << "============================================================" << endl;

        Category::getInstance("Init").debug("Using configuration file : " + 
                configFilePath);
        appConfig.loadConfig ((char *)configFilePath.c_str());


    } catch (exception& e) {
        throw AppException(__FILE__, __LINE__, e.what());
    }

    if (workingPath.size()) {
        appConfig.setWorkingPath(workingPath);
    }

    return;
}
int main (int argc, char *argv[])
{
    int stat = 0;
    bool executionComplete = false;
    bool optDebug = false;
    CommInpCfg appConfig;
    string sep="";
    string connectionString="";
    parseCommandLineArgs(argc,
        argv,
        executionComplete,
        optDebug,
        appConfig,
        sep,
        connectionString);
    cout<<"parseCommanLineArgs finished"<<endl;
    std::auto_ptr<PB5CollectionProcess> proc( 
        new PB5CollectionProcess(
        sep, 
        executionComplete,
        optDebug,
        appConfig,
        connectionString,
        -1 //-1 indicates to only use last record
     )
    );

    try {
        proc->init(argc,argv);
        cout<<"init finished"<<endl;
        while (true){
            proc->run();
            int fastest_table_sec = proc->smallestTableInt();
            cout<<"Fastest table(sec)"<<fastest_table_sec<<endl;
            portable_sleep_s(fastest_table_sec);
        }
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
