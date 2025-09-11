/**
 * 
 * @file pb5_data.h
 * Contains data structures and classes for data collection and storage. 
 */
/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: pb5_data.h,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:54:46 $
 *   $State: Exp $
 *****************************************************/

#ifndef PBDATA_H
#define PBDATA_H

#include <vector>
#include <string>
#include <map>
#include <stdexcept>
#include <memory>
#include <libxml2/libxml/parser.h>
#include <libxml2/libxml/tree.h>
#include <typeinfo>
#include "utils.h"
using namespace std;

/**
 * Structure containing second and nanosecond component of a time measurement.
 */
struct NSec {
    NSec() : sec(0), nsec(0) {}
    void operator+=(NSec& timeVal);
    uint4 sec;
    uint4 nsec;
    bool operator<(const NSec& other) const;
    bool operator>(const NSec& other) const;
    bool operator==(const NSec& other) const;
};

/**
 * Compares two NSec structures.
 * @return Returns -1, 0, 1 if nsec1 is less than, equal or greater than nsec2.
 */
int nseccmp(const NSec& nsec1, const NSec& nsec2);

#define SECS_BEFORE_1990 631152000
#define InvalidTableName      1

/**
 * Structure containing various metadata information about the datalogger 
 * programming environment. The programming statistics transaction is used
 * fetch this information.
 */
struct DLProgStats {
    DLProgStats() : OSSig((uint2)0), ProgSig((uint2)0) {}
    string OSVer;
    uint2  OSSig;
    string SerialNbr;
    string PowUpProg;
    string ProgName;
    uint2  ProgSig;
};


/**
 * A collection of all parameters that can be used to configure the data 
 * download and persistence process.
 */
typedef struct {
    string WorkingPath;
    string StationName;
    string LoggerType;
} DataOutputConfig;

/**
 * Data structure that mirrors the binary structure in which the metadata for
 * a "Table" is stored in the data logger memory. As obvious, a table contains
 * measurements/data for a collection of Fields. A datalogger typically stores
 * data for multiple tables.
 */
struct Table {
    Table() : TblNum(0), TblSize((uint4)0), TblSignature((uint2)0),
    NextRecordNbr(0){}
    /* 
     * The following parameters are read in from the Table Definitions file
     * stored on the logger.
     */
    string TblName;
    int    TblNum;
    uint4  TblSize;
    byte   TimeType;
    NSec   TblTimeInfo;
    NSec   TblTimeInterval;
    vector<Field>  field_list;
    uint2  TblSignature;
    NSec LastRecordTime;
    int  NextRecordNbr;
};

class TableDataWriter;

/**
 * Class for holding the data structure information for Tables being stored
 * on the data logger and provide an interface for data storage.
 */

class TableDataManager {

    public :
        TableDataManager(const string separator,const string working_path);
        ~TableDataManager ();

        const DataOutputConfig& getDataOutputConfig() const;
        void   setDataOutputConfig(const DataOutputConfig& data_opt);

        const DLProgStats& getProgStats () const; 
        void   setProgStats (DLProgStats& stats);

        TableDataWriter* getTableDataWriter();

        int BuildTDF(istream& table_structure);
        int    xmlDumpTDF (char *filename);

        Table& getTableRef (const string& TableName) throw (invalid_argument);
        int    storeRecord (Table& tbl_ref, byte **data, uint4 rec_num,bool parseTimestamp)
               throw (StorageException);
        int    getRecordSize (const Table& tbl);
        int    getMaxRecordSize();

        int getNextRecordNumber(const Table& tbl);
        void initWrite(string table_name);
        void finishWrite(string table_name);

        vector<Table>& getTables(){
            return tableList__;
        }

    protected : 
        int    readTableDefinition (int table_num, byte *ptr, byte *endptr);
        int    readFieldList (byte *ptr, byte *endptr, Table& Tbl);

        void   writeTableToXml (xmlNodePtr doc_root, Table& tbl);
        void   writeFieldToXml (xmlNodePtr table_node, Field& var);

        void   logUnimplementedDataError(const Field& var);

        void   storeDataSample(const Field& var, byte **data);
        int    getFieldSize (const Field& field);


    private :
        byte          fslVersion__;
        vector<Table> tableList__;
        DataOutputConfig dataOutputConfig__;
        DLProgStats   dataLoggerProgStats__;
        map<string,TableDataWriter*> tblDataWriters;
        string separator;
        string working_path__;
        string additional_header__;
};

/**
 * Interface for implementing data storage functionalities.
 * This contains a list of callback functions that are executed at 
 * different points of a record processing cycle. Concrete implementation
 * of this interface would support different persistence mechanisms as
 * file-based (ASCII, NetCDF) or database driven storage.
 */
class TableDataWriter {
public: 
    TableDataWriter(Table& table):table__(table){}
    virtual ~TableDataWriter() {}

    /** Function called while starting data collection for a specific table. */
    virtual void initWrite()=0;

    /** Function called just before processing a binary data record */
    virtual void processRecordBegin(int recordIdx, 
                NSec recordTime) = 0;

    virtual void logUnimplementedDataError (const Field& var) = 0;
    virtual void storeDataSample (const Field& var, byte **data)=0;
    /** Function called for storing a bool data sample */
    virtual void storeBool(const Field& var, bool flag) = 0;

    /** Function called for storing a integer data sample */
    virtual void storeInt(const Field& var, int num) = 0;

    /** Function called for storing a float data sample */
    virtual void storeFloat(const Field& var, float num) = 0;

    /** Function called for storing a c-string data sample */
    virtual void storeString(const Field& var, string& str) = 0;

    /** Function called for storing a unsigned integer data sample */
    virtual void storeUint4(const Field& var, uint4 num) = 0;

    /** Function called for storing a unsigned short data sample */
    virtual void storeUint2(const Field& var, uint2 num) = 0;

    /** Function invoked when a sample with unknown format is found */
    virtual void processUnimplemented(const Field& var) = 0;

    /** Function called upon completion of parsing a binary data record */
    virtual void processRecordEnd() = 0;

    /** 
     * Function called to indicate the completion of data collection
     * for a specific table. 
     */
    virtual void finishWrite() throw (StorageException) = 0; 

protected:
    Table& table__;
private:
    /** 
     * A handle to the TableDataManager object which will invoke this 
     * writer.
     */ 
};


/**
 * Implementation of the TableDataWriter interface for writing to cout
 */
class CharacterOutputWriter : public TableDataWriter {
public:
    CharacterOutputWriter(const string pipe_name, 
        string separator, 
        Table& table, 
        const string& additional_header=""
    );
    virtual void initWrite();
    virtual void processRecordBegin(int recordIdx,NSec recordTime);
    virtual void storeDataSample (const Field& var, byte **data);
    virtual void logUnimplementedDataError (const Field& var);
    virtual void storeBool(const Field& var, bool flag);
    virtual void storeInt(const Field& var, int num);
    virtual void storeFloat(const Field& var, float num);
    virtual void storeString(const Field& var, string& str);
    virtual void storeUint2(const Field& var, uint2 num);
    virtual void storeUint4(const Field& var, uint4 num);

    virtual void processUnimplemented(const Field& var);
    virtual void processRecordEnd();
    virtual void finishWrite() throw (StorageException);

    static int   GetTimestamp(char *timestamp, const NSec& timeInfo);

protected:
    ofstream dataFileStream__;
    string getFileTimestamp(uint4 sample_time) throw (invalid_argument);
    void   reportRecordCount();

private:
    string   dataDir__;
    string     seperator__;
    int      recordCount__;        
    DLProgStats   dlProgStats__;
    const string& additional_header__;
    bool header_sent;
    bool fileValid(ifstream& file);
};
NSec   parseRecordTime(const byte* data);

void writeHeader(ostream& out, const Table& table, const string& additional_header);
int readLastRecordNumber(ifstream& file);
void printHeaderLine(ostream& out,const char* prefix, const vector<Field>& fieldList, 
        int infoType);
float GetFinalStorageFloat (uint2 unum);
//! Function to convert a bit pattern to the equivalent floating
//! point number following specifications of IEEE-754 standard.
float  intBitsToFloat (uint4 bits);


#endif