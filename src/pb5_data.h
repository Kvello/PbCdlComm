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
#include <stdexcept>
#include <memory>
#include <libxml2/libxml/parser.h>
#include <libxml2/libxml/tree.h>
#include <typeinfo>
#include "utils.h"
using namespace std;

typedef unsigned short uint2;
typedef unsigned int   uint4;
typedef unsigned char  byte;

/**
 * Structure containing second and nanosecond component of a time measurement.
 */
struct NSec {
    NSec() : sec(0), nsec(0) {}
    void operator+=(NSec& timeVal);
    uint4 sec;
    uint4 nsec;
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
 * Structure containing per table based output options for downloading and 
 * storing data. 
 */
struct TableOpt {
    TableOpt() : TableSpan(3600), SampleInt(0) {}
    string TableName;
    int    TableSpan;
    int    SampleInt;
} ;

/**
 * A collection of all parameters that can be used to configure the data 
 * download and persistence process.
 */
typedef struct {
    string WorkingPath;
    string StationName;
    string LoggerType;
    vector<TableOpt> Tables;
} DataOutputConfig;

/**
 * Structure representing a data field or variable whose members mirror the
 * the storage format for storing the metadata for a field in datalogger memory.
 */
struct Field {
    Field() : FieldType(0), NullByte(0), BegIdx((uint4)0), 
            Dimension((uint4)0), SubDimListTerm((uint4)0) {}
    byte   FieldType;
    string FieldName;
    byte   NullByte;
    string Processing;
    string Unit;
    string Description;
    uint4  BegIdx;
    uint4  Dimension;
    vector<uint4>  SubDim;
    uint4  SubDimListTerm;

    string getProperty(int infoType, int dim) const;
} ;

/**
 * Data structure that mirrors the binary structure in which the metadata for
 * a "Table" is stored in the data logger memory. As obvious, a table contains
 * measurements/data for a collection of Fields. A datalogger typically stores
 * data for multiple tables.
 */
struct Table {
    Table() : TblNum(0), TblSize((uint4)0), TblSignature((uint2)0), header_sent(false){}
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
    bool header_sent;
    NSec LastRecordTime;
};

class TableDataWriter;

/**
 * Class for holding the data structure information for Tables being stored
 * on the data logger and provide an interface for data storage.
 */

class TableDataManager {

    public :
        TableDataManager(string pipe_name, string separator);
        ~TableDataManager ();

        const DataOutputConfig& getDataOutputConfig() const;
        void   setDataOutputConfig(const DataOutputConfig& data_opt);

        const DLProgStats& getProgStats () const; 
        void   setProgStats (DLProgStats& stats);

        TableDataWriter* getTableDataWriter();
        void   setTableDataWriter(TableDataWriter* tblDataWriter);

        int BuildTDF(istream& table_structyre);
        int    xmlDumpTDF (char *filename);

        Table& getTableRef (const string& TableName) throw (invalid_argument);
        int    storeRecord (Table& tbl_ref, byte **data, uint4 rec_num)
               throw (StorageException);
        int    getRecordSize (const Table& tbl);
        int    getMaxRecordSize();


    protected : 
        int    readTableDefinition (int table_num, byte *ptr, byte *endptr);
        int    readFieldList (byte *ptr, byte *endptr, Table& Tbl);

        void   writeTableToXml (xmlNodePtr doc_root, Table& tbl);
        void   writeFieldToXml (xmlNodePtr table_node, Field& var);

        const char* getDataType (const Field& var);
        void   logUnimplementedDataError(const Field& var);

        void   storeDataSample(const Field& var, byte **data);
        int    getFieldSize (const Field& field);


    private :
        byte          fslVersion__;
        vector<Table> tableList__;
        DataOutputConfig dataOutputConfig__;
        DLProgStats   dataLoggerProgStats__;
        auto_ptr<TableDataWriter> tblDataWriter__;
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
    TableDataWriter() : tableDataMgr__(NULL) {}
    virtual ~TableDataWriter() {}

    /** Getter function for the pointer to current TableDataManager object */
    const TableDataManager* getTableDataManager() const throw (runtime_error) 
    {
        if (NULL == tableDataMgr__) {
            string errorMsg("Uninitialized tableDataMgr__ member in ");
            errorMsg += typeid(*this).name();
            throw runtime_error(errorMsg);
        }
        return tableDataMgr__;
    }

    /** Setter function for the pointer to a TableDataManager object */
    void setTableDataManager(const TableDataManager* tblDataMgr) 
    {
        tableDataMgr__ = const_cast<TableDataManager*> (tblDataMgr);
    }
    /** Function called while starting data collection for a specific table. */
    virtual void initWrite(Table& tblRef) = 0;

    /** Function called just before processing a binary data record */
    virtual void processRecordBegin(Table& tblRef, int recordIdx, 
                NSec recordTime) = 0;

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
    virtual void processRecordEnd(Table& tblRef) = 0;

    /** 
     * Function called to indicate the completion of data collection
     * for a specific table. 
     */
    virtual void finishWrite(Table& tblRef) throw (StorageException) = 0; 

private:
    /** 
     * A handle to the TableDataManager object which will invoke this 
     * writer.
     */ 
    TableDataManager* tableDataMgr__;
};


/**
 * Implementation of the TableDataWriter interface for writing to std::cout
 */
class CharacterOutputWriter : public TableDataWriter {
public:
    CharacterOutputWriter(const string pipe_name, string separator);
    virtual void initWrite(Table& tblRef);
    virtual void processRecordBegin(Table& tblRef, int recordIdx, 
                NSec recordTime);

    virtual void storeBool(const Field& var, bool flag);
    virtual void storeInt(const Field& var, int num);
    virtual void storeFloat(const Field& var, float num);
    virtual void storeString(const Field& var, string& str);
    virtual void storeUint2(const Field& var, uint2 num);
    virtual void storeUint4(const Field& var, uint4 num);

    virtual void processUnimplemented(const Field& var);
    virtual void processRecordEnd(Table& tblRef);
    virtual void finishWrite(Table& tblRef) throw (StorageException);

    static int   GetTimestamp(char *timestamp, const NSec& timeInfo);

protected:
    ofstream dataFileStream__;
    void   writeHeader(const Table& tbl_ref);
    void   printHeaderLine(const char* prefix, const vector<Field>& fieldList, 
               int infoType);
    string getFileTimestamp(uint4 sample_time) throw (invalid_argument);
    void   reportRecordCount();

private:
    string   dataDir__;
    string     seperator__;
    int      recordCount__;
};
string GetVarLenString (const byte *ptr);
string GetFixedLenString (const byte *str_ptr, Field& var);
NSec   parseRecordTime(const byte* data);

//! Function to convert a bit pattern to the equivalent floating
//! point number following specifications of IEEE-754 standard.
float  intBitsToFloat (uint4 bits);


#endif