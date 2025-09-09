/**
 * @file pb5_data_writer.cpp
 * This will contain implementation of "writer" modules using various persistence mechanisms.
 * Presently only the CharacterOutputWriter class is implemented. 
 */
#include <stdexcept>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <log4cpp/Category.hh>
#include "pb5.h"
#include "utils.h"
#include "collection_process.h"
using namespace std;
using namespace log4cpp;


/**
 * Constructor for an CharacterOutputWriter object.
 * 
 * @param datadir: Data directory for storing final datafiles.
 * @param filespan: Maximum timespan of a datfile. Defaults to 60 minutes.
 * @param sep: Seperator/delimiter character to use while writing data records.
 */
CharacterOutputWriter :: CharacterOutputWriter(const string pipe_name, string sep) : 
seperator__(sep), recordCount__(0)
{
    dataFileStream__.exceptions(ofstream::badbit | ofstream::failbit); 
    dataFileStream__.open(pipe_name.c_str(),ios_base::app);
}

/**
 * Convert the number of seconds (and nanoseconds) since 1990 into an 
 * equivalent timestamp. 
 *
 * @param timestamp: Pointer to the character string for storing the timestamp.
 * @param timeInfo:  Reference to the NSec structure containing time information.
 */

int CharacterOutputWriter :: GetTimestamp (char *timestamp, const NSec& timeInfo)
{
    if (!timestamp) {
        return FAILURE;
    }

    struct tm *ptm;
    static int nano_precision = 3;
    static int factor = (10^(6-nano_precision));

    int nsecs = (int)(timeInfo.nsec/factor);
    // The sec component of the NSec datatype represents number of seconds 
    // since 1990.
    time_t  secs1970 = timeInfo.sec + SECS_BEFORE_1990;
    ptm = gmtime (&secs1970);
    
    if (ptm) {
        sprintf (timestamp, "\"%04d-%02d-%02d %02d:%02d:%02d.%d\"", 
                ptm->tm_year+1900, ptm->tm_mon+1, ptm->tm_mday, ptm->tm_hour, 
                ptm->tm_min, ptm->tm_sec, nsecs);
        return SUCCESS;
    }
    else {
        return FAILURE;
    }
}

/**
 * Function to obtain a timestamp as part of the filename based on a time 
 * specified using Campbell Scientific Time (measured from 1990).
 *
 * @param sample_time: Time measured from 1990.
 * @return Pointer to a character string containing the timestamp.
 */
string CharacterOutputWriter :: getFileTimestamp (uint4 sample_time) 
        throw (invalid_argument)
{
    if (0 == sample_time) {
        throw invalid_argument("invalid sample time input to getFileTimestamp");
    }
    struct tm *ptm;
    time_t     secs1970 = (time_t) sample_time + SECS_BEFORE_1990;
    char file_timestamp[16];

    ptm = gmtime (&secs1970);
    if (ptm) {
        sprintf (file_timestamp, "%d%02d%02d_%02d%02d%02d", ptm->tm_year+1900,
                ptm->tm_mon+1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min,
                ptm->tm_sec);
    }
    else {
        throw invalid_argument("invalid sample time input to getFileTimestamp");
    }
    return file_timestamp;
}

void CharacterOutputWriter :: initWrite(Table& tblRef)
{
    if(!tblRef.header_sent){
        writeHeader(tblRef);
        tblRef.header_sent = true;
    }
    dataFileStream__<<"["<<tblRef.TblName<<"]"<<endl;
}

void CharacterOutputWriter :: reportRecordCount()
{
    if (recordCount__) {
        stringstream msg;
        msg << "Wrote " << recordCount__ << " records";
        Category::getInstance("CharacterOutputWriter")
                 .debug(msg.str());
        recordCount__ = 0;
    }
}

void CharacterOutputWriter :: processRecordBegin(Table& tbl_ref, int recordIdx, 
        NSec recordTime) 
{
    char timestamp[32];
    // GetTimestamp(timestamp, tbl_ref.LastRecordTime); 
    CharacterOutputWriter::GetTimestamp(timestamp, recordTime);
    // Print the timestamp for the record followed by the values 
    // for each column in the table. 

    dataFileStream__ << timestamp << this->seperator__ << recordIdx;
}

void CharacterOutputWriter :: processRecordEnd(Table& tbl_ref) 
{
    dataFileStream__ << endl;
    recordCount__ += 1;
}

void CharacterOutputWriter :: finishWrite(Table& tblRef) throw (StorageException)
{
    dataFileStream__<<"\n";
    dataFileStream__.flush();
}

void CharacterOutputWriter :: storeBool(const Field& var, bool flag)
{
   dataFileStream__ << this->seperator__ << flag;
}

void CharacterOutputWriter :: storeFloat(const Field& var, float num)
{
   dataFileStream__ << this->seperator__ << num;
}

void CharacterOutputWriter :: storeInt(const Field& var, int num)
{
    dataFileStream__ << this->seperator__ << num;
}

void CharacterOutputWriter :: storeUint4(const Field& var, uint4 num)
{
   dataFileStream__ << this->seperator__ << num;
}

void CharacterOutputWriter :: storeUint2(const Field& var, uint2 num)
{
   dataFileStream__ << this->seperator__ << num;
}

void CharacterOutputWriter :: storeString(const Field& var, string& str)
{
   dataFileStream__ << this->seperator__ << "\"" << str << "\"";;
}

void CharacterOutputWriter :: processUnimplemented(const Field& var)
{
   dataFileStream__ << this->seperator__ << "-9999";
}


string Field::getProperty(int infoType, int dim) const
{
    stringstream formattedPropertyValue;

    switch(infoType) 
    {
        case 1 : if (dim) {
                     formattedPropertyValue << "\"" <<  FieldName << "(" << dim << ")\"";
                 }
                 else {
                     formattedPropertyValue << "\"" <<  FieldName << "\"";
                 }
                 break;
        case 2 : formattedPropertyValue << "\"" <<  Unit << "\"";
                 break;
        case 3 : formattedPropertyValue << "\"" <<  Processing << "\"";
                 break;
        default: throw logic_error("Unknown field property queried"); 
    }
    return formattedPropertyValue.str();
}

void CharacterOutputWriter :: printHeaderLine(const char* prefix, 
        const vector<Field>& fieldList, int infoType)
{
    int dim;
    vector<Field>::const_iterator fieldItr;

    if ((fieldList.size() == 0) || ((infoType < 1) || (infoType > 3))) {
        return;
    }

    fieldItr = fieldList.begin();
    dataFileStream__ << prefix;

    for (fieldItr = fieldList.begin(); fieldItr != fieldList.end(); 
            fieldItr++){
        if ( (fieldItr->Dimension > 1) && ( (fieldItr->FieldType != 11) &&
                (fieldItr->FieldType != 16) ) ) {
            for (dim = 1; dim <= (int)fieldItr->Dimension; dim++) {
               dataFileStream__ << fieldItr->getProperty(infoType, dim);
            }
        }
        else {
           dataFileStream__ << fieldItr->getProperty(infoType, 0);
        }
    }

   dataFileStream__ << endl;
   return;
}

void CharacterOutputWriter :: writeHeader(const Table& tbl_ref)
{
    const vector<Field>& fieldList = tbl_ref.field_list;

    const TableDataManager* tblDataMgr = this->getTableDataManager();

    if (NULL == tblDataMgr) {
        throw runtime_error("NULL TableDataManager member in CharacterOutputWriter!");
    }

    const DataOutputConfig& dataOutputConfig = tblDataMgr->getDataOutputConfig();
    const DLProgStats& dlProgStats = tblDataMgr->getProgStats();

    //////////////////////////////////////////////////////
    // Print the file header : File format type, Station
    // name, Datalogger type, serial number, OS Version,
    // Datalogger program name, Datalogger program 
    // signature and the table name
    //////////////////////////////////////////////////////

   dataFileStream__ << "\"TOA5\",\"" << dataOutputConfig.StationName << "\",\""
                                     << dataOutputConfig.LoggerType << "\",\""
                                     << dlProgStats.SerialNbr << "\",\"" 
                                     << dlProgStats.OSVer << "\",\""
                                     << dlProgStats.ProgName << "\",\"" 
                                     << dlProgStats.ProgSig << "\",\"" 
                                     << tbl_ref.TblName << "\",\"" 
                                     << PB5_APP_NAME << "-" 
                                     << PB5_APP_VERS << "\"" << endl;

    // Print field names
    printHeaderLine("\"TIMESTAMP\",\"RECORD\",", fieldList, 1);

    // Print field unit
    printHeaderLine("\"TS\",\"RN\",", fieldList, 2);

    // Print processing type for each field
    printHeaderLine("\"\",\"\",", fieldList, 3);

    return;
}
