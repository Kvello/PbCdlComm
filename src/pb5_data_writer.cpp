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
#include <cmath>
#include <string>
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
CharacterOutputWriter :: CharacterOutputWriter(
    const string pipe_name, 
    string separator,
    Table& table,
    const string& additional_header
) : seperator__(separator),
    TableDataWriter(table),
    recordCount__(0),
    header_sent(false),
    additional_header__(additional_header)
{
    bool recovery_successs = false;
    if(fileExists(pipe_name)){
        //TODO: If the file exists, check integrity(pretty basic), get the newest line, and extract record number
        // Set this record number in the table
        ifstream file(pipe_name.c_str());
        if(file.good()&&fileValid(file)){
            // Records are always the second field in the data line
            // The above should have stripped away the header
            int next_rec_nbr = readLastRecordNumber(file);
            file.close();
            dataFileStream__.open(pipe_name.c_str(),ios_base::app);
            table__.NextRecordNbr=next_rec_nbr;
            recovery_successs = dataFileStream__.good();
        }
    }
    if(!recovery_successs){
        dataFileStream__.open(pipe_name.c_str(),ios_base::out);
        if(!dataFileStream__.good()) throw ios_base::failure("Cannot open file "+pipe_name );
    }
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

void CharacterOutputWriter :: initWrite()
{
    if(!header_sent){
        writeHeader(dataFileStream__,table__, additional_header__);
        header_sent = true;
    }
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

void CharacterOutputWriter :: processRecordBegin(int recordIdx, NSec recordTime) 
{
    char timestamp[32];
    // GetTimestamp(timestamp, tbl_ref.LastRecordTime); 
    CharacterOutputWriter::GetTimestamp(timestamp, recordTime);
    // Print the timestamp for the record followed by the values 
    // for each column in the table. 

    dataFileStream__ << timestamp << this->seperator__ << recordIdx;
}

void CharacterOutputWriter :: processRecordEnd() 
{
    dataFileStream__ << endl;
    recordCount__ += 1;
}

void CharacterOutputWriter :: finishWrite() throw (StorageException)
{
    dataFileStream__.flush();
}

/**
 * Function to extract a sample for a particular field from a data record
 * and write it to an output file stream.
 *
 * @param fs:   Reference to the output file stream.
 * @param var:  Reference to the Field structure being extracted from data.
 * @param data: Address of the pointer to the memory where the data sample
 *              is stored.
 */
void CharacterOutputWriter :: storeDataSample (const Field& var, byte **data)
{
     uint4   unum = 0;
     uint2   unum2 = 0;
     int     num  = 0;
     string  str;
    
     switch (var.FieldType) 
     {
         case 7 : 
             // 2-byte final storage floating point - Tested with CR1000 data
             unum2 = (uint2) PBDeserialize (*data, 2);
             storeFloat(var, GetFinalStorageFloat(unum2));
             *data += 2;
             break;

         case 6 : 
             // 4-byte signed integer (MSB first) - Tested with CR1000 data
             num   = (int)PBDeserialize (*data, 4);
             storeInt(var, num);
             *data += 4;
             break;

         case 9 : 
             // 4-byte floating point (IEEE standard, MSB first) - Tested with 
             // CR1000 data
             num = PBDeserialize (*data, 4);
             storeFloat(var, intBitsToFloat(num));
             *data += 4;
             break;

         case 10 : 
             // Boolean value - Tested with CR1000 data
             unum = PBDeserialize (*data, 1);
             storeBool(var, unum & 0x80);
             *data += 1;
             break;

         case 16 : 
             // variable length null-terminated string of length n+1 - Tested 
             // with CR1000 data
             str = GetVarLenString (*data);
             storeString(var, str);
             *data += str.size() + 1;
             break;

         case 11 : 
             // fixed length string of lengh n, unused portion filled 
             // with spaces/null - Tested with CR1000 data
             str = GetFixedLenString (*data, var);
             storeString(var, str);
             *data += var.Dimension;
             break;

         case 1 : 
             // 1-byte uint
             unum = PBDeserialize (*data, 1);
             storeUint4(var, unum);
             *data += 1;
             break;
         case 2 : 
             // 2-byte unsigned integer (MSB first)
             unum = PBDeserialize (*data, 2);
             storeUint4(var, unum);
             *data += 2;
             break;
         case 3 : 
             // 4-byte unsigned integer (MSB first)
             unum = PBDeserialize (*data, 4);
             storeUint4(var, unum);
             *data += 4;
             break;
         case 4 : 
             // 1-byte signed integer
             num  = (int)PBDeserialize (*data, 1);
             storeInt(var, num);
             *data += 1;
             break;
         case 5 : 
             // 2-byte signed integer (MSB first)
             num = (int)PBDeserialize (*data, 2);
             storeInt(var, num);
             *data += 2;
             break;
         case 18 : 
             // 8-byte floating point (IEEE standard, MSB first)
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 8;
             break;
         case 15 : 
             // 3-byte final storage floating point
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 3;
             break;
         case 8 : 
             // 4-byte final storage floating point (CSI format)
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         case 17 : 
             // Byte of flags
             unum = PBDeserialize (*data, 1);
             storeUint4(var, unum);
             *data += 1;
             break;
         case 27 : 
             // Boolean value
             unum = PBDeserialize (*data, 1);
             storeBool(var, unum & 0x80);
             *data += 1;
             break;
         case 28 : 
             // Boolean value
             unum = PBDeserialize (*data, 1);
             storeBool(var, unum & 0x80);
             *data += 1;
             break;
         case 12 : 
             // 4-byte integer used for 1-sec resolution time
             unum = PBDeserialize (*data, 4);
             storeUint4(var, unum);
             *data += 4;
             break;
         case 13 : 
             // 6-byte unsigned integer, 10's of ms resolution
             // Read a ulong, then mask out last 2 bytes
             unum = PBDeserialize (*data, 4);
             storeUint4(var, unum);
             *data += 6;
             break;
         case 14 : 
             // 2 4-byte integers, nanosecond time resolution
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 8;
             break;
         case 19 : 
             // 2-byte integer (LSB first)
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 2;
             break;
         case 20 : 
             // 4-byte integer (LSB first)
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         case 21 : 
             // 2-byte unsigned integer (LSB first)
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 2;
             break;
         case 22 : 
             // 4-byte unsigned integer (LSB first)
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         case 24 : 
             // 4-byte floating point (IEEE format, LSB first)
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         case 25 : 
             // 8-byte floating point (IEEE format, LSB first)
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 8;
             break;
         case 23 : 
             // 2 longs (LSB first), seconds then nanoseconds
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 8;
             break;
         case 26 : 
             // 4-byte floating point value
             processUnimplemented(var);
             logUnimplementedDataError(var);
             *data += 4;
             break;
         default : 
             processUnimplemented(var);
             logUnimplementedDataError(var);
    }
    return;
}
/**
 * Function to print out an error message in the log file if an unsupported
 * data type was found while collecting data for a table.
 * 
 * @param var: Reference to the Field structure with the unimplemented data
 *             type.
 */
void CharacterOutputWriter :: logUnimplementedDataError (const Field& var)
{
    static vector<string> err_field_list(100);
    static string last_err_field_name;

    if (last_err_field_name == var.FieldName) {
        return;
    }

    if (!err_field_list.empty()) {
        vector<string>::iterator result = find (err_field_list.begin(), 
                err_field_list.end(), var.FieldName); 
        if ( (result != err_field_list.end()) || 
                !(var.FieldName.compare(err_field_list.back())) ) {
            return;
        }
    }

    stringstream msgstrm;
    msgstrm << "ERROR in decoding data values for Field \"" << var.FieldName 
            << "\" [" << getDataType (var) << "]" << endl;
    Category::getInstance("TableDataManager")
            .info(msgstrm.str());
    err_field_list.push_back(var.FieldName);
    last_err_field_name = var.FieldName;
    return;
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

void printHeaderLine(ostream& out, const char* prefix, 
        const vector<Field>& fieldList, int infoType)
{
    int dim;
    vector<Field>::const_iterator fieldItr;

    if ((fieldList.size() == 0) || ((infoType < 1) || (infoType > 3))) {
        return;
    }

    fieldItr = fieldList.begin();
    out << prefix;

    for (fieldItr = fieldList.begin(); fieldItr != fieldList.end(); 
            fieldItr++){
        if ( (fieldItr->Dimension > 1) && ( (fieldItr->FieldType != 11) &&
                (fieldItr->FieldType != 16) ) ) {
            for (dim = 1; dim <= (int)fieldItr->Dimension; dim++) {
               out << fieldItr->getProperty(infoType, dim);
            }
        }
        else {
           out << fieldItr->getProperty(infoType, 0);
        }
    }

   out << endl;
   return;
}

void writeHeader(ostream& out, const Table& table, const string& additional_header)
{
    const vector<Field>& fieldList = table.field_list;
    //////////////////////////////////////////////////////
    // Print the file header : File format type, Station
    // name, Datalogger type, serial number, OS Version,
    // Datalogger program name, Datalogger program 
    // signature and the table name
    //////////////////////////////////////////////////////
    const string table_name = table.TblName;
    out<<additional_header
        << table_name << "\",\"" 
        << PB5_APP_NAME << "-" 
        << PB5_APP_VERS << "\"" << endl;
    // Print field names
    printHeaderLine(out,"\"TIMESTAMP\",\"RECORD\",", fieldList, 1);

    // Print field unit
    printHeaderLine(out,"\"TS\",\"RN\",", fieldList, 2);

    // Print processing type for each field
    printHeaderLine(out, "\"\",\"\",", fieldList, 3);

    return;
}
/**
 * This function ASSUMES that the file file_name exist
 * The function in only used once, and only extracted as a separate funciton
 * for simpler implementation
 */
bool CharacterOutputWriter::fileValid(ifstream& file){
    ostringstream header_oss;
    ostringstream header_from_file;
    string valid_header;
    writeHeader(header_oss,table__,additional_header__);
    if(file.is_open()){
        // File exist and we can open. Attempt recovery
        valid_header = header_oss.str();
        if (valid_header.empty()) {
            // We cannot use header to verify file
            return false;
        }
        int line_count = count(valid_header.begin(), valid_header.end(), '\n');
        // If the string doesn't end with a newline, add 1 for the last line
        if (valid_header[valid_header.size() - 1] != '\n') {
            line_count++;
        }
        int count = 0;
        string line;
        ostringstream header_from_file;
        while (getline(file, line) && count < line_count) {
            header_from_file << line << endl;
            count++;
        }   
    }
    return header_from_file.str() == valid_header;
}
int readLastRecordNumber(ifstream& file){
    file.seekg(0, ios::end);
    streamoff fileSize = file.tellg();

    if (fileSize == 0) {
        cerr << "File is empty\n";
        return 1;
    }

    char ch;
    // Move backwards until newline (or beginning of file)
    for (long long pos = fileSize - 1; pos >= 0; --pos) {
        file.seekg(pos);
        file.get(ch);
        if (ch == '\n' && pos != fileSize - 1) {
            break;
        }
    }

    // Now read the last line
    string lastLine;
    getline(file, lastLine);

    // --- Parse line
    stringstream ss(lastLine);
    string token;
    int colIdx = 0;
    int recordNum = -1;

    while (getline(ss, token, ',')) {
        if (colIdx == 1) { // second column
            istringstream iss(token);
            iss>>recordNum;
            break;
        }
        ++colIdx;
    }
    if(recordNum==-1){
        cout<<"Program recovery attempted, but could not find last record number\n";
        cout<<"Program will fetch only new data from this point onwards\n";
    }

    return recordNum;
}