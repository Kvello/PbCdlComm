/** 
 * @file pb5_data.cpp
 * Implements functionalities for data storage and metadata management.
 */

/*****************************************************
 * RCS INFORMATION:
 *   $RCSfile: pb5_data.cpp,v $
 *   $Revision: 1.2 $
 *   $Author: choudhury $
 *   $Locker:  $
 *   $Date: 2008/06/09 18:55:19 $
 *   $State: Exp $
 *****************************************************/

#include <string>
#include <algorithm>
#include <iterator>
#include <fstream>
#include <sstream>
#include <string>
#include <cmath>
#include <time.h>
#include <unistd.h>
#include <libxml2/libxml/parser.h>
#include <libxml2/libxml/tree.h>
#include <log4cpp/Category.hh>
#include "pb5.h"
#include "utils.h"
using namespace std;
using namespace log4cpp;

bool NSec::operator<(const NSec& other) const{
    if(this->sec < other.sec){
        return true;
    }
    if(this->sec > other.sec){
        return false;
    }
    if(this->nsec < other.nsec){
        return true;
    }
    else return false;
}
bool NSec::operator>(const NSec& other) const{
    return other < *this;
}
bool NSec::operator==(const NSec& other) const{
    return (this->sec==other.sec)&&(this->nsec==other.nsec);
}
/**
 * Operator += for NSec structure
 * 
 * @param timeval The operand
 */
void NSec :: operator+=(NSec& timeVal)
{
    this->sec += timeVal.sec;
    uint4 tmp = this->nsec + timeVal.nsec;
    if (tmp >= 1E9) {
        this->sec += 1;
        this->nsec = tmp - (uint4) 1E9;
    }
}

int nseccmp(const NSec& t1, const NSec& t2)
{
    if (t1.sec < t2.sec) {
        return -1;
    }
    else if (t1.sec > t2.sec) {
        return 1;
    }
    else {
        if (t1.nsec == t2.nsec) {
            return 0;
        }
        else if (t1.nsec < t2.nsec) {
            return -1;
        }
        else {
            return 1;
        }
    }
}



/**
 * Function to convert a bit pattern to the equivalent floating
 * point number following IEEE-754 standard specifications.
 * The most significant bit -> sign, next 8 bits is the exponent
 * and the rest is the mantissa.
 * http://java.sun.com/j2se/1.4.2/docs/api/java/lang/Float.html
 * is a good place for more information about such functions.
 *
 * @param bits: Input bit pattern represented as a unsigned integer.
 * @return float: equivalent floating point number.
 */
float intBitsToFloat (uint4 bits)
{
    int s = ((bits >> 31) == 0) ? 1 : -1; 
    int e = ((bits >> 23) & 0xff);
    int m = (e == 0) ? 
        (bits & 0x7fffff) << 1 :
        (bits & 0x7fffff) | 0x800000;
    return (s*m*pow(2.0, (e-150)));
}



/**
 * Constructor for the TableDataManager class. 
 *
 * @param data_opt: Reference to the DataOutputConfig structure that contains
 *                  various information for generating file headers. 
 */
TableDataManager :: TableDataManager (const string separator,const string working_path): 
separator(separator), working_path__(working_path)
{ 
    stringstream header;
    header << "\"TOA5\",\"" << dataOutputConfig__.StationName << "\",\""
                                     << dataOutputConfig__.LoggerType << "\",\""
                                     << dataLoggerProgStats__.SerialNbr << "\",\"" 
                                     << dataLoggerProgStats__.OSVer << "\",\""
                                     << dataLoggerProgStats__.ProgName << "\",\"" 
                                     << dataLoggerProgStats__.ProgSig << "\",\"" ;
    additional_header__ = header.str();
}



const DLProgStats& TableDataManager :: getProgStats() const
{
    return dataLoggerProgStats__;
}

void TableDataManager :: setProgStats(DLProgStats& progStats)
{
    dataLoggerProgStats__ = progStats;
    return; 
}

const DataOutputConfig& TableDataManager :: getDataOutputConfig() const
{
    return dataOutputConfig__;
}

void TableDataManager :: setDataOutputConfig(const DataOutputConfig& dataOpt)
{
    dataOutputConfig__ = dataOpt;

    return; 
}

/**
 * Destructor for the TableDataManager class.
 * For each table structure build from the table definition file, it
 * stores the currently available information for initiating the next
 * data collection and next data file creation.
 */
TableDataManager :: ~TableDataManager ()
{ 
    Category::getInstance("TableDataManager")
             .debug("Saving history for all collected tables.");
    for(map<string,TableDataWriter*>::iterator it = tblDataWriters.begin();
     it!=tblDataWriters.end();it++){
        delete it->second;
     }
}

/**
 * Function to construct Table structure information from the logger
 * It builds and maintains the table information in a vector of Table
 * structures in the class.
 *
 * @return SUCCESS | FAILURE
 */
int TableDataManager :: BuildTDF(istream& table_structure)
{
    tableList__.clear();
    table_structure.seekg (0, ios_base::end);
    int len = table_structure.tellg ();
    table_structure.seekg (0, ios_base::beg);

        if (len == 0) {
        Category::getInstance("TableDataManager")
                 .error("No data available for parsing Table definitions");
        return FAILURE;
    }

    byte* tdf_data = NULL;

    try {
        tdf_data = new byte[len];
    } 
    catch(bad_alloc& bae) {
        Category::getInstance("TableDataManager")
                 .error("Failed to allocate buffer to parsing Table definitions");
        return FAILURE;
    }

    table_structure.read ((char *)tdf_data, len);


    byte* ptr    = tdf_data;
    byte* endptr = tdf_data+len;
    int   table_num = 1; 

    fslVersion__ = *ptr++;
    
    while (ptr < endptr) {
        int nbytes = readTableDefinition (table_num, ptr, endptr);
        if (nbytes == -1) {
            Category::getInstance("TableDataManager")
                     .error("Failed to parse table definitions from stream");
            tableList__.clear();
            delete [] tdf_data;
            Category::getInstance("TableDataManager")
                     .info("Removing invalid table definition in stream");
            return FAILURE;
            break;
        }
        ptr += nbytes;
        table_num++;
    }    
    for(vector<Table>::iterator tbl_it=tableList__.begin();tbl_it!=tableList__.end(); tbl_it++){
        string table_name = tbl_it->TblName;
        string file_name = working_path__ +"/"+ table_name + ".raw";
        TableDataWriter* writer(new CharacterOutputWriter(file_name, separator,*tbl_it,additional_header__));
        tblDataWriters.insert(make_pair(table_name, writer));
    }
   
    return SUCCESS;
}



/** 
 * Read the structure of a table. A successful call returns the 
 * length of the segment for this table in the table definition file.
 *
 * @param table_num: Index for table number, beginning from 1. 
 * @param byte_ptr:  Pointer to the memory location to start reading next
 *                   table structure information.
 * @param endptr:    Pointer to the end of the buffer containing the 
 *                   table definition information. A read shouldn't be
 *                   performed beyond this limit.
 * @return Returns size of the table structure read in bytes. If the 
 * end of the byte stream was reached before completing the parsing,
 * -1 is returned.
 */
int TableDataManager :: readTableDefinition (int table_num, byte *byte_ptr, byte *endptr)
{
    Table tbl;
    byte *ptr = byte_ptr;

    if (ptr > endptr) return (-1);
    tbl.TblName = GetVarLenString (ptr);
    ptr += tbl.TblName.size ()+1;

    if (ptr > endptr) return -1;
    tbl.TblSize = PBDeserialize (ptr, 4);
    ptr += 4;

    if (ptr > endptr) return -1;
    tbl.TimeType = *ptr++;

    if (ptr > endptr) return -1;
    tbl.TblTimeInfo.sec = PBDeserialize (ptr, 4);
    ptr += 4;

    if (ptr > endptr) return -1;
    tbl.TblTimeInfo.nsec = PBDeserialize (ptr, 4);
    ptr += 4;

    if (ptr > endptr) return -1;
    tbl.TblTimeInterval.sec = PBDeserialize (ptr, 4);
    ptr += 4;

    if (ptr > endptr) return -1;
    tbl.TblTimeInterval.nsec = PBDeserialize (ptr, 4);
    ptr += 4;

    int nbytes = readFieldList (ptr, endptr, tbl);
    if (nbytes == -1) {
        return -1;
    }
    ptr += nbytes;
    
    int table_len = ptr - byte_ptr;
    tbl.TblSignature = CalcSig (byte_ptr, (uint4)(table_len), 0xaaaa);
    tbl.TblNum = table_num;    

    stringstream logmsg;

    if (tbl.TblName.size() > 0) {

        vector<Table>::const_iterator tblItr;
        bool dupFound(false);

        // Usually very few tables are stored on the logger, doesn't 
        // hurt to loop over.

        for (tblItr = tableList__.begin(); tblItr != tableList__.end();
                tblItr++) {
            if (tblItr->TblName.compare(tbl.TblName) == 0) {
                logmsg << "Duplicate entry found for [" << tbl.TblName 
                       << "] in table definitions file, ignoring later";
                Category::getInstance("TableDataManager")
                         .debug(logmsg.str());
                logmsg.str("");
                dupFound = true;
            }              
        }
       
        if (true != dupFound) {
            tableList__.push_back(tbl);
        }
    }
    else {
        stringstream logmsg;
        logmsg << "Ignoring " << table_len 
               << "-byte long entry in table definitions file with empty name string";
        Category::getInstance("TableDataManager")
                 .debug(logmsg.str());
    }
    return (table_len);
}

/**
 *  Reads in the list of fields from the bytestream. The field list
 *  is attached to a Table structure, also passed as the argument.
 *
 * @param byte_ptr:  Pointer to the memory location to start reading 
 *                   field list information.
 * @param endptr:    Pointer to the end of the buffer containing the 
 *                   table definition information. A read shouldn't be
 *                   performed beyond this limit.
 * @return Returns size of the field list read in bytes. If the end of
 * the buffer was reached before completing the parsing, -1 is returned.
 */

int TableDataManager :: readFieldList (byte *byte_ptr, byte *endptr, Table& Tbl)
{
    Field var;
    byte next_num;
    byte *ptr = byte_ptr;

    do
    {
        if (ptr > endptr) return -1;
        var.FieldType = *ptr++;
        var.FieldType &= 0x7f;

        if (ptr > endptr) return -1;
        var.FieldName = GetVarLenString (ptr);
        ptr += var.FieldName.size ()+1;
        ptr++;  // Get past the null byte as namelist terminator

        if (ptr > endptr) return -1;
        var.Processing = GetVarLenString (ptr);
        ptr += var.Processing.size ()+1;

        if (ptr > endptr) return -1;
        var.Unit = GetVarLenString (ptr);
        ptr += var.Unit.size ()+1;

        if (ptr > endptr) return -1;
        var.Description = GetVarLenString (ptr);
        ptr += var.Description.size ()+1;
        
        if (ptr > endptr) return -1;
        var.BegIdx  = PBDeserialize (ptr, 4);
        ptr += 4;

        if (ptr > endptr) return -1;
        var.Dimension = PBDeserialize (ptr, 4);
        ptr += 4;

        // Commenting out the if statement based on Dennis Oracheski's suggestion
        // if (var.Dimension > 1) {
        
            while (ptr < endptr) {

                uint4 num = PBDeserialize (ptr, 4);
                ptr += 4;

                if (num != 0x00) {
                    var.SubDim.push_back(num);
                }
                else{
                    break;
                }
            }
        // }
        // else {
            // ptr += 4;
        // }
        Tbl.field_list.push_back (var);
        next_num = PBDeserialize (ptr, 1);

    } while (next_num != 0);
    
    ptr += 1;
    return (ptr - byte_ptr);
}

/**
 * Function to dump the table definition format information into a 
 * XML file. 
 *
 * @param xmlDumpFile: Path of the file to write the table structure
 *                     information. 
 * @return SUCCESS | FAILURE;
 */
int TableDataManager :: xmlDumpTDF (char* xmlDumpFile)
{
    xmlDocPtr  doc;
    xmlNodePtr root;
    int        nbytes;
    vector<Table>::iterator tbl_itr;

    // Create new XML document with version 1.0 and create a 
    // root node named "TDF"

    doc = xmlNewDoc ((const xmlChar *)"1.0");
    root = xmlNewNode (NULL, (const xmlChar *)"TDF");
    xmlDocSetRootElement (doc, root);

    // Write each table information to the XML file

    for (tbl_itr = tableList__.begin(); tbl_itr != tableList__.end(); 
            tbl_itr++) {
        writeTableToXml (root, *tbl_itr);
    }

    // Save the document tree and free used memory

    nbytes = xmlSaveFormatFile (xmlDumpFile, doc, 1);
    xmlFreeDoc (doc);

    if (nbytes == -1) {
        return FAILURE;
    }
    else {
        return SUCCESS;
    }
}

/**
 * This function writes the structure information for each table to a
 * XML document. The name, record size, signature and information about
 * each field in the table is added to the document tree.
 *
 * @param doc_root: Pointer to the root of the XML document.
 * @param tbl: Reference to the table structure being written out.
 */
// TODO Test the occasional segmentation fault occuring in this function.
void TableDataManager :: writeTableToXml (xmlNodePtr doc_root, Table& tbl)
{
    xmlNodePtr  table_node;
    char        buf[64];
    vector<Field>::iterator field_itr;

    // Create a node for each table and set the name attribute
    if (!tbl.TblName.size()) {
        return;
    }
    table_node = xmlNewChild (doc_root, NULL, (const xmlChar *)"TABLE", NULL);

    // Set various attributes for the table node
    
    xmlSetProp (table_node, (const xmlChar *)"Name", 
            (const xmlChar *)tbl.TblName.c_str() );

    sprintf (buf, "%d", tbl.TblSize);
    xmlSetProp (table_node, (const xmlChar *)"Table_Size", 
            (const xmlChar *)buf);

    sprintf (buf, "%d", getRecordSize (tbl)); 
    xmlSetProp (table_node, (const xmlChar *)"Record_Size", 
            (const xmlChar *)buf);

    sprintf (buf, "%d", tbl.TblSignature);
    xmlSetProp (table_node, (const xmlChar *)"Signature", 
            (const xmlChar *)buf);

    sprintf (buf, "%d", (int)tbl.TimeType);
    xmlSetProp (table_node, (const xmlChar *)"Time_Type", 
            (const xmlChar *)buf);

    sprintf (buf, "%d.%ds", tbl.TblTimeInterval.sec, tbl.TblTimeInterval.nsec);
    xmlSetProp (table_node, (const xmlChar *)"Time_Interval", 
            (const xmlChar *)buf);

    // Now add information about each field in the table

    for (field_itr = tbl.field_list.begin(); field_itr != tbl.field_list.end(); 
            field_itr++) {
        writeFieldToXml (table_node, *field_itr);
    }
    return;
}

/**
 * This function creates a "field" node under a "table" node in the 
 * XML document tree. Name, Unit, Processing, Data Type, Description
 * and Dimension information for each field is written out.
 *
 * @param table_node: xmlNodePtr to which the field information will be
 *                    attached.
 * @param var:        Reference to the field structure being written out.
 */
void TableDataManager :: writeFieldToXml (xmlNodePtr table_node, Field& var)
{
    xmlNodePtr field_node;
    char       buf[128];

    field_node = xmlNewChild (table_node, NULL, (const xmlChar *)"Field", 
            NULL);

    xmlSetProp (field_node, (const xmlChar *)"Name", 
            (const xmlChar *)var.FieldName.c_str());

    if (var.Unit.length() > 0) {
        xmlSetProp (field_node, (const xmlChar *)"Unit", 
                (const xmlChar *)var.Unit.c_str());
    }
    
    if (var.Processing.length() > 0) {
        xmlSetProp (field_node, (const xmlChar *)"Processing", 
                (const xmlChar *)var.Processing.c_str());
    }
    
    xmlSetProp (field_node, (const xmlChar *)"Type", 
            (const xmlChar *)getDataType (var));
    
    if (var.Description.length() > 0) {
        xmlSetProp (field_node, (const xmlChar *)"Description", 
                (const xmlChar *)var.Description.c_str());
    }
    
    sprintf (buf, "%d", var.Dimension);
    xmlSetProp (field_node, (const xmlChar *)"Dimension", 
            (const xmlChar *)buf);
    return;
}

/** 
 * Function to get record size for a particular table structure.
 * 
 * @param tbl: Reference to table structure whose size is being queried.
 * @return Record size for that input table, -1 if the table contains a
 *             variable length member.
 */
int TableDataManager :: getRecordSize (const Table& tbl) 
{
    int field_size;
    int RecSize = 0;
    vector<Field>::const_iterator field_itr;

    for (field_itr = tbl.field_list.begin(); field_itr != tbl.field_list.end();
            field_itr++) {
        field_size = getFieldSize (*field_itr);
        if (field_size > 0) {
            RecSize += field_size;
        }
        else {
            return -1;
        }
    }

    return RecSize;
}

void TableDataManager::initWrite(string table_name){


    tblDataWriters.at(table_name)->initWrite();
    
}
void TableDataManager::finishWrite(string table_name){
    tblDataWriters.at(table_name)->finishWrite();
}

int TableDataManager::getNextRecordNumber(const Table& tbl){
    return tbl.NextRecordNbr;
}

/**
 * Function to determine the maximum record size.
 */
int TableDataManager :: getMaxRecordSize() 
{
    vector<Table>::const_iterator tblItr;
    int maxTableSize = -1;
    int tableSize;

    for (tblItr = tableList__.begin(); tblItr != tableList__.end(); tblItr++) {
        tableSize = getRecordSize(*tblItr);
        maxTableSize = max(maxTableSize, tableSize);
    }
    return maxTableSize;        
}

/**
 * Function to obtain number of bytes allocated for a particular field in 
 * the record for a table.
 *
 * @param field: Referene to the Field structure being queried.
 * @return Field size (within record) in bytes, -1 if the field size is 
 *               variable of unknown.
 */
int TableDataManager :: getFieldSize (const Field& field)
{
    int field_type = (int) field.FieldType;
    int field_size = 0;

    switch (field_type) {
        case 1  :
            field_size = 1;
            break;
        case 2  :
            field_size = 2;
            break;
        case 3  :
            field_size = 4;
            break;
        case 4  :
            field_size = 1;
            break;
        case 5  :
            field_size = 2;
            break;
        case 6  :
            field_size = 4;
            break;
        case 7  :
            field_size = 2;
            break;
        case 8  :
            field_size = 4;
            break;
        case 9  :
            field_size = 4;
            break;
        case 10 :
            field_size = 1;
            break;
        case 11 :
            field_size = field.Dimension;
            break;
        case 12 :
            field_size = 4;
            break;
        case 13 :
            field_size = 6;
            break;
        case 14 :
            field_size = 8;
            break;
        case 15 :
            field_size = 3;
            break;
        case 16 :
            field_size = -1;
            break;
        case 17 :
            field_size = 1;
            break;
        case 18 :
            field_size = 8;
            break;
        case 19 :
            field_size = 2;
            break;
        case 20 :
            field_size = 4;
            break;
        case 21 :
            field_size = 2;
            break;
        case 22 :
            field_size = 4;
            break;
        case 23 :
            field_size = 8;
            break;
        case 24 :
            field_size = 4;
            break;
        case 25 :
            field_size = 8;
            break;
        case 27 :
            field_size = 2;
            break;
        case 28 :
            field_size = 4;
            break;
        default : 
            field_size = -1;
    }

    if (field_size > 0) {
        if (field.FieldType != 11) {
           field_size *= field.Dimension;
        } 
    }
    return field_size;
}



/**
 * Function to obtain a reference to a particular Table structure contained in
 * the TableDataManager class.
 * The table list stored within the TableDataManager class is searched for a table
 * with the given name. Throws exception if table couldn't be found.
 *
 * @param TableName: string containing the table name to search for.
 * @return Reference to the appropriate table structure.
 */
Table& TableDataManager :: getTableRef (const string& TableName) throw (invalid_argument)
{
    vector<Table>::iterator tbl_itr;
    int  idx = 0;
    
    for (tbl_itr = tableList__.begin (); tbl_itr != tableList__.end (); tbl_itr++) {
        if (tbl_itr->TblName == TableName) {
            return tableList__[idx];
        }
        idx++;
    }
    stringstream logmsg;
    logmsg << "Failed to find information about [" << TableName 
           << "] among table definition file entries";
    throw invalid_argument(logmsg.str());
}

NSec parseRecordTime(const byte* data)
{
    NSec recordTime;
    recordTime.sec = PBDeserialize (data, 4);
    recordTime.nsec = PBDeserialize (data+4, 4);
    return recordTime; 
}

/**
 * This function extracts a record from a byte stream for a Table structure and 
 * writes that to a file using the specified output stream.
 * In addition to extracting records and writing data to file, this function also
 * ensures splitting of files on hour boundaries.
 *
 * @param fs: Reference to output file stream.
 * @param tbl_ref: Reference to corresponding Table strucrure.
 * @param data: Address of the pointer to the beginning of the byte sequence.
 * @param rec_num: Number of the record to store in file.
 * @param file_span: Span of a datafile in seconds
 * @return SUCCESS | FAILURE (If the data file couldn't be opened).
 */ 
int TableDataManager :: storeRecord (Table& tbl_ref, byte **data, uint4 rec_num) throw (StorageException)
{
    vector<Field>           field_list = tbl_ref.field_list;
    vector<Field>::const_iterator start, end, itr;
    NSec recordTime;

    start = field_list.begin();
    end   = field_list.end();
    TableDataWriter* tblDataWriter = tblDataWriters.at(tbl_ref.TblName);

    try {
        // tbl_ref.LastRecordTime = parseRecordTime(*data);
        recordTime = parseRecordTime(*data);
        *data += 8;
    
        tblDataWriter->processRecordBegin(rec_num, recordTime);
        
        for (itr = start; itr < end; itr++) {
            if ((itr->FieldType == 11) || (itr->FieldType == 16)) {
                tblDataWriter->storeDataSample(*itr, data);
            }
            else {
                for (int dim = 0; dim < (int)itr->Dimension; dim++) {
                    tblDataWriter->storeDataSample(*itr, data);
                } 
            }
        }

        tblDataWriter->processRecordEnd();
               
        // Update state variables
        tbl_ref.LastRecordTime = recordTime;
    }       

    catch (...) {
        stringstream errormsg;
        char timestamp[64];
        CharacterOutputWriter::GetTimestamp(timestamp, recordTime);
        errormsg << "Failure in storing data record{\"id\":" 
                 << ", \"timestamp\":"
                 << timestamp << "}";
        throw StorageException(__FILE__, __LINE__, errormsg.str().c_str());
    } 
    return SUCCESS; 
}


