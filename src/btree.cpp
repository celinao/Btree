/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

using namespace std;
//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	// NOTE: I am unsure if we are supposed to call BlobFile::exists() in this function. 
	// I don't see a need for it since if the file does/doesn't exist an exception should be 
	// thrown when creating with constructor. However, a piazza post seems to indicate it should be???? 

	// Initialize variables
	// Assuming all inputs are integers (as specified in assignment document)
	bufMgr = bufMgrIn; 
	leafOccupancy = INTARRAYLEAFSIZE;
	nodeOccupancy = INTARRAYNONLEAFSIZE;
	this->attrByteOffset = attrByteOffset;
	attributeType = attrType;

	// Construct indexName and return via outIndexName (code from assignment doc)
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); 
	outIndexName = indexName;
	
	// Create BlobFile
	try{
		// File Exists: 
		
		// Create a disk image of the index file (doesn't create a new file)
		file = new BlobFile(indexName, false);
		
		// Get existing header & root pages
		Page *headerPage; 
		headerPageNum = file->getFirstPageNo();
		bufMgr->readPage(file, headerPageNum, headerPage);
		IndexMetaInfo *metaInfo = (IndexMetaInfo *) headerPage;
		rootPageNum = metaInfo->rootPageNo;

		// No need to check for correct MetaInfo and throw a BadIndexInfoException (piazza)

		// UnPinPage once done 
		bufMgr->unPinPage(file, headerPageNum, false);

	}catch(const FileNotFoundException &e){
		// File Does Not Exist: 

		// Create a disk image of the index file 
		file = new BlobFile(indexName, true);

		// Create root & header pages
		headerPageNum = 0;
		Page *headerPage;
		bufMgr->allocPage(file, headerPageNum, headerPage);

		rootPageNum = 1; 
		Page *rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);

		// Create root node and add right sibling 
		LeafNodeInt *rootNode = (LeafNodeInt *) rootPage;
		rootNode->rightSibPageNo = 0;

		// Adding Meta Information to headerPage
		IndexMetaInfo *metaInfo = (IndexMetaInfo *) headerPage;
		strcpy(metaInfo->relationName, relationName.c_str());
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;
		metaInfo->rootPageNo = rootPageNum;

		// UnPinPage once done 
		bufMgr->unPinPage(file, rootPageNum, true);
		bufMgr->unPinPage(file, headerPageNum, true);

		// Create new FileScan object
		FileScan fscan(relationName, bufMgr);
		try
		{
			// Get all tuples in relation. 
			RecordId scanRid;
			while(1)
			{
				// Find key
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();

				// Insert into BTree
				insertEntry(record+attrByteOffset, scanRid); 
			}
		}
		catch(const EndOfFileException &e)
		{
			std::cout << "Read all records" << std::endl;
			bufMgr->flushFile(file);
		}
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// Stop scanning 
	try{
		endScan();
	}catch(const ScanNotInitializedException &e){
		// No Scan has been initialized. Catching Error and closing BTreeIndex
	}

	// Remove file 
	bufMgr->flushFile(BTreeIndex::file);
	delete file;
	file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// Inserts a new entry into index using the pair <key, rid>
	// key = Pointer to the value we want to insert 
	// corresponding record id of tuple in base realtion 
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
