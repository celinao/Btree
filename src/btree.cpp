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
	leafOccupancy = INTARRAYLEAFSIZE; // Not sure of the purpose of either of these
	nodeOccupancy = INTARRAYNONLEAFSIZE;
	this->scanExecuting = false; 

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
		metaInfo->rootIsLeaf = true;

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
		metaInfo->rootIsLeaf = true;

		// UnPinPage once done 
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);

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
			// std::cout << "_______________________" << std::endl;
			// std::cout << "Print Tree: " << std::endl;
			// printTree(rootPageNum, false); 
			bufMgr->flushFile(file);
			std::cout << "Finished Flushing" << std::endl;
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
	// std::cout << "_________________________________________" << std::endl;

	// Get headerPage 
	Page *headerPage; 
	headerPageNum = file->getFirstPageNo();
	bufMgr->readPage(file, headerPageNum, headerPage);
	IndexMetaInfo *metaInfo = (IndexMetaInfo *) headerPage;

	// Check if root is a leaf or internal node 
	PageKeyPair<int> pageKey; 
	if(metaInfo->rootIsLeaf){
		
		// Root is a leaf node 
		pageKey = insertToLeaf(key, rid, rootPageNum); 

		// Check if root was split and needs to change into an internal node 
		if(pageKey.pageNo != 0){
			// std::cout << "Updating Root: " << std::endl;
			
			// Create a new internal node for the new root 
			Page* newRootPage; 
			PageId newRootNo; 
			bufMgr->allocPage(file, newRootNo, newRootPage);
			NonLeafNodeInt *newRootNode = (NonLeafNodeInt *)newRootPage;
			
			// Initiialize new Root 
			newRootNode->keyArray[0] = pageKey.key;
			newRootNode->pageNoArray[0] = rootPageNum;
			newRootNode->pageNoArray[1] = pageKey.pageNo;
			newRootNode->level = 1; 

			// Update header page 
			metaInfo->rootPageNo = newRootNo; 
			metaInfo->rootIsLeaf = false;
			rootPageNum = newRootNo;

			bufMgr->unPinPage(file, newRootNo, true);
		}
	}else{
		// std::cout << "Insert to Internal: " << std::endl;
		// Create Root node 
		Page *rootPage; 
		bufMgr->readPage(file, rootPageNum, rootPage);
		NonLeafNodeInt *rootNode = (NonLeafNodeInt *) rootPage;
		bufMgr->unPinPage(file, rootPageNum, false); 

		insertLeafHelper(key, rootPageNum, rid, rootNode->level);
	}	

	// Unpin Header page
	bufMgr->unPinPage(file, headerPageNum, true);
}

PageKeyPair<int> BTreeIndex::insertToLeaf(const void *key, const RecordId rid, PageId pageNo) {
	// std::cout << "InsertToLeaf " << *(int*)key << " " << rid.page_number << std::endl;
	
	// Read Node that is being inserted to 
	Page* page;
	bufMgr->readPage(file, pageNo, page);
	LeafNodeInt* node = (LeafNodeInt*)page;

	// Check if leaf node is full and keep track of leaf size
	bool full = true; 
	int size = -1; 
	for (int i = 0; i < INTARRAYLEAFSIZE+1; i++) {
		if (node->ridArray[i].page_number == 0 ) {
			size = i;
			full = false;
			break;
		}
	}

	// Find position for insertion in keyArray
	int pos = 0; 
	for (int i = 0; i < size+1; i++) {
		if (*(int*)key < node->keyArray[i]) {
			pos = i;
			break;
		}else{
			pos = i;
		}
	}

	// if node is not full
	if (full == false) {
		// shifting keys and rids to right to make space to insert
		for (int i = size; i > pos; i--) {
			node->keyArray[i] = node->keyArray[i-1];
			node->ridArray[i] = node->ridArray[i-1];
		}

		// Add new record 
		node->keyArray[pos] = *(int*)key;
		node->ridArray[pos] = rid;

		bufMgr->unPinPage(this->file, pageNo, true);		
	}
	else {
		// Node is full and must be split 
		PageKeyPair<int> pageKey = splitLeaf(pageNo);

		// Check if key belongs in left or right split node
		// No need to check return of insertToLeaf since the node was just split and
		// therefore not full. 
		if(pageKey.key < *(int*)key){
			insertToLeaf(key, rid, pageKey.pageNo);
		}else{
			insertToLeaf(key, rid, pageNo); 
		}

		// Unpin pages and return pageKey to be added internally 
		bufMgr->unPinPage(this->file, pageNo, true);
		return pageKey; 
	}

	// Return empty pageKey if no splitting necessary 
	PageKeyPair<int> pageKey; 
	pageKey.pageNo = 0; 
	return pageKey;
}

void BTreeIndex::updateRootNode(PageKeyPair<int> pageKey){
	// std::cout << "updateRootNode" << std::endl;

	// Get headerPage 
	Page *headerPage; 
	headerPageNum = file->getFirstPageNo();
	bufMgr->readPage(file, headerPageNum, headerPage);
	IndexMetaInfo *metaInfo = (IndexMetaInfo *) headerPage;

	// Create a new internal node for the new root 
	Page* newRootPage; 
	PageId newRootNo; 
	bufMgr->allocPage(file, newRootNo, newRootPage);
	NonLeafNodeInt *newRootNode = (NonLeafNodeInt *)newRootPage;

	// Get Old Root Page  
	Page *oldRootPage; 
	bufMgr->readPage(file, rootPageNum, oldRootPage);

	// Update Level 
	NonLeafNodeInt *oldRootNode = (NonLeafNodeInt *)oldRootPage; 
	newRootNode->level = oldRootNode->level+1; 
	
	bufMgr->unPinPage(file, rootPageNum, false);
	
	// Initiialize new Root 
	newRootNode->keyArray[0] = pageKey.key;
	newRootNode->pageNoArray[0] = rootPageNum;
	newRootNode->pageNoArray[1] = pageKey.pageNo;

	// Update header page 
	metaInfo->rootPageNo = newRootNo; 
	metaInfo->rootIsLeaf = false;
	rootPageNum = newRootNo;

	bufMgr->unPinPage(file, headerPageNum, true);  
	bufMgr->unPinPage(file, newRootNo, true);
}

PageId BTreeIndex::findParentNode(PageKeyPair<int> pageKey, PageId pageNo, int level){
	// std::cout << "findParentNode()" << std::endl; 

	// Create root node 
	Page* page; 
	bufMgr->readPage(file, pageNo, page); 
	NonLeafNodeInt* node = (NonLeafNodeInt*)page; 

	// Find position to be inserted
	int i = 0; 
	while(node->pageNoArray[i+1] != 0 && i != INTARRAYNONLEAFSIZE+1){
		if(pageKey.key < node->keyArray[i]){
			break; 
		}
		i++; 
	}

	bufMgr->unPinPage(file, pageNo, false);

	if(node->level == level){
		return pageNo; 
	}else{
		return findParentNode(pageKey, node->pageNoArray[i], level);
	}
}

void BTreeIndex::insertToNonLeaf(PageKeyPair<int> pageKey, PageId pageNo) {
	// std::cout << "insertToNonLeaf()" << std::endl; 

	// Create internal node 
	Page* page;
	bufMgr->readPage(file, pageNo, page);
	NonLeafNodeInt* node = (NonLeafNodeInt*)page;

	// Check if leaf node is full and keep track of leaf size
	bool full = true; 
	int size = -1; 
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
		size = i; 
		if(node->pageNoArray[i+1] == 0){
			full = false;
			size = i+1;
			break;
		}
	}

	// Find position for insertion in keyArray
	int pos = -1; 
	for (int i = 0; i < size+1; i++) {
		pos = i;
		if (pageKey.key < node->keyArray[i]) {
			pos = i;
			break;
		}
	}

	// if node is not full
	if (full == false) {
		// shifting keys and rids to right to make space to insert
		for (int i = size; i > pos; i--) {
			node->keyArray[i] = node->keyArray[i-1];
			node->pageNoArray[i+1] = node->pageNoArray[i];
		}

		// Add pointer
		node->keyArray[pos] = pageKey.key;
		node->pageNoArray[pos+1] = pageKey.pageNo;
	}
	else {
		PageKeyPair<int> pushUp = splitNonLeaf(pageNo, pageKey); 

		if(pageNo == rootPageNum){
			updateRootNode(pushUp);
		}else{
			// WRONG
			PageId parentId = findParentNode(pushUp, rootPageNum, node->level+1);
			insertToNonLeaf(pushUp, parentId); 
		}
	}

	bufMgr->unPinPage(file, pageNo, true);
}

PageKeyPair<int> BTreeIndex::splitNonLeaf(PageId pageNo, PageKeyPair<int> newPageKey){
	// std::cout << "splitNonLeaf() " << std::endl; 

	// Read node to be split 
	Page* page; 
	bufMgr->readPage(file, pageNo, page);
	NonLeafNodeInt* node = (NonLeafNodeInt*)page; 

	// Create new Node (will be inserted to the left of node)
	Page* newPage; 
	PageId newPageNo; 
	bufMgr->allocPage(file, newPageNo, newPage);
	NonLeafNodeInt *newNode = (NonLeafNodeInt*)newPage; 
	// std::cout << "Setting newNode level" << node->level << std::endl; 
	newNode->level = node->level; 
	
	int mid = INTARRAYNONLEAFSIZE/2;

	// PageKey to be pushed to parent 
	PageKeyPair<int> pageKey; 
	pageKey.pageNo = newPageNo;
	pageKey.key = node->keyArray[mid];

	// Fill Arrays 	
	for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
		if(i < mid-1){
			// Add 
			newNode->keyArray[i] = node->keyArray[i+mid+1];
			newNode->pageNoArray[i] = node->pageNoArray[i+mid+1];

			// Remove 
			node->keyArray[i+mid+1] = 0; 
			node->pageNoArray[i+mid+1] = 0; 
		}else if(i == mid-1){
			// Add 
			newNode->keyArray[i] = newPageKey.key; 
			newNode->pageNoArray[i] = node->pageNoArray[i+mid+1];
			newNode->pageNoArray[i+1] = newPageKey.pageNo; 

			// Remove
			node->pageNoArray[i+mid+1] = 0;
			node->keyArray[i+1] = 0; 	
		}
	}

	// Unpin Pages
	bufMgr->unPinPage(file, pageNo, true);
	bufMgr->unPinPage(file, newPageNo, true);

	return pageKey; 
}

PageKeyPair<int> BTreeIndex::splitLeaf(PageId pageNo){
	// std::cout << "splitLeaf()" << std::endl;

	// Read node to be split 
	Page* page;
	bufMgr->readPage(file, pageNo, page);
	LeafNodeInt* node = (LeafNodeInt*)page;

	// Create new Node (will be inserted to the left of node) 
	Page* newPage; 
	PageId newPageNo; 
	bufMgr->allocPage(file, newPageNo, newPage);
	LeafNodeInt *newNode = (LeafNodeInt *) newPage;

	// insert newNode into linked list 
	newNode->rightSibPageNo = node->rightSibPageNo; 
	node->rightSibPageNo = newPageNo;

	// Populate new page with last half of old node records 
	int idx = 0; 
	for(int i = INTARRAYLEAFSIZE/2; i < INTARRAYLEAFSIZE; i++){
		// Add key and rid to newNode
		newNode->keyArray[idx] = node->keyArray[i]; 
		newNode->ridArray[idx] = node->ridArray[i];

		// Remove key and rid from node
		node->keyArray[i] = 0;
		node->ridArray[i].page_number = 0;
		idx++; 
	}

	// unpin old and new nodes
	bufMgr->unPinPage(file, pageNo, true);
	bufMgr->unPinPage(file, newPageNo, true);

	// return the new pageNo and key to be inserted to parent node 
	PageKeyPair<int> pageKey; 
	pageKey.set(newPageNo, newNode->keyArray[0]);

	return pageKey;
}

void BTreeIndex::insertLeafHelper(const void *key, PageId pageNo, const RecordId rid, int level){
	// std::cout << "<< InsertLeafHelper" << std::endl;
	
	// Create internal node 
	Page* page; 
	bufMgr->readPage(file, pageNo, page);
	NonLeafNodeInt *node = (NonLeafNodeInt *)page;

	// Check every key in array for insertion position 
	PageKeyPair<int> pageKey; 
	for(int i = 0; i < INTARRAYNONLEAFSIZE+1; i++){
		
		// insert if key is less than node's key, 0 (end of partially filled array), INTARRAYNONLEAFSIZE (end of full array)
		if(*(int *)key < node->keyArray[i] || 0 == node->pageNoArray[i+1] || i == INTARRAYNONLEAFSIZE){
			
			// Insert into leaf
			if(node->level == 1){

				// Insert Page
				pageKey = insertToLeaf(key, rid, node->pageNoArray[i]); 
				
				// Check if node split and needs to be updated
				if(pageKey.pageNo != 0){
					insertToNonLeaf(pageKey, pageNo); 
				}
				break;
			}else if(node->level == level){
				insertLeafHelper(key, node->pageNoArray[i], rid, level-1);
			}
			break;
		}
	}
	
	bufMgr->unPinPage(file, pageNo, false);
}

void BTreeIndex::printTree(PageId pageNo, bool leaf){
	std::cout << "Page Number: " << pageNo << std::endl;

	Page* headerPage; 
	bufMgr->readPage(file, headerPageNum, headerPage); 
	IndexMetaInfo* metaInfo = (IndexMetaInfo*)headerPage; 

	if(metaInfo->rootIsLeaf){
		printNode(pageNo);
		bufMgr->unPinPage(file, headerPageNum, false);
	}else{
		bufMgr->unPinPage(file, headerPageNum, false);

		Page* page; 
		bufMgr->readPage(file, pageNo, page); 
		NonLeafNodeInt* node = (NonLeafNodeInt*)page; 

		int size = 0; 
		for (int i = 0; i < INTARRAYNONLEAFSIZE+1; i++) {
			size = i; 
			if(node->pageNoArray[i+1] == 0){
				size = i; 
				break;
			}
		}

		for (int i = 0; i < size+1; i++) {
			if(node->level == 1){
				std::cout << "." << i << "." << node->pageNoArray[i] << "." << std::endl;
				if(i == 0){
					printNode(node->pageNoArray[i]);
				}else{
					std::cout << "[" << i << "]: " << node->keyArray[i-1] << std::endl;
					printNode(node->pageNoArray[i]);
				}
			}else{
				if(node->pageNoArray[i] == 0){
					break;
				}else if(i == 0){
					std::cout << pageNo << " Level: " << node->level << std::endl; 
					printTree(node->pageNoArray[i], false); 
				}else{
					std::cout << pageNo << " Level: " << node->level << " - Key: " << node->keyArray[i-1] << std::endl;
					printTree(node->pageNoArray[i], false);
				}
				
			}
		}
		bufMgr->unPinPage(file, pageNo, false);
	}
}

void BTreeIndex::printNode(PageId pageNo){
	Page* page;
	bufMgr->readPage(this->file, pageNo, page);
	LeafNodeInt* node = (LeafNodeInt*)page;

	int size = -1; // if not, keep track of node's size
	
	for (int i = 0; i < INTARRAYLEAFSIZE+1; i++) {
		if(node->ridArray[i].page_number == 0){
			size = i;
			break;
		}
	}

	std::cout << "     Printing Node " << size << std::endl; 
	for (int i = 0; i < 5; i++) {
		std::cout << "     [" << i << "]: " << node->keyArray[i] << "." << node->ridArray[i].page_number << std::endl;
	}
	std::cout << "     ..." << std::endl;
	bufMgr->unPinPage(file, pageNo, false);
}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	std::cout << "___________________Start Scan__________________" << std::endl;
	if (lowOpParm != GT && lowOpParm != GTE) {
		throw BadOpcodesException();
	}

	if (highOpParm != LT && highOpParm != LTE) {
		throw BadOpcodesException();
	}

	if (this->scanExecuting) {
		std::cout << (this->scanExecuting == true) << std::endl;
		this->endScan();
	}
	
	if (*(int*)lowValParm > *(int*)highValParm) {
		throw BadScanrangeException();
	}
	

	this->scanExecuting = true;
	this->lowValInt = *(int*)lowValParm;
    this->highValInt = *(int*)highValParm;
    this->lowOp = lowOpParm;
    this->highOp = highOpParm;

	// Get headerPage 
	Page *headerPage; 
	headerPageNum = file->getFirstPageNo();
	bufMgr->readPage(file, headerPageNum, headerPage);
	IndexMetaInfo *metaInfo = (IndexMetaInfo *) headerPage;
	
	if(metaInfo->rootIsLeaf){
		std::cout << "____rootIsLeaf FUNCTION NOT WRITTEN" << std::endl;
		// this->currentPageNum = this->rootPageNum;
		// bufMgr->readPage(file, currentPageNum, currentPageData); 
     	// LeafNodeInt* root = (LeafNodeInt*)currentPageData; 

		// for(int i = 0; i < INTARRAYLEAFSIZE; i++){
		// 	if (this->lowOp == GT && root->keyArray[i] > this->lowValInt) {
		// 		nextEntry = i; 
		// 		break;
		// 	} else if (this->lowOp == GTE && root->keyArray[i] >= this->lowValInt) {
		// 		nextEntry = i; 
		// 		break;
		// 	} 
		// }
		return; 
	}

	bufMgr->unPinPage(file, headerPageNum, false); 

	Page *page; 
	bufMgr->readPage(file, rootPageNum, page);
	NonLeafNodeInt* node = (NonLeafNodeInt*)page;
	std::cout << "Page Number: " << rootPageNum << " level: " << node->level << std::endl; 
	bufMgr->unPinPage(file, rootPageNum, false);

	scanHelper(rootPageNum);
}


void BTreeIndex::scanHelper(PageId pageNo) {
	
	// Creates Internal node being checked 
	Page *page; 
	bufMgr->readPage(this->file, pageNo, page);
	NonLeafNodeInt* node = (NonLeafNodeInt*)page;

	std::cout << "scanHelper -- Page Number: " << pageNo << " level: " << node->level << std::endl; 
	
	bufMgr->unPinPage(file, pageNo, false); 

	// Find the position 
	int i;
	for (i = 0; i < INTARRAYNONLEAFSIZE+1; i++) {
		if (node->keyArray[i] > this->lowValInt ) {
			break;
		}else if( node->pageNoArray[i+1] == 0 ){
			break;
		}
	}

	if(node->level == 1){
		// Leaf Node found. Set global variables 
		bufMgr->readPage(file, node->pageNoArray[i], this->currentPageData); 
		this->currentPageNum = node->pageNoArray[i]; 

		LeafNodeInt *leafNode = (LeafNodeInt*)currentPageData; 
		
		for(int i = 0; i < INTARRAYLEAFSIZE; i++){
			// std::cout << "checking keys: " << i << "." << leafNode->keyArray[i] << std::endl;

			if (this->lowOp == GT && leafNode->keyArray[i] > this->lowValInt) {
				nextEntry = i; 
				break;
			} else if (this->lowOp == GTE && leafNode->keyArray[i] >= this->lowValInt) {
				nextEntry = i; 
				break;
			} 
		}

	}else{
		scanHelper(node->pageNoArray[i]);
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	std::cout << "scanNext()" << std::endl;
	if (this->scanExecuting == false){
	 	throw ScanNotInitializedException();
	}

    LeafNodeInt* node = (LeafNodeInt*) this->currentPageData;

	// End Scan or go to next node 
	if(this->nextEntry >= INTARRAYLEAFSIZE || node->ridArray[nextEntry].page_number == 0){

		if (node->rightSibPageNo == 0) {
			throw IndexScanCompletedException(); // if leaf is over
		} else {
			bufMgr->unPinPage(this->file, this->currentPageNum, false);

			this->nextEntry = 0; //reinitialize nextentry
			this->currentPageNum = node->rightSibPageNo;
			bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
			node = (LeafNodeInt*) this->currentPageData;
		}
	}

	std::cout << "GETTING ENTRY  " << nextEntry << ": " << node->keyArray[nextEntry] << "." << node->ridArray[nextEntry].page_number << std::endl;

	if (this->highOp == LT && node->keyArray[this->nextEntry] < this->highValInt) {
		outRid = node->ridArray[nextEntry];
	} else if (this->highOp == LTE && node->keyArray[this->nextEntry] <= this->highValInt) {
		outRid = node->ridArray[nextEntry];
	} else {throw IndexScanCompletedException();}

	this->nextEntry += 1; //update to next
	std::cout << "completed" << std::endl;
	return;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
	std::cout << "endscan()" << std::endl;
    if (this->scanExecuting == false){ // if no scan started, throw exception
	 	throw ScanNotInitializedException();
	}
	
	// unpin pages if pinned
	bufMgr->unPinPage(this->file, this->currentPageNum, false);
	scanExecuting = false; // signifies scan complete
	return;
}

}
