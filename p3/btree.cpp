/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG
#define EMPTY_SLOT 255
#define LEAF 1
#define NON_LEAF 0

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

    BTreeIndex::BTreeIndex(const std::string &relationName,
                           std::string &outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType) {

        std::ostringstream idxStr;
        idxStr << relationName << '.' << attrByteOffset;
        outIndexName = idxStr.str(); // indeName is the name of the index file


        Page *page;
        bufMgr = bufMgrIn;

        // If the index file exists, the file is opened
        try {

            file = new BlobFile(outIndexName, false);
            assert(file != NULL);
            bufMgr->readPage(file, 1, page);
            IndexMetaInfo *indexMetaInfo = reinterpret_cast<IndexMetaInfo *>(page);
            bufMgr->unPinPage(file, 1, false);

            if (relationName != indexMetaInfo->relationName || attrByteOffset != indexMetaInfo->
                    attrByteOffset || attrType != indexMetaInfo->attrType) {
                throw BadIndexInfoException(outIndexName);
            }

            rootPageNum = indexMetaInfo->rootPageNo;
        }

            // Else, a new index file is created.
        catch (FileNotFoundException e) {
            file = new BlobFile(outIndexName, true);


            PageId pid;
            IndexMetaInfo *meta;
            bufMgr->allocPage(file, pid, (Page *&) meta);
            strncpy((char *) (&(meta->relationName)), relationName.c_str(), 20);
            meta->relationName[19] = 0;
            meta->attrByteOffset = attrByteOffset;
            meta->attrType = attrType;
            PageIDPair rootResult = createNode(NON_LEAF);
            rootPageNum = rootResult.pageNo;
            NonLeafNodeInt *root = (NonLeafNodeInt *) (rootResult.page);
            root->level = 1;
            PageIDPair leafResult = createNode(LEAF);
            bufMgr->unPinPage(file, leafResult.pageNo, true);
            root->pageNoArray[0] = leafResult.pageNo;
            bufMgr->unPinPage(file, rootResult.pageNo, true);
            meta->rootPageNo = rootPageNum;
            bufMgr->unPinPage(file, pid, true);


            FileScan *newFileScan = new FileScan(relationName, bufMgrIn);

            try {
                while (1) {
                    RecordId outRid;
                    newFileScan->scanNext(outRid);
                    std::string record = newFileScan->getRecord();
                    const char *cstr = record.c_str();
                    insertEntry(cstr + attrByteOffset, outRid);
                }
            }
            catch (EndOfFileException e) {}
            delete newFileScan;
        }
    }

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

    BTreeIndex::~BTreeIndex() {
        bufMgr->flushFile(file);
        file->~File();
    }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        PageKeyPair<int> result = insertToNode(rootPageNum, key, rid);
        if (result.pageNo != (PageId) (EMPTY_SLOT)) {
            PageIDPair newRootPair = createNode(NON_LEAF);
            NonLeafNodeInt *newRoot = (NonLeafNodeInt *) (newRootPair.page);
            newRoot->level = 0;
            newRoot->keyArray[0] = result.key;
            newRoot->pageNoArray[0] = rootPageNum;
            newRoot->pageNoArray[1] = result.pageNo;
            bufMgr->unPinPage(file, newRootPair.pageNo, true);

            rootPageNum = newRootPair.pageNo;

            IndexMetaInfo *meta;
            bufMgr->readPage(file, 1, (Page *&) meta);
            meta->rootPageNo = rootPageNum;
            bufMgr->unPinPage(file, 1, true);
        }
    }



// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

    const void BTreeIndex::startScan(const void *lowValParm,
                                     const Operator lowOpParm,
                                     const void *highValParm,
                                     const Operator highOpParm) {
        lowValInt = *((int *) lowValParm);
        highValInt = *((int *) highValParm);
        lowOp = lowOpParm;
        highOp = highOpParm;

        NonLeafNodeInt *node;
        LeafNodeInt *leafNode;
        PageId pageNum = rootPageNum;

        bufMgr->readPage(file, pageNum, (Page *&) node);
        while (node->level == 0) {
            int idx = 0;
            while (idx < INTARRAYNONLEAFSIZE && node->keyArray[idx] < lowValInt &&
                   node->keyArray[idx] != EMPTY_SLOT)
                idx++;
            const PageId oldPageNum = pageNum;
            pageNum = node->pageNoArray[idx];
            bufMgr->unPinPage(file, oldPageNum, false);
            bufMgr->readPage(file, pageNum, (Page *&) node);
        }

        int idx = 0;
        while (idx < INTARRAYNONLEAFSIZE && node->keyArray[idx] < lowValInt && node->keyArray[idx] != EMPTY_SLOT)
            idx++;
        const PageId oldPageNum = pageNum;
        pageNum = node->pageNoArray[idx];
        bufMgr->unPinPage(file, oldPageNum, false);
        bufMgr->readPage(file, pageNum, (Page *&) leafNode);


        bool found = false;

        for (int idx = 0; idx < INTARRAYLEAFSIZE && leafNode->keyArray[idx] != EMPTY_SLOT; idx++) {
            const int key = leafNode->keyArray[idx];
            if (lowOpParm == GTE && highOpParm == LTE) {
                if (!(key >= lowValInt && key <= highValInt))
                    continue;
            } else if (lowOpParm == GT && highOpParm == LTE) {
                if (!(key > lowValInt && key <= highValInt))
                    continue;
            } else if (lowOpParm == GTE && highOpParm == LT) {
                if (!(key >= lowValInt && key < highValInt))
                    continue;
            } else {
                if (!(key > lowValInt && key < highValInt))
                    continue;
            }

            found = true;
            currentPageNum = pageNum;
            currentPageData = (Page *) leafNode;
            nextEntry = idx;
            scanExecuting = true;
            break;
        }

        if (!found) {
            scanExecuting = false;
            bufMgr->unPinPage(file, pageNum, false);
            throw NoSuchKeyFoundException();
        }
    }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

    const void BTreeIndex::scanNext(RecordId &outRid) {
        if (!scanExecuting) {
            throw ScanNotInitializedException();
        }

        LeafNodeInt *node = (LeafNodeInt *) currentPageData;

        if (nextEntry == INTARRAYLEAFSIZE || node->keyArray[nextEntry] == EMPTY_SLOT) {
            if (node->rightSibPageNo == (PageId) EMPTY_SLOT) {
                throw IndexScanCompletedException();
            }
            const PageId oldPageNum = currentPageNum;
            currentPageNum = node->rightSibPageNo;
            nextEntry = 0;
            bufMgr->unPinPage(file, oldPageNum, false);
            bufMgr->readPage(file, currentPageNum, (Page *&) currentPageData);
            node = (LeafNodeInt *) currentPageData;
        }

        const int key = node->keyArray[nextEntry];
        if (lowOp == GTE && highOp == LTE) {
            if (key >= lowValInt && key <= highValInt) {
                outRid = node->ridArray[nextEntry];
                nextEntry++;
            } else {
                throw IndexScanCompletedException();
            }
        } else if (lowOp == GT && highOp == LTE) {
            if (key > lowValInt && key <= highValInt) {
                outRid = node->ridArray[nextEntry];
                nextEntry++;
            } else {
                throw IndexScanCompletedException();
            }
        } else if (lowOp == GTE && highOp == LT) {
            if (key >= lowValInt && key < highValInt) {
                outRid = node->ridArray[nextEntry];
                nextEntry++;
            } else {
                throw IndexScanCompletedException();
            }
        } else {
            if (key > lowValInt && key < highValInt) {
                outRid = node->ridArray[nextEntry];
                nextEntry++;
            } else {
                throw IndexScanCompletedException();
            }
        }
    }

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
    const void BTreeIndex::endScan() {
        if (!scanExecuting) {
            throw ScanNotInitializedException();
        }
        bufMgr->unPinPage(file, currentPageNum, false);

        scanExecuting = false;
        currentPageData = nullptr;
        currentPageNum = -1;
        nextEntry = -1;
    }

// -----------------------------------------------------------------------------
//  Helper function
// -----------------------------------------------------------------------------
//
    PageIDPair BTreeIndex::createNode(int nodeType) {
        PageId pid;
        Page *page;
        bufMgr->allocPage(file, pid, page);

        if (nodeType == LEAF) {
            LeafNodeInt *node = reinterpret_cast<LeafNodeInt *>(page);

            node->rightSibPageNo = EMPTY_SLOT;
            for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                node->keyArray[i] = EMPTY_SLOT;
                node->ridArray[i].page_number = (PageId) EMPTY_SLOT;
                node->ridArray[i].slot_number = (SlotId) EMPTY_SLOT;
            }

            PageIDPair pair;
            pair.set(page, pid);
            return pair;
        }

        NonLeafNodeInt *node = reinterpret_cast<NonLeafNodeInt *>(page);

        node->level = 0;
        for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
            node->keyArray[i] = (PageId) EMPTY_SLOT;
            node->pageNoArray[i] = (PageId) EMPTY_SLOT;
        }
        node->pageNoArray[INTARRAYNONLEAFSIZE] = (PageId) EMPTY_SLOT;

        PageIDPair pair;
        pair.set(page, pid);
        return pair;
    }


    const PageKeyPair<int> BTreeIndex::insertToNode(PageId nodeId, const void *key, const RecordId rid) {
        NonLeafNodeInt *node;
        bufMgr->readPage(file, nodeId, (Page *&) node);
        PageKeyPair<int> retVal;
        retVal.set(EMPTY_SLOT, EMPTY_SLOT);
        bool isInUse = false;
        int ikey = *((int *) key);
        int idx = 0;
        while (idx < INTARRAYNONLEAFSIZE && node->keyArray[idx] < ikey && node->keyArray[idx] != EMPTY_SLOT)
            idx++;
        PageId nextNodeId = node->pageNoArray[idx];
        PageKeyPair<int> result;
        if (node->level == 1) {

            LeafNodeInt *node;
            bufMgr->readPage(file, nextNodeId, (Page *&) node);
            PageKeyPair<int> retVal;
            retVal.set(EMPTY_SLOT, EMPTY_SLOT);

            if (node->keyArray[INTARRAYLEAFSIZE - 1] == EMPTY_SLOT) {
                assert(node->keyArray[INTARRAYLEAFSIZE - 1] == EMPTY_SLOT);

                int ikey = *((int *) key);

                int emptySlotIdx = -1;
                for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                    if (node->keyArray[i] == EMPTY_SLOT) {
                        emptySlotIdx = i;
                        break;
                    }
                }

                for (int i = emptySlotIdx; i >= 0; i--) {
                    if (i == 0) {
                        node->keyArray[0] = ikey;
                        node->ridArray[0] = rid;
                    } else {
                        if (node->keyArray[i - 1] > ikey) {
                            node->keyArray[i] = node->keyArray[i - 1];
                            node->ridArray[i] = node->ridArray[i - 1];
                        } else {
                            node->keyArray[i] = ikey;
                            node->ridArray[i] = rid;
                            break;
                        }
                    }
                }
            } else {
                int half = (INTARRAYLEAFSIZE + 1) / 2;
                PageIDPair newLeaf = createNode(LEAF);
                LeafNodeInt *newNode = reinterpret_cast<LeafNodeInt *>(newLeaf.page);

                int ikey = *((int *) key);

                bool insertLeft = false;
                if (ikey < node->keyArray[half]) {
                    insertLeft = true;
                    half = half - 1;
                }

                for (int i = half; i < INTARRAYLEAFSIZE; i++) {
                    newNode->keyArray[i - half] = node->keyArray[i];
                    newNode->ridArray[i - half] = node->ridArray[i];
                    node->keyArray[i] = EMPTY_SLOT;
                    node->ridArray[i].page_number = (PageId) EMPTY_SLOT;
                    node->ridArray[i].slot_number = (SlotId) EMPTY_SLOT;
                }
                if (insertLeft) {
                    assert(node->keyArray[INTARRAYLEAFSIZE - 1] == EMPTY_SLOT);

                    int ikey = *((int *) key);

                    int emptySlotIdx = -1;
                    for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                        if (node->keyArray[i] == EMPTY_SLOT) {
                            emptySlotIdx = i;
                            break;
                        }
                    }

                    for (int i = emptySlotIdx; i >= 0; i--) {
                        if (i == 0) {
                            node->keyArray[0] = ikey;
                            node->ridArray[0] = rid;
                        } else {
                            if (node->keyArray[i - 1] > ikey) {
                                node->keyArray[i] = node->keyArray[i - 1];
                                node->ridArray[i] = node->ridArray[i - 1];
                            } else {
                                node->keyArray[i] = ikey;
                                node->ridArray[i] = rid;
                                break;
                            }
                        }
                    }

                } else {
                    assert(newNode->keyArray[INTARRAYLEAFSIZE - 1] == EMPTY_SLOT);

                    int ikey = *((int *) key);

                    int emptySlotIdx = -1;
                    for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                        if (newNode->keyArray[i] == EMPTY_SLOT) {
                            emptySlotIdx = i;
                            break;
                        }
                    }

                    for (int i = emptySlotIdx; i >= 0; i--) {
                        if (i == 0) {
                            newNode->keyArray[0] = ikey;
                            newNode->ridArray[0] = rid;
                        } else {
                            if (newNode->keyArray[i - 1] > ikey) {
                                newNode->keyArray[i] = newNode->keyArray[i - 1];
                                newNode->ridArray[i] = newNode->ridArray[i - 1];
                            } else {
                                newNode->keyArray[i] = ikey;
                                newNode->ridArray[i] = rid;
                                break;
                            }
                        }
                    }
                }
                PageKeyPair<int> newPair;
                newPair.set(newLeaf.pageNo, newNode->keyArray[0]);
                newNode->rightSibPageNo = node->rightSibPageNo;
                node->rightSibPageNo = newLeaf.pageNo;


                bufMgr->unPinPage(file, newPair.pageNo, true);
                retVal = newPair;


            }
            isInUse = true;

            bufMgr->unPinPage(file, nextNodeId, isInUse);
            result = retVal;

        } else {
            result = insertToNode(nextNodeId, key, rid);
        }
        if (result.pageNo == PageId(EMPTY_SLOT)) {
            bufMgr->unPinPage(file, nodeId, isInUse);
            return retVal;
        }
        if (node->keyArray[INTARRAYNONLEAFSIZE - 1] == EMPTY_SLOT) {
            int emptySlotIdx = -1;
            for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
                if (node->keyArray[i] == EMPTY_SLOT) {
                    emptySlotIdx = i;
                    break;
                }
            }

            for (int i = emptySlotIdx; i >= 0; i--) {
                if (i == 0) {
                    node->keyArray[0] = result.key;
                    node->pageNoArray[1] = result.pageNo;
                } else {
                    if (node->keyArray[i - 1] > result.key) {
                        node->keyArray[i] = node->keyArray[i - 1];
                        node->pageNoArray[i + 1] = node->pageNoArray[i];
                    } else {
                        node->keyArray[i] = result.key;
                        node->pageNoArray[i + 1] = result.pageNo;
                        break;
                    }
                }
            }
        } else {
            PageIDPair newSibPair = createNode(NON_LEAF);
            NonLeafNodeInt *newSib = reinterpret_cast<NonLeafNodeInt *>(newSibPair.page);
            newSib->level = node->level;
            PageKeyPair<int> parentEntry;

            int index = 0;
            while (index < INTARRAYNONLEAFSIZE && node->keyArray[index] < result.key)
                index++;

            int half = (INTARRAYNONLEAFSIZE + 1) / 2;
            if (index < half) {
                for (int i = 0; i < half - 1; i++) {
                    newSib->keyArray[i] = node->keyArray[i + half];
                    newSib->pageNoArray[i] = node->pageNoArray[i + half];
                    node->keyArray[i + half] = EMPTY_SLOT;
                    node->pageNoArray[i + half] = EMPTY_SLOT;
                }
                newSib->pageNoArray[half - 1] = node->pageNoArray[INTARRAYNONLEAFSIZE];
                node->pageNoArray[INTARRAYNONLEAFSIZE] = EMPTY_SLOT;

                parentEntry.set(newSibPair.pageNo, node->keyArray[half - 1]);

                node->keyArray[half - 1] = EMPTY_SLOT;
                int emptySlotIdx = -1;
                for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
                    if (node->keyArray[i] == EMPTY_SLOT) {
                        emptySlotIdx = i;
                        break;
                    }
                }

                for (int i = emptySlotIdx; i >= 0; i--) {
                    if (i == 0) {
                        node->keyArray[0] = result.key;
                        node->pageNoArray[1] = result.pageNo;
                    } else {
                        if (node->keyArray[i - 1] > result.key) {
                            node->keyArray[i] = node->keyArray[i - 1];
                            node->pageNoArray[i + 1] = node->pageNoArray[i];
                        } else {
                            node->keyArray[i] = result.key;
                            node->pageNoArray[i + 1] = result.pageNo;
                            break;
                        }
                    }
                }
            } else if (index == half) {
                for (int i = 0; i < half - 1; i++) {
                    newSib->keyArray[i] = node->keyArray[i + half];
                    newSib->pageNoArray[i + 1] = node->pageNoArray[i + half + 1];
                    node->keyArray[i + half] = EMPTY_SLOT;
                    node->pageNoArray[i + half + 1] = EMPTY_SLOT;
                }
                newSib->pageNoArray[0] = result.pageNo;

                parentEntry.set(newSibPair.pageNo, result.key);
            } else {
                for (int i = 0; i < half - 2; i++) {
                    newSib->keyArray[i] = node->keyArray[i + half + 1];
                    newSib->pageNoArray[i] = node->pageNoArray[i + half + 1];
                    node->keyArray[i + half + 1] = EMPTY_SLOT;
                    node->pageNoArray[i + half + 1] = EMPTY_SLOT;
                }
                newSib->pageNoArray[half - 2] = node->pageNoArray[INTARRAYNONLEAFSIZE];
                node->pageNoArray[INTARRAYNONLEAFSIZE] = EMPTY_SLOT;

                parentEntry.set(newSibPair.pageNo, node->keyArray[half]);

                node->keyArray[half] = EMPTY_SLOT;
                int emptySlotIdx = -1;
                for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
                    if (newSib->keyArray[i] == EMPTY_SLOT) {
                        emptySlotIdx = i;
                        break;
                    }
                }

                for (int i = emptySlotIdx; i >= 0; i--) {
                    if (i == 0) {
                        newSib->keyArray[0] = result.key;
                        newSib->pageNoArray[1] = result.pageNo;
                    } else {
                        if (newSib->keyArray[i - 1] > result.key) {
                            newSib->keyArray[i] = newSib->keyArray[i - 1];
                            newSib->pageNoArray[i + 1] = newSib->pageNoArray[i];
                        } else {
                            newSib->keyArray[i] = result.key;
                            newSib->pageNoArray[i + 1] = result.pageNo;
                            break;
                        }
                    }
                }
            }


            bufMgr->unPinPage(file, newSibPair.pageNo, true);
            retVal = parentEntry;
        }
        isInUse = true;
        bufMgr->unPinPage(file, nodeId, isInUse);
        return retVal;
    }

}





