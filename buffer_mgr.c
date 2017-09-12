#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
//#include <crtdbg.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"

typedef struct BM_Frame {
    int activeFlag;
    int frameReadCount;
    int frameWriteCount;
    bool fixCount;
    int dirtyFlag;
    int framePriority;
    BM_PageHandle *bm_pageHandle;
} BM_Frame;

typedef struct BM_Pool {
    int poolReadCount;
    int poolWriteCount;
    int poolPriority;
    BM_Frame *frames;
} BM_Pool;


/* Function : initBufferPool
*
* Initializes buffer pool of the buffer manager to store pagefile into memory.
* 
* Params:
* BM_BufferPool *const bm        : constant pointer which acts as handle for buffer pool. This will be initalized in this function.
* const char *const pageFileName : constatnt pointer to a constant variable which is the name of the requested page file.
* const int numPages             : Total number of pages that the buffer pool can store
* ReplacementStrategy strategy   : Replacement Strategy to be used once all the frames are full.
* void *stratData                : Null pointer for future use. This will only be initialized in this function.
*
* Returns: Appropriate return code
*/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {

      /* Return if invalid number of pages in input */
    if (numPages <= 0) {
        return RC_INVALID_BM;
    }

	SM_FileHandle *sm_FileHandle = malloc(sizeof(SM_FileHandle));

    /* try to open the pagefile and return if invalid */
    if (openPageFile(pageFileName, sm_FileHandle) != RC_OK) {
        return RC_FILE_NOT_FOUND;
    }

	sm_FileHandle->curPagePos = 3; //Need to override this as first two pages are used to store schema and custom data;

    /* try to close the page file and return if invalid */
    if (closePageFile(sm_FileHandle) != RC_OK) {
        return RC_CLOSE_FILE_FAILED;
    }
	free(sm_FileHandle);
    // initialize BufferPool
	struct BM_Pool *bmPool;
	bmPool= malloc(sizeof(BM_Pool));
	struct BM_Frame *frames;
	frames= malloc(sizeof(BM_Frame) * numPages);

    int i;

    for (i = 0; i < numPages; i++) {
        frames[i].activeFlag = 0;
        frames[i].frameReadCount = 0;
        frames[i].frameWriteCount = 0;
        frames[i].fixCount = 0;
        frames[i].dirtyFlag = FALSE;
        frames[i].framePriority = -1;
        frames[i].bm_pageHandle = NULL;
    }

    /* Initialize rest of the counters */
    bmPool->poolReadCount = 0;
    bmPool->poolWriteCount = 0;
    bmPool->poolPriority = 0;
    bmPool->frames = frames;

    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = bmPool;

    return RC_OK;
}


/* Function : shutdownBufferPool
*
* Shutsdown the input bufferpool;
*
* Params
* BM_BufferPool : A pointer to the buffer pool that is to be shutdown
*
* Returns: Appropriate return code
*
*/
RC shutdownBufferPool(BM_BufferPool *const bm) {
    // make sure every page is successfuly write to file
    // destory every variable
    struct BM_Pool *bmPool = bm->mgmtData;
    struct BM_Frame *frames = bmPool->frames;
    int i;

    SM_FileHandle fh;
    openPageFile(bm->pageFile, &fh);

    for(i = 0; i < bm->numPages; i++) {
        if (frames[i].dirtyFlag == 1 && frames[i].fixCount > 0) {
            writeBlock(frames[i].bm_pageHandle->pageNum, &fh, frames[i].bm_pageHandle->data);
            bmPool->poolWriteCount++;
            frames[i].frameWriteCount++;
        }
        if (frames[i].bm_pageHandle != NULL) {
            free(frames[i].bm_pageHandle);
            free(frames[i].bm_pageHandle->data);
        }
        frames[i].activeFlag = 0;
        frames[i].frameReadCount = 0;
        frames[i].frameWriteCount = 0;
        frames[i].fixCount = 0;
        frames[i].dirtyFlag = FALSE;
        frames[i].framePriority = -1;
        frames[i].bm_pageHandle = NULL;
    }
    closePageFile(&fh);
    return RC_OK;
}


/*
* Function : forceFlushPool
*
* Loops through all the frames and writes all the dirty frames to disk
*
* Params
* BM_BufferPool : A pointer to the buffer pool that is to be shutdown
*
* Returns: Appropriate return code
*
*/
RC forceFlushPool(BM_BufferPool *const bm) {
     struct BM_Pool *bmPool = bm->mgmtData;
    struct BM_Frame *frames = bmPool->frames;
	int i;
    for (i = 0; i < bm->numPages; i++) {
        if (frames[i].activeFlag == 1 && frames[i].dirtyFlag == TRUE) {
            frames[i].dirtyFlag = FALSE;
        }
    }
    return RC_OK;

}

// Clients of the buffer manager can request pages identified by their position in the page file (page number) to be loaded in a page frame.

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
   
    struct BM_Pool *bmPool = bm->mgmtData;
    struct BM_Frame *frames = bmPool->frames;
    int i;
    int poolPos = -1;
    
    for (i = 0; i < bm->numPages; i++) {
        
        if (frames[i].activeFlag == 0) {
            
            poolPos = i;
            break;
        }
        
        if (frames[i].activeFlag == 1 && frames[i].bm_pageHandle->pageNum == pageNum) {
           
            poolPos = i;
            break;
        }
    }

    int minpriority = INT_MAX;
    
    if (poolPos < 0) {
        for (i = 0; i < bm->numPages; i++) {
            if (frames[i].framePriority < minpriority){
                if(frames[i].fixCount == 0) {
                minpriority = frames[i].framePriority;
                poolPos = i;
            }
        }
        }
    }

    if (poolPos < 0) {
        return RC_ERROR;
    }

	SM_FileHandle* fh = malloc(sizeof(SM_FileHandle));
    openPageFile(bm->pageFile, fh);


    if (frames[poolPos].activeFlag == 1 && frames[poolPos].bm_pageHandle->pageNum == pageNum) {
        
        if (bm->strategy == RS_LRU) {
            frames[poolPos].framePriority = ++(bmPool->poolPriority);
        }
        
        frames[poolPos].fixCount++;
        page->pageNum = pageNum;
		page->data = (char *)malloc(PAGE_SIZE*sizeof(char));
        strcpy(page->data, frames[poolPos].bm_pageHandle->data);
    } else {
       
        if (frames[poolPos].activeFlag == 1) {
            free(frames[poolPos].bm_pageHandle->data);
            free(frames[poolPos].bm_pageHandle);
        }

        BM_PageHandle *newpage = MAKE_PAGE_HANDLE();
        page->data = malloc(PAGE_SIZE);
        newpage->data = malloc(PAGE_SIZE);
        page->pageNum = pageNum;
        newpage->pageNum = pageNum;
        readBlock(pageNum, fh, page->data);
        bmPool->poolReadCount = bmPool->poolReadCount + 1;
        strcpy(newpage->data, page->data);

        frames[poolPos].activeFlag = 1;
        frames[poolPos].frameReadCount = 1;
        frames[poolPos].frameWriteCount = 0;
        frames[poolPos].fixCount = 1;
        frames[poolPos].framePriority = ++(bmPool->poolPriority);
        frames[poolPos].dirtyFlag = FALSE;
        frames[poolPos].bm_pageHandle = newpage;

		//free(page->data);

    }
    closePageFile(fh);
	free(fh);
    return RC_OK;
}

/* To mark a page as dirty when its content is modified. */

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {

    struct BM_Pool *bmPool = bm->mgmtData;
    struct BM_Frame *frames = bmPool->frames;

    int poolPos = -1;

    int i;
    for (i = 0; i < bm->numPages; i++) {
        if (frames[i].bm_pageHandle->pageNum == page->pageNum && frames[i].activeFlag == 1) {
            poolPos = i;
            break;
        }
    }

    if (poolPos < 0) {
        return PAGE_ERROR;
    }

    frames[poolPos].dirtyFlag = TRUE;
    return RC_OK;
}

/* TO inform buffer manager that a page is no longer needed*/

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {

    struct BM_Pool *bmPool = bm->mgmtData;
    struct BM_Frame *frames = bmPool->frames;
    int i;
    int poolPos = -1;

    for (i = 0; i < bm->numPages; i++) {
        if (frames[i].activeFlag == 1 && frames[i].bm_pageHandle->pageNum == page->pageNum) {
            poolPos = i;
            break;
        }
    }

    if (poolPos < 0) {
        return PAGE_ERROR;
    }

    if (frames[poolPos].dirtyFlag == 1) {
		//printf("unpin page trying to alloc memorey\n");
		SM_FileHandle* fh = malloc(sizeof(SM_FileHandle));
		//printf("unpin page open page file\n");
        openPageFile(bm->pageFile, fh);
		//printf("unpin page write page file\n");
        writeBlock(page->pageNum, fh, page->data);
		//printf("unpin page close page file\n");
        closePageFile(fh);
        frames[poolPos].frameWriteCount = frames[poolPos].frameWriteCount + 1;
        bmPool->poolWriteCount = bmPool->poolWriteCount + 1;
		free(fh);
    }

    frames[poolPos].fixCount = frames[poolPos].fixCount - 1;
	
    
    return RC_OK;
}

/* To write the current content of the page back to the page file on disk. */

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    struct BM_Pool *bmPool = bm->mgmtData;
    struct BM_Frame *frames = bmPool->frames;
    int i;

	SM_FileHandle* fh = malloc(sizeof(SM_FileHandle));
    openPageFile(bm->pageFile, fh);
    writeBlock(page->pageNum, fh, page->data);
    closePageFile(fh);

    int exist = 0;
    for (i = 0; i < bm->numPages; i++) {
        if (frames[i].activeFlag == 1 && frames[i].bm_pageHandle->pageNum == page->pageNum) {
            exist = i;
            break;
        }
    }
    frames[exist].frameWriteCount = frames[exist].frameWriteCount + 1;
    strcpy(frames[exist].bm_pageHandle->data, page->data);

    bmPool->poolWriteCount = bmPool->poolWriteCount + 1;
    return RC_OK;
}



// Statistics Interface
PageNumber *getFrameContents(BM_BufferPool *const bm) {
    int *frameContents = malloc(sizeof(int) * bm->numPages);
    struct BM_Pool *bmPool = bm->mgmtData;
    struct BM_Frame *frames = bmPool->frames;

    int i;
    for (i = 0; i < bm->numPages; i++) {
        if (frames[i].activeFlag == 1) {
            frameContents[i] = frames[i].bm_pageHandle->pageNum;
        } 
    	/*else {
            frameContents[i] = -1;
        }*/
    }

    return frameContents;
}

/* returns an array of bools (of size numPages) where the ith element is TRUE if 
 * the page stored in the ith page frame is dirty. */
bool *getDirtyFlags(BM_BufferPool *const bm) {
    bool *dirtyFlags = malloc(sizeof(bool) * bm->numPages);
    struct BM_Pool *bmPool = bm->mgmtData;
    struct BM_Frame *frames = bmPool->frames;
    int i;

    for (i = 0; i < bm->numPages; i++) {
        dirtyFlags[i] = frames[i].dirtyFlag;
    }

    return dirtyFlags;
}

/*returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame */
int *getFixCounts(BM_BufferPool *const bm) {
    int *fixCounts = malloc(sizeof(bool) * bm->numPages);
    struct BM_Pool *bmPool = bm->mgmtData;
    struct BM_Frame *frames = bmPool->frames;
    int i;

    for (i = 0; i < bm->numPages; i++) {
        fixCounts[i] = frames[i].fixCount;
    }
    return fixCounts;
}

/* returns the number of pages that have been read from disk since a buffer pool has been initialized */
int getNumReadIO(BM_BufferPool *const bm) {
    // calculate total read io
    struct BM_Pool *bmPool = bm->mgmtData;
    return bmPool->poolReadCount;
}

/* returns the number of pages written to the page file since the buffer pool has been initialized. */

int getNumWriteIO(BM_BufferPool *const bm) {
    // calculate total write io
    struct BM_Pool *bmPool = bm->mgmtData;
    return bmPool->poolWriteCount;
}
