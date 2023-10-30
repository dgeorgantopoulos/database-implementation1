#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"



enum { HEAPFILE , HASHFILE};

#define HP_OK 0
#define HP_ERROR -1





int printfileexit(int exitcode)
{
    if (exitcode==0)
        printf( "HP_OK");
    else
        printf( "HP_ERROR");
}


#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

int HP_CreateFile(char *fileName){
    

    // update 
    int maxrecords=  ( BF_BLOCK_SIZE -sizeof(HP_block_info) ) / sizeof(Record);
    int index_offset=BF_BLOCK_SIZE - sizeof(HP_block_info);
    int ptroffset=index_offset+ 2*(sizeof(int));

    
    MAXrecords=maxrecords;
    index_OFFSET=index_offset;
    ptr_OFFSET=ptroffset;

    BF_Block *block0;
    BF_Block_Init(&block0);
    

    char* data;   // temporal block data first byte address
    
    int fileIndex;//  file descriptor
    
    if(BF_CreateFile(fileName) != BF_OK)
    return HP_ERROR;
    
    if(BF_OpenFile(fileName, &fileIndex) != BF_OK)
        return HP_ERROR;
    
    if(BF_AllocateBlock(fileIndex, block0) != BF_OK)
        return HP_ERROR;
    
    data=BF_Block_GetData(block0);

    HP_info header_info;
    HP_block_info index_info;

    //initialise metadata header
    header_info.fileDesc=fileIndex;
    header_info.lastBlockid=0;
    header_info.blockNum=0;
    header_info.maxRecords=MAXrecords;
    header_info.fileType=HEAPFILE;

    memcpy(data,&header_info,sizeof(HP_info));

    // initialize index at the end of the block
    index_info.recordsCounter=0;
    index_info.blockNum=0;
    index_info.nextBlock=NULL;

    memcpy(data+index_OFFSET, &index_info,sizeof( HP_block_info));
    
    BF_Block_SetDirty(block0);
    BF_UnpinBlock(block0);
    BF_Block_Destroy(&block0);

    
    
    return HP_OK;
    
}

HP_info* HP_OpenFile(char *fileName){

    //heap file in buffer 
    int fileIndex;
    BF_Block* blockvar;
    BF_Block_Init(&blockvar);
        

    
    if (BF_OpenFile( fileName,&fileIndex)!=BF_OK)
        return NULL;

    
    if ( BF_GetBlock( fileIndex, 0, blockvar)!=BF_OK )
        return NULL;

    //creates,initializes  & returns buffer handle struct
    HP_info* hinfo=(HP_info* )malloc( sizeof(HP_info));


    char* data_start;
    data_start=BF_Block_GetData(blockvar);

    if (data_start==NULL)
        return NULL;
    

    memcpy( hinfo,data_start, sizeof(HP_info));
    

    printf( " %d ,%d ,%d,%d,%d ",hinfo->fileDesc, hinfo->lastBlockid,hinfo->blockNum, hinfo-> maxRecords,hinfo->fileType);

    BF_UnpinBlock(blockvar);
    

    
    return hinfo;
    
}


int HP_CloseFile( HP_info* hp_info ){
//destroy buffer handle struct
    
    int fileIndex=hp_info->fileDesc;

    if ( BF_CloseFile(fileIndex)!=BF_OK)
        return HP_ERROR;
    

    free(hp_info);
    
    return HP_OK;
    
}

int HP_InsertEntry(HP_info* hp_info, Record record){

    //filetype check
    if ( hp_info->fileType != HEAPFILE)
        return HP_ERROR;


    BF_Block* blockvar;
    BF_Block * newblock;

    BF_Block_Init(&newblock);
    BF_Block_Init(&blockvar);

    int fileIndex= hp_info->fileDesc;
    
    // now we get block0 at blockvar
    if (BF_GetBlock( fileIndex , 0 ,blockvar)!=BF_OK)
        return HP_ERROR;
    char* data0;
    data0=BF_Block_GetData(blockvar);


    HP_info dummy_info;

    memcpy(&dummy_info ,data0, sizeof(HP_info));

    int lastblock=dummy_info.lastBlockid;
    int currentblock=dummy_info.blockNum;
    int capacity=dummy_info.maxRecords;




    // in case file has only block0 so far
    //printf (" lastblock = %d , currentblock  = %d \n\n ", lastblock, currentblock);
    if ( lastblock==currentblock)
    {           
                // allocate one more block for this filedesc,update it and pass it
                if ( BF_AllocateBlock(fileIndex,newblock)!= BF_OK)
                    return HP_ERROR;
                int block_counter;
                char* newdata;
                BF_GetBlockCounter(fileIndex,&block_counter);
                block_counter-=1;
                HP_block_info new_index_info;
                new_index_info.recordsCounter=1;// will be 1
                new_index_info.blockNum=block_counter;
                new_index_info.nextBlock=NULL;
                newdata=BF_Block_GetData(newblock);
                // initialize new block index
                memcpy(newdata+index_OFFSET,&new_index_info,sizeof(HP_block_info));
                //write the new entry
                memcpy( newdata, &record, sizeof(Record));
                BF_Block_SetDirty(newblock);
                BF_UnpinBlock(newblock);
                BF_Block_Destroy(&newblock);                
                HP_block_info block0index;
                //fixing block0 header
                memcpy( data0 + sizeof(int), &block_counter,sizeof(int) );
                //fixing block0 index
                memcpy( data0+ptr_OFFSET ,&newblock,sizeof(char*));
                BF_Block_SetDirty(blockvar);
                BF_UnpinBlock(blockvar);
                BF_Block_Destroy(&blockvar);
                printf("Record entered at new block with num %d \n\n",block_counter);
                hp_info->lastBlockid=block_counter;
                return block_counter;

    }
    else {// in case there is at least one more block 

        //find last block and push insertion handling there
        if ( BF_GetBlock(fileIndex,lastblock,newblock)!=BF_OK)
                    return HP_ERROR;
        char* newdata;
        int current_record_counter=0;
        newdata=BF_Block_GetData(newblock);
        memcpy( &current_record_counter ,newdata + index_OFFSET, sizeof(int));

        

        if ( current_record_counter > MAXrecords)
        {//never occured,exists for expansion error indication
            printf(" too many records prior malfunciton");
            return HP_ERROR;
        }

        if (  current_record_counter < MAXrecords)
        {// insert upcoming record to last  block,there is space

            memcpy( newdata+ current_record_counter*sizeof(Record), &record , sizeof(Record));

            int newcounter=current_record_counter+1;

            memcpy( newdata + index_OFFSET, &newcounter, sizeof(int));


            BF_Block_SetDirty(newblock);
            BF_UnpinBlock(newblock);
            

            BF_UnpinBlock(blockvar);

            BF_Block_Destroy(&newblock);
            printf( "current records are %d \n", newcounter);
            printf( "record entered at already existing block with num : %d\n\n",lastblock);


            hp_info->lastBlockid=lastblock;
            return lastblock;
        }

        else if(current_record_counter==MAXrecords)
        {//create new block for the record, add it it there

            BF_Block* new_last;
            BF_Block_Init(&new_last);

            if(BF_AllocateBlock(fileIndex, new_last)!=BF_OK)
                return HP_ERROR;

            memcpy(newdata+ptr_OFFSET , new_last , sizeof(char*));
            BF_Block_SetDirty(newblock);
            BF_UnpinBlock(newblock);
            BF_Block_Destroy(&newblock);

            int new_block_counter;

            BF_GetBlockCounter(fileIndex,&new_block_counter);
            new_block_counter-=1;

            memcpy(data0 +sizeof(int) ,&new_block_counter,sizeof(int));
            BF_Block_SetDirty(blockvar);
            BF_UnpinBlock(blockvar);
            BF_Block_Destroy(&blockvar);


            char* new_last_data;
            new_last_data= BF_Block_GetData(new_last);

            HP_block_info new_index_info;

            new_index_info.recordsCounter=1;// will be 1
            new_index_info.blockNum=new_block_counter;
            new_index_info.nextBlock=NULL;

            memcpy(new_last_data+index_OFFSET,&new_index_info,sizeof(HP_block_info));
            BF_Block_SetDirty(new_last);

            memcpy(new_last_data,&record,sizeof(record));
            BF_UnpinBlock(new_last);
            BF_Block_Destroy(&new_last);

            printf( "current records are %d \n", *(new_last_data+index_OFFSET) );
            printf( "record entered at new block with num : %d\n\n",new_block_counter);

            hp_info->lastBlockid=new_block_counter;
            return new_block_counter;
        }





    } 



    return 0;
    
}

int HP_GetAllEntries(HP_info* hp_info, int value){
    //filetype check
    if ( hp_info->fileType != HEAPFILE)
        return HP_ERROR;
    
    int last_appearence_ofkey=0;
    int fileIndex=hp_info->fileDesc;
    BF_Block* block0;
    BF_Block_Init(&block0);


    BF_Block* blockvar;
    BF_Block_Init(&blockvar);
    int block_counter;

    
    if ( BF_GetBlockCounter( fileIndex, &block_counter)!=BF_OK)
        return HP_ERROR;

    block_counter-=1;


    int i;
    for ( i =1 ; i<= block_counter ; i++)
    {//for each block search each record 

        if (BF_GetBlock(fileIndex,i,blockvar)!=BF_OK)
            return HP_ERROR;

        char* currentblock;
        int recordsinblock;
        
        currentblock=BF_Block_GetData(blockvar);

        memcpy(&recordsinblock,currentblock+index_OFFSET,sizeof(int));

        int temp_key=-1;

        for ( int j =0; j< recordsinblock ; j++)
        {   
            memcpy( &temp_key , currentblock + ( j * sizeof(Record) ) + (16* sizeof(char) ) , sizeof(int) );
            if ( temp_key==value)
            {
                Record toprintRec;
                last_appearence_ofkey=i;
                memcpy(&toprintRec, currentblock + ( j*sizeof(Record) ),sizeof(Record));
                printf( " At block %d , found record : ", i);
                printRecord(toprintRec);
                
            }

        }

    } 
    
    BF_UnpinBlock(block0);
    BF_UnpinBlock(blockvar);

    
    return i-1;
}

