#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

#include <ctype.h>

#include <sys/types.h>
#include <sys/time.h>
#include <err.h>

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

#define SHT_ERROR -1
#define SHT_OK 0

//log files
FILE* errorfile;
FILE* demofile;
//FILE* configfile;




int hashnameToBucket(  char name[15], int numberofbuckets)
{ 
  int temp,final=0;
  char c;
  
  for ( int i =0 ; i<5; i++)
   {
    c = name[i];
   
    if ( isalpha (c) )
    {
      //printf( "c = %c \n",c);
      temp= (int )(c);
      final+=temp;
    }
    
  }

final = final % numberofbuckets;
//printf( " final  = %d \n", final);
return final;

}

void printfileSHTrecord ( SHT_name_Record record)
{
  fprintf( demofile,"(%d,%s)\n",record.datablockID,record.nameKey);
}

void printSHTrecord ( SHT_name_Record record)
{
  printf( "(%d,%s)",record.datablockID,record.nameKey);
}
void printSHTfilerecord ( SHT_name_Record record)
{
  fprintf(demofile,"(%d,%s)\n",record.datablockID,record.nameKey);
}



int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){


  if ( buckets >19)
        {// can be 2x by removing of bucket pointers
         // educational demo only field
           printf( " Wrong buckets parameter , max capacity for  page/block size 512 byte  is 19 buckets");
            fprintf(errorfile, " Wrong buckets parameter , max capacity for  page/block size 512 byte  is 19 buckets");
            return SHT_ERROR;
        } 

  BF_Block* index_block0;
  BF_Block_Init(&index_block0);
  char * index_data_0;
  int fileIndex;

  if (BF_CreateFile(sfileName)!=BF_OK)
    {
      fprintf(errorfile,"SHT_ERROR1\n");
      return SHT_ERROR;
    }
  if(BF_OpenFile(sfileName, &fileIndex) != BF_OK)
    {
      fprintf( errorfile,"SHT_ERROR12\n");
      return SHT_ERROR;
    }
  if(BF_AllocateBlock(fileIndex, index_block0) != BF_OK)
    {
      fprintf( errorfile,"SHT_ERROR13\n");
      return SHT_ERROR;
    }

  int counter=0;
  BF_GetBlockCounter(fileIndex,&counter);
  counter--;

  int data_descriptor=-1;
  if(BF_OpenFile(fileName,&data_descriptor)!=BF_OK)
  {
    fprintf( errorfile,"SHT_ERROR14\n");
      return SHT_ERROR; 
  }


  //##############################//
  // initializing index's block 0   
  int maxrecords= ( BF_BLOCK_SIZE - sizeof(SHT_block_info) ) / sizeof(SHT_name_Record);
  printf(" maxRecords= %d\n",maxrecords);

  int index_offset=BF_BLOCK_SIZE - sizeof( SHT_block_info);
  int block_num_offset=index_offset+sizeof(int);
  int next_id_offset=block_num_offset+sizeof(int);

  SHT_info index_header_info;
  index_header_info.fileDesc=fileIndex;
  index_header_info.primaryDesc=data_descriptor;
  index_header_info.numBuckets=buckets;
  index_header_info.myBlockNumber=counter;
  index_header_info.maxRecords=maxrecords;
  index_header_info.fileType=SECONDARYHASHTABLE;

  index_header_info.indexOffset=index_offset;
  index_header_info.blocknumOffset=block_num_offset;
  index_header_info.nextidOffset=next_id_offset;
  for ( int i =0 ; i < index_header_info.numBuckets ; i ++)
  {
    index_header_info.SHT_BUCKET_ID[i]=-1;// last block of correspondive bucket
    index_header_info.SHT_INDEX_RECS[i]=0;// counter of records in bucket
  }

  //##############################//

  index_data_0=BF_Block_GetData(index_block0);

  memcpy(index_data_0,&index_header_info,sizeof(SHT_info));
  BF_Block_SetDirty(index_block0);
  BF_UnpinBlock(index_block0);
  return SHT_OK;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
  int indexFileDesc;
  BF_Block* blockvar;
  BF_Block_Init(&blockvar);

    // open secondary hast table index file with name indexName
    if (BF_OpenFile( indexName,&indexFileDesc)!=BF_OK)
    {
          fprintf(errorfile," SHT_ERROR2\n");
          return NULL;
        
    }
    //read metadata of secondary hash table index
    if ( BF_GetBlock( indexFileDesc, 0, blockvar)!=BF_OK )
    {
          fprintf(errorfile," SHT_ERROR21\n");
          return NULL;
        
    }

    // create update and return buffer handle structure
    // this is the interface of the index for the using function(main etc..)
    // updates permantly pinned diskblock0 when the file will be closed
    SHT_info* shisto=(SHT_info*) malloc ( sizeof(SHT_info));
    char* data0_start=BF_Block_GetData( blockvar);

    if (data0_start==NULL)
    {
          fprintf(errorfile," SHT_ERROR22\n");
          return NULL;
    }

    memcpy(shisto,data0_start,sizeof(SHT_info));
    //#####
    BF_UnpinBlock(blockvar);
    return shisto;

}


int SHT_CloseSecondaryIndex( SHT_info* sht_info ){

  if( sht_info->fileType != SECONDARYHASHTABLE)
  {
    fprintf(errorfile,"SHT_ERROR49\n");
    return SHT_ERROR;
  }
  
  int indexFileDesc=sht_info->fileDesc;

  BF_Block* blockvar;
  BF_Block_Init(&blockvar);

  /// get permanently pinned index block
  if ( BF_GetBlock( indexFileDesc, 0, blockvar)!=BF_OK )
    {
          fprintf(errorfile," SHT_ERROR3\n");
          return SHT_ERROR;
        
    }
  char* data0_start=BF_Block_GetData(blockvar);

  if (data0_start==NULL)
      {
            fprintf(errorfile," SHT_ERROR31\n");
            return SHT_ERROR;
      }
  // update disk -index block 0
  memcpy(data0_start,sht_info,sizeof(SHT_info));
  

  
  // realese memory allocated for buffer handler object

    //printf ( " filedesc = %d , numBuckets = %d , maxRecords = %d , fileType = %d , " 
      //,SHT_info->fileDesc, SHT_info->numBuckets
    //, SHT_info->maxRecords, SHT_info->fileType);


    //printf ( " sizeof sht record = %lu \n", sizeof(int));
   printf( "\n Closing SHT file\n");

    
    BF_Block_SetDirty(blockvar);
    BF_UnpinBlock(blockvar);

    if ( BF_CloseFile(indexFileDesc)!=BF_OK)
      {
        fprintf( errorfile,"SHT_ERROR32\n");
        return SHT_ERROR;
      }

    free(sht_info);
    return SHT_OK;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id)
{
  fprintf(demofile,"*************************************\n");
  fprintf(demofile, " Inserting Record in secondary HT :");
  printfileRecord( record);


  if( sht_info->fileType != SECONDARYHASHTABLE)
  {
    fprintf(errorfile,"SHT_ERROR49\n");
    return SHT_ERROR;
  }

  SHT_name_Record  new_secondary_entry;
 
  new_secondary_entry.datablockID=block_id;
  strcpy ( new_secondary_entry.nameKey,record.name);

  // using name as key to produce a hash value
  int hash_result=hashnameToBucket(new_secondary_entry.nameKey, sht_info->numBuckets);
  printf(" Secondary index hash_result = %d\n",hash_result);
  fprintf(demofile," Secondary index hash_result = %d\n",hash_result);
  // now we know which bucket is going to receive new entry
  // there are 3 scenarios
  // 1. bucket is empty
  // 2.1 bucket is not empty and last block has enough space
  // 2.2 bucket is not empty but last block has not enough space



  BF_Block* blockvar;
  BF_Block_Init(&blockvar);
  char* newdata;
  int counter=-1;
  

  //####################//
  //Initial index values of data containing blocks in a secondary hash table 
  SHT_block_info blockTailInfo;

  blockTailInfo.recordsCounter=1;
  blockTailInfo.blockNum=-1;
  blockTailInfo.nextBlockID=-1;
  //####################//

  //printf( "KALOS \n");


  if ( sht_info->SHT_BUCKET_ID[hash_result]==-1)
    {//case 1. bucket is empty

      //printf( "KALOS 1\n");
      if( BF_AllocateBlock(sht_info->fileDesc , blockvar) !=BF_OK)
      {
        fprintf( errorfile,"SHT_ERROR8\n");return SHT_ERROR;
      }
      
      
      newdata=BF_Block_GetData(blockvar);

      //learn new born block's ID
      BF_GetBlockCounter(sht_info->fileDesc, &counter);
      counter--;
      blockTailInfo.blockNum=counter;

      //inialize new block's index
      memcpy(newdata + sht_info->indexOffset , &blockTailInfo,sizeof(SHT_block_info));
      BF_Block_SetDirty(blockvar);

      //insert new entry at the beginning of new block
      memcpy(newdata,&new_secondary_entry,sizeof(SHT_name_Record));
      //printf( "KALOS 2\n");
      //update buffer handler variable
      sht_info->SHT_BUCKET_ID[hash_result]=counter;
      sht_info->SHT_INDEX_RECS[hash_result]++;

      BF_UnpinBlock(blockvar);
      
      //printf( "KALOS 3\n");
      printf( " New Record inserted in bucket %d of secondary hast table, block%d is the container \n",hash_result, counter);
      fprintf(demofile," New Record inserted in bucket %d of secondary hast table, block %d is the container \n",hash_result, counter);
      
      fprintf(demofile,"*************************************\n\n\n");
      return SHT_OK;

    }
  else
  { // 2. bucket is not empty 

      //printf( "KALOS 4\n");
      BF_Block* lastblock;    
      BF_Block_Init ( &lastblock);
  
      char* lastdata;

      BF_GetBlock( sht_info->fileDesc , sht_info->SHT_BUCKET_ID[hash_result], lastblock);
      

      lastdata=BF_Block_GetData(lastblock);
      fprintf(demofile," Secondary Bucket will be %d \n",hash_result);
      
      SHT_block_info lastblockindex;
      //retrieve last blocks index 

      memcpy( &lastblockindex ,lastdata+ sht_info->indexOffset, sizeof(SHT_block_info));
      

        if( lastblockindex.recordsCounter<sht_info->maxRecords)
        { // 2.1 bucket is not empty and last block has enough space
            //printf( "KALOS 5\n");
            // write new entry at correct space
            memcpy(lastdata+lastblockindex.recordsCounter*(sizeof(SHT_name_Record)), &new_secondary_entry, sizeof(SHT_name_Record));

            lastblockindex.recordsCounter++;

            //update last block index
            memcpy(lastdata + sht_info->indexOffset , &lastblockindex,sizeof(SHT_block_info));

            //update buffer handler variable
            sht_info->SHT_INDEX_RECS[hash_result]++;

            //printf( "KALOS 6\n");

            BF_Block_SetDirty( lastblock);
            BF_UnpinBlock( lastblock);

            int currentlastblockid=sht_info->SHT_BUCKET_ID[hash_result];
            printf( " New Record inserted in bucket %d of secondary hast table, block%d is the container \n",hash_result, currentlastblockid);
            fprintf(demofile," New Record inserted in bucket %d of secondary hast table, block %d is the container \n",hash_result, currentlastblockid);

            fprintf(demofile,"*************************************\n\n\n");
            return SHT_OK;

        }

        else
        {// 2.2 bucket is not empty but last block has not enough space
          //printf( "KALOS 7\n");

          if ( BF_AllocateBlock (sht_info->fileDesc, blockvar)!=BF_OK)
              {fprintf( errorfile,"SHT_ERROR9\n");return SHT_ERROR;}
          
          newdata= BF_Block_GetData( blockvar);

          BF_GetBlockCounter(sht_info->fileDesc,&counter);
          counter--;

          // initialize new last block index in a way that points to old last block 
          blockTailInfo.recordsCounter=1;
          blockTailInfo.blockNum=counter;
          blockTailInfo.nextBlockID=lastblockindex.blockNum;

          memcpy( newdata + sht_info->indexOffset, &blockTailInfo , sizeof(SHT_block_info));
          //printf( "KALOS 8\n");
          // write entry at the beginning of new block
          memcpy( newdata ,&new_secondary_entry,sizeof(SHT_name_Record));

          BF_Block_SetDirty(blockvar);

          sht_info->SHT_BUCKET_ID[hash_result]=counter;
          sht_info->SHT_INDEX_RECS[hash_result]++;
          BF_UnpinBlock(blockvar);
          BF_UnpinBlock(lastblock);

          printf( " New Record inserted in bucket %d of secondary hast table, block%d is the container \n",hash_result, counter);
          fprintf(demofile," New Record inserted in bucket %d of secondary hast table, block %d is the container \n",hash_result, counter);

          //printf( "KALOS 9\n");

          fprintf(demofile,"*************************************\n\n\n");
          return SHT_OK;
        }


  }

  return SHT_ERROR;
}


int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){
  

    if ( ht_info->fileType != HASHFILE)
        {fprintf( errorfile,"SHT_ERROR61\n");return SHT_ERROR;}
    if ( sht_info->fileType != SECONDARYHASHTABLE)
        {fprintf( errorfile,"SHT_ERROR62\n");return SHT_ERROR;}
    printf("\n\nRUNsht GETAllEntries\n\n");
    fprintf(demofile,"\n\nRUNsht GETAllEntries\n\n");

    int blockcounter=0;
    int primarycounter=0;
    int nameappear=0;

    BF_Block* blockvar;
    BF_Block_Init(&blockvar);
    char* data_start;

    BF_Block* primblock;
    BF_Block_Init(&primblock);
    char* prim_datastart;


    
    int foundflag=0;

    SHT_block_info dummy_index;

  //get  bucket
  int hash_result= hashnameToBucket(name,sht_info->numBuckets);

  int currentblockid=sht_info->SHT_BUCKET_ID[hash_result];

  while( currentblockid!=-1)
  {// check block 
      blockcounter++;

      BF_UnpinBlock(blockvar);
      if(BF_GetBlock(sht_info->fileDesc, currentblockid, blockvar)!=BF_OK)
      {fprintf(errorfile,"SHT_ERROR12\n");return SHT_ERROR;      
      }

      data_start=BF_Block_GetData(blockvar);
      memcpy(&dummy_index,data_start+sht_info->indexOffset,sizeof(SHT_block_info));

      SHT_name_Record temp_sht_record;
      fprintf(demofile,"________________________\n\n");
      for ( int i  =0; i< dummy_index.recordsCounter; i++)
      {//check all records in a secondary block

        memcpy(&temp_sht_record, data_start + ( i * sizeof( SHT_name_Record)) , sizeof(SHT_name_Record));
        

        if( strcmp(name,temp_sht_record.nameKey)==0)
        {// if given name matches a record
          nameappear++;
          primarycounter++;

          printf("Found ");
          fprintf( demofile,"Found ");

          printSHTrecord(temp_sht_record);printf(" in SHT .\nDatabase entries : \n\n");
          printSHTfilerecord(temp_sht_record);
          //printf(" in block %d of secondary HT.\n",currentblockid);
          fprintf( demofile," in block %d of secondary HT.",currentblockid);
          foundflag=1;  

          //now we will search corresponding block of primary HT ,the db file
            //printf("primblock = %p", primblock);
            if(BF_GetBlock(ht_info->fileDesc, temp_sht_record.datablockID, primblock)!=BF_OK)
              {fprintf(errorfile,"SHT_ERROR31\n");
                printf(" SHT_ERROR31 and  %d , %d \n",sht_info->fileDesc,temp_sht_record.datablockID);
                //printf("primblock = %p", primblock);
                return SHT_ERROR;      
              }

            prim_datastart =BF_Block_GetData(primblock);

            HT_block_info index_var;
            memcpy(&index_var,prim_datastart+ht_info->indexOffset,sizeof(HT_block_info));
            
            //printf(" Searching primary block .... : %d \n",temp_sht_record.datablockID);
            fprintf(demofile," Searching primary block .... : %d \n",temp_sht_record.datablockID);
            for ( int j =0;j<index_var.recordsCounter;j++)
            {// searching primary block


              char tempstring[16];
              memcpy(&tempstring, prim_datastart+ (j* sizeof(Record)) + (16*sizeof(char) +sizeof(int)),16*sizeof(char) );
              if(strcmp(name,tempstring )==0)
                {//when we find the name in block
                   Record toprintRec;   
                   memcpy(&toprintRec, prim_datastart + ( j*sizeof(Record) ),sizeof(Record));

                   printf(" ***");
                   printRecord(toprintRec);
                   //printf(" *******\n");
                   fprintf(demofile," \n\n");
                   printfileRecord(toprintRec);

                   
                }

              }//end for
              printf("_____________\n");
              fprintf(demofile,"_______________________________________\n\n");





          }//endif
          BF_UnpinBlock(primblock);

      }//endfor

      currentblockid=dummy_index.nextBlockID;


  }//endwhile
BF_UnpinBlock(blockvar);

return blockcounter+primarycounter;
}

int sHashStatistics(char* filename  )
{

    BF_Block* block0;
    BF_Block_Init(&block0);
    int fileIndex;
    if(BF_OpenFile(filename, &fileIndex) != BF_OK)
        {fprintf( errorfile,"SHT_ERROR85\n");return SHT_ERROR;}
    
    if ( BF_GetBlock( fileIndex, 0, block0)!=BF_OK )
        {return -1;}
    char* data0;
    data0=BF_Block_GetData(block0);
    if (data0==NULL)
        {
            fprintf(errorfile," SHTErrorStatistics\n");
            return -1;
        }

    SHT_info shinfo;

    memcpy(&shinfo,data0,sizeof(SHT_info));

    printf(" _______________________   ");
    printf("\n    Stats of file :   %s \n",filename);
    printf(" _______________________   \n");

    int average_blocks_perbucket[shinfo.numBuckets];
    int min=616614146;
    int max=0;
    int sum_blocks=0;
    int sum_records=0;
    int ovrfl_buckets_counter=0;


    for ( int i =0 ; i < shinfo.numBuckets; i++)
    {// for every bucket
        int max_block=-1;
        
        int recs=0;
        max_block=shinfo.SHT_BUCKET_ID[i];
       
        if ( max_block >0)
            {   
                recs =shinfo.SHT_INDEX_RECS[i];
                max_block= recs/shinfo.maxRecords +1;

                printf( " SHTBucket [%d] contains %d records  in %d  blocks . ", i , recs ,max_block);
                if ( max_block>1)
                    {
                        printf( "Overflowing block(s) : %d \n", max_block-1);
                        ovrfl_buckets_counter++;

                    }
                else 
                    {

                        printf( " No overflowing blocks\n");
                    }

            }  
 
        if ( min > recs)
        {
            min=recs;
        }

        if ( max < recs)
        {
            max=recs;
        }

        sum_blocks+=max_block;
        sum_records+=recs;



    }

    printf("\nFile %s is stored in %d blocks \n" , filename,sum_blocks);
    printf(" Minimum |records_per_bucket| = %d \n",min);
    

    printf( " Average |records_per_bucket| = %0.2lf \n",(double)sum_records/shinfo.numBuckets);

    printf(" Maximum |records_per_bucket| = %d\n",max);

    printf(" %d of its' buckets are overflowing \n\n\n",ovrfl_buckets_counter);

    BF_UnpinBlock(block0);

    if ( BF_CloseFile(fileIndex)!=BF_OK)
      {
        fprintf( errorfile,"SHT_ERROR95\n");
        return SHT_ERROR;
      }

    
    return 0;







}





/*
typedef struct {
    int  fileDesk;
    int  numBuckets;
    int  myBlockNumber;
    int  maxRecords;
    int  fileType;

    int  indexOffset;
    int  blovcknumOffset;
    int  nextidOffset;
    
    int  SHT_BUCKET_ID[MAXBUCKETS];
    int  SHT_INDEX_RECS[MAXBUCKESTS];
    char basefilename[20];


} SHT_info;

typedef struct {
    int recordsCounter;
    int blockNum;
    int nextBlockID;

} SHT_block_info;

*/



void userinptsht()
{// enters user control flow mode
 // used only for prints currently
 // expandable to user commands , queries ,data base manipulation

  int fl= 1;
  char c,bychar;
  int maxrecords=  ( BF_BLOCK_SIZE -sizeof(HT_block_info) ) / sizeof(Record);
  while(fl!=0)
  {       printf(" Press  D to see the demononstration file (demo.txt)\n");
          printf(" Press  E to see the error log file ( error.txt) \n");
          printf(" Press  P to see execution parameters\n");
          printf(" Press  CTRL + C to exit\n");
          
        int value = 0;
        struct timeval tmo;
        fd_set readfds;

        fflush(stdout);

        
        FD_ZERO(&readfds);
        FD_SET(0, &readfds);
        tmo.tv_sec = 5;
        tmo.tv_usec = 0;

        switch (select(1, &readfds, NULL, NULL, &tmo)) {
        case -1:
            err(1, "select");
            break;
        case 0:
            printf("Thanks for using our program!\n");
            fl=0;
            return;
        } 
        
        c=getchar();

          switch (c)
          {
            case 'D':
                    rewind(demofile);
                    bychar = fgetc(demofile);
                    while(bychar!=EOF)
                    {
                        printf("%c", bychar);
                        bychar = fgetc(demofile);
                    }
                    
                    fl=1;
                    printf("\n");
                    continue;
            case 'E':
                    fseek(errorfile, 0, SEEK_SET);
                    bychar = fgetc(errorfile);
                    while(bychar!=EOF)
                    {
                        printf("%c", bychar);
                        bychar = fgetc(errorfile);
                    }
                    fl=1;
                    printf("\n");
                    continue;
            case 'P':
                    printf( " Size of Block:  %d\n" ,BF_BLOCK_SIZE);
                    printf( " Size of ht info:  %lu\n" ,sizeof(HT_info));
                    printf( " Size of SHT info:  %lu\n" ,sizeof(SHT_info));
                    printf( " Size of ht block info:  %lu\n" ,sizeof(HT_block_info));
                    printf( " Size of sht block info:  %lu\n" ,sizeof(SHT_block_info));
                    printf( " Size of Record: %lu\n" ,sizeof(Record));
                    printf( " Size of usable space per block in primary ht: %lu / %d  bytes\n",maxrecords*sizeof(Record),BF_BLOCK_SIZE);
                    printf( " Records capacity per block : %d\n\n" ,maxrecords);

                    fl=1;
                    continue;
              default:
                    fl=0;
                    break;

          }

    }
    printf("Thanks for using our program!\n");
}