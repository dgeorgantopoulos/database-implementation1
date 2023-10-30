#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#include "bf.h"
#include "ht_table.h"
#include "record.h"

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

//log files
FILE* errorfile;
FILE* demofile;
//FILE* configfile;

void print_bucket_index( int bucket_index[], int index_size)
{   fprintf(demofile,"Index State :\n");
    for ( int i=0; i < index_size; i++)
        fprintf(demofile," bucket_index[ %d ] first element = %d \n", i ,bucket_index[i]);
       
}

void printfileexit(int exitcode)
{
    if (exitcode==0)
        printf( "HT_OK");
    else
        {   
            printf( "HT_ERROR");
            fprintf( errorfile,"HT_ERROR1\n");
        }
}

void cleanblock( char* blockstart)
{   
    char c=' ';
    for( int i=0;i<BF_BLOCK_SIZE; i++)
    {
        memcpy(blockstart,&c,sizeof(char));

    }
}


int hash_mitsos_giorgos( int keytohash,int numBuckets)
{
    int temp=0;
    //temp= (keytohash + 1037) * 2042;
    temp= keytohash % numBuckets;


    return temp;
}

void printfileRecord(Record record){

    fprintf(demofile,"(%d,%s,%s,%s)\n",record.id,record.name,record.surname,record.city);
}

int HT_CreateFile(char *fileName,  int buckets){


    if ( buckets >19)
        {// can be 2x by removing of bucket pointers
         // educational demo only field
           printf( " Wrong buckets parameter , max capacity for  page/block size 512 byte  is 19 buckets");
            fprintf(errorfile, " Wrong buckets parameter , max capacity for  page/block size 512 byte  is 19 buckets");
            return HT_ERROR;
        }
    int maxrecords=  ( BF_BLOCK_SIZE -sizeof(HT_block_info) ) / sizeof(Record);
    int index_offset=BF_BLOCK_SIZE - sizeof(HT_block_info);
    int ptroffset=index_offset+ 2*(sizeof(int));
    BF_Block* block0;
    BF_Block_Init (&block0);
    char* data0;
    int fileIndex;
    // create & open in-buffer  object get your reference to it via file index-descriptor
    if(BF_CreateFile(fileName) != BF_OK)
        {fprintf( errorfile,"HT_ERROR2\n");return HT_ERROR;}
    if(BF_OpenFile(fileName, &fileIndex) != BF_OK)
        {fprintf( errorfile,"HT_ERROR3\n");return HT_ERROR;}
    //metadata block creation
    if(BF_AllocateBlock(fileIndex, block0) != BF_OK)
        {fprintf( errorfile,"HT_ERROR4\n");return HT_ERROR;}

    //temporary address of metadata block
    data0= BF_Block_GetData(block0);

    HT_info header_info;
    /// INITIALISATION METADATA
    header_info.fileDesc=fileIndex;
    header_info.numBuckets=buckets;
    header_info.blockNum=0;
    header_info.maxRecords=maxrecords;
    header_info.fileType=HASHFILE;
    header_info.indexOffset=index_offset;
    header_info.ptrOffset=ptroffset;

    //initialization of bucket index
    for ( int jj =0; jj<header_info.numBuckets ; jj++)
    {// ID BASED navigation for search insertion delete etc... 
     // is superior to &block based thus the pointer array can -and should -be removed
     // we got this uncommented and update throughout the demo 
     // for deggree purposes only

        header_info.HT_INDEX_BUCKETS[jj]=-1;
        header_info.HT_INDEX_PTRS[jj]=NULL;
        header_info.HT_INDEX_RECS[jj]=0;

    }

    //updating metadata block
    memcpy(data0, &header_info,sizeof(HT_info));
    BF_Block_SetDirty(block0);
    BF_UnpinBlock(block0);
    if(BF_CloseFile(fileIndex) != BF_OK)
        {fprintf( errorfile,"HT_ERROR33\n");return HT_ERROR;}
    return HT_OK;

}

HT_info* HT_OpenFile(char *fileName){
    int fileIndex;
    BF_Block* blockvar;
    BF_Block_Init(&blockvar);
    //retrieving proper file descriptor for the give file

    if (BF_OpenFile( fileName,&fileIndex)!=BF_OK)
        return NULL;
    //read metadata of this hast table  structured database
    if ( BF_GetBlock( fileIndex, 0, blockvar)!=BF_OK )
        return NULL;


    // create update & return an object of these data 
    // to get handled instead of actual index block 0 

    HT_info* hinfo=(HT_info* )malloc( sizeof(HT_info));

    char* data_start;
    data_start=BF_Block_GetData(blockvar);

    if (data_start==NULL)
        return NULL;

    memcpy( hinfo,data_start, sizeof(HT_info));
    BF_UnpinBlock(blockvar);

    //BF_Block_Destroy(&blockvar);

    return hinfo;
}


int HT_CloseFile( HT_info* ht_info ){

    
    /*
    BF_Block* blockran;
    BF_Block_Init(&blockran);
    char* data0;

    int cou;
    BF_GetBlockCounter(ht_info->fileDesc,&cou);

    for ( int i =0 ; i <cou; i++)
    {
        if (BF_GetBlock(ht_info->fileDesc,i, blockran)!=BF_OK)
        {
            fprintf(errorfile," minor error HT069 on %d\n",i);
        }
        if (BF_UnpinBlock( blockran)!=BF_OK)
        {
            fprintf(errorfile," minor error HT070 on %d\n",i);
        }
    }
    */
    //close file in buffer
    int k =BF_CloseFile(ht_info->fileDesc);
    if ( k!=BF_OK)
        {   
            BF_PrintError(k);
            fprintf( errorfile,"HT_ERROR5\n");
            return HT_ERROR;
        }
    
    // release memory allocated for buffer handler object
    free(ht_info);
    
    return HT_OK;
}
 



int HT_InsertEntry(HT_info* ht_info, Record record){

    // verification of file type
    if ( ht_info->fileType != HASHFILE)
        {fprintf( errorfile,"HT_ERROR6\n");return HT_ERROR;}
    fprintf(demofile,"\n Inserting Record :");
    printfileRecord(record);

    BF_Block* block0;
    BF_Block * blockvar;

    BF_Block_Init(&block0);
    BF_Block_Init(&blockvar);
    
    int fileIndex= ht_info->fileDesc;
    int indof =ht_info->indexOffset;
    int maximumrecs=ht_info->maxRecords;
    // now we get block0 at blockvar
    if (BF_GetBlock( fileIndex , 0 ,blockvar)!=BF_OK)
        {fprintf( errorfile,"HT_ERROR7\n");return HT_ERROR;}

    char* data0;
    data0=BF_Block_GetData(blockvar);


    HT_info dummy_info;

    memcpy(&dummy_info ,data0, sizeof(HT_info));



    int bucketsnum=dummy_info.numBuckets;
    int capacity=dummy_info.maxRecords;

    int hash_result= hash_mitsos_giorgos( record.id, bucketsnum);

    printf( " Hash result = %d \n",hash_result);
    fprintf( demofile," Hash result = %d \n",hash_result);
    
    char* newdata;

    HT_block_info index_info;

    index_info.recordsCounter=1;
    index_info.nextBlock=NULL;
    index_info.nextBlockID=-1;
    int counter=-1;

    int returnval=-1;

    if ( dummy_info .HT_INDEX_BUCKETS[hash_result]==-1)
    {// in case the bucket is empty of blocks 


        if ( BF_AllocateBlock ( fileIndex, blockvar)!=BF_OK)
            {fprintf( errorfile,"HT_ERROR8\n");return HT_ERROR;}

        newdata= BF_Block_GetData( blockvar);
        
        
        //cleanblock(newdata);
        
        
        BF_GetBlockCounter(fileIndex,&counter);
        counter--;
        index_info.blockNum=counter;

        memcpy( newdata +indof, &index_info , sizeof(HT_block_info));

        BF_Block_SetDirty(blockvar);

        memcpy( newdata , &record,sizeof(Record));
        dummy_info.HT_INDEX_PTRS[hash_result]=blockvar;
        dummy_info.HT_INDEX_BUCKETS[hash_result]=counter;

        BF_UnpinBlock(blockvar);
        //BF_Block_Destroy(&blockvar);


        //printf ( " dummy_info.HT_INDEX_BUCKETS[hash_result] =%d\n", dummy_info.HT_INDEX_BUCKETS[hash_result]);
        printf( " New Record inserted in bucket %d , a new block with id  %d is the container \n",hash_result, counter);
        fprintf( demofile," New Record inserted in bucket %d , a new block with id  %d is the container \n",hash_result, counter);
        returnval=counter;
    }
    else 
    {//in case the bucket already exists BF_Block* lastblock;
    BF_Block* lastblock;    
    BF_Block_Init ( &lastblock);
    
    char* lastdata;
                                                                                            
    BF_GetBlock( fileIndex, dummy_info.HT_INDEX_BUCKETS[hash_result],lastblock);

    
    lastdata=BF_Block_GetData(lastblock);
    
    //printf( " Bucket will be %d \n",hash_result);
    fprintf(demofile," Bucket will be %d \n",hash_result);

    
    HT_block_info lastblockindex;
    
    memcpy( &lastblockindex ,lastdata+ indof , sizeof(HT_block_info));
    

            if(lastblockindex.recordsCounter<maximumrecs)
            {// in case there is space in current last block of this bucket
                
                memcpy(lastdata+lastblockindex.recordsCounter*(sizeof(Record)), &record, sizeof(Record));
                
                lastblockindex.recordsCounter++;
                memcpy ( lastdata + indof , &lastblockindex.recordsCounter, sizeof(int));

                

                
                counter=dummy_info.HT_INDEX_BUCKETS[hash_result];
                returnval=counter;

                dummy_info.HT_INDEX_PTRS[hash_result]=blockvar;
                dummy_info.HT_INDEX_BUCKETS[hash_result]=counter;

                BF_Block_SetDirty( lastblock);
                BF_UnpinBlock( lastblock);
                BF_UnpinBlock(blockvar);
                //BF_Block_Destroy(&lastblock);
                

                //printf ( " dummy_info.HT_INDEX_BUCKETS[hash_result] =%d\n", dummy_info.HT_INDEX_BUCKETS[hash_result]);
                printf( " New Record inserted in bucket %d , block %d is the container \n",hash_result, lastblockindex.blockNum);
                fprintf( demofile," New Record inserted in bucket %d , block %d is the container \n",hash_result, lastblockindex.blockNum);
                
            }

            else
                {// in case current last block is full

                    
                    if ( BF_AllocateBlock ( fileIndex, blockvar)!=BF_OK)
                        {fprintf( errorfile,"HT_ERROR9\n");return HT_ERROR;}
                    newdata= BF_Block_GetData( blockvar);
                    
                    
                    //cleanblock(newdata);
                    
                    
                    BF_GetBlockCounter(fileIndex,&counter);
                    counter--;
                    index_info.blockNum=counter;
                    index_info.nextBlock=lastblock;
                    index_info.nextBlockID=lastblockindex.blockNum;

                    memcpy( newdata +indof, &index_info , sizeof(HT_block_info));
                    BF_Block_SetDirty(blockvar);
                    memcpy( newdata , &record,sizeof(Record));

                    dummy_info.HT_INDEX_PTRS[hash_result]=blockvar;
                    dummy_info.HT_INDEX_BUCKETS[hash_result]=counter;

                    
                    
                    returnval=counter;
                    
                    printf( " New Record inserted in bucket %d , block %d is the container \n",hash_result, counter);
                    fprintf(demofile," New Record inserted in bucket %d , block %d is the container \n",hash_result, counter);

                    BF_UnpinBlock(blockvar);
                    //BF_Block_Destroy(&blockvar);

                    BF_UnpinBlock(lastblock);
                    //BF_Block_Destroy(&lastblock);
                    


                }

                        
        }

    dummy_info.HT_INDEX_RECS[hash_result]++;
    // ενημερωση ht_info 
    *ht_info=dummy_info;
    memcpy(ht_info,&dummy_info,sizeof(HT_info));

    //print_bucket_index( ht_info->HT_INDEX_BUCKETS, bucketsnum);
    print_bucket_index(dummy_info.HT_INDEX_BUCKETS,bucketsnum);
    if (returnval>=0)
        fprintf(demofile,"Succesfoul insert of :\n");
    else
        {   fprintf( demofile," FAIL Insert %d \n",record.id);
            fprintf( errorfile," FAIL Insert %d \n",record.id);
        }
    
    printfileRecord(record);

    fprintf(demofile,"___________________\n");


    memcpy( data0 , &dummy_info, sizeof( HT_info));

    BF_Block_SetDirty( block0);
    BF_UnpinBlock(block0);
    //BF_Block_Destroy(&block0);


    return returnval;

}

int HT_GetAllEntries(HT_info* ht_info, void *value ){
    // prints all entries under the specified key
    // not only primary key thus can be used 
    // in real data only for secondary indexes.
    // for generated data should be used as professors declared
    // to make autograding easier ( IDs == naturals inserted increasingly)
    // ( and generally works for primary keys if the outside source-main ensures uniqueness)

    printf("\n\nRUN PrintAllEntries\n\n");
    if ( ht_info->fileType != HASHFILE)
        {fprintf( errorfile,"HT_ERROR10\n");return HT_ERROR;}

    fprintf(demofile,"\n\nRUN PrintAllEntries\n");
    
    //cast key ## may lose hashable info here
    int* keyset=value;
    printf(" Key = %d \n",*keyset);
    fprintf(demofile," Key = %d \n",*keyset);

    
    
    //BF_Block* block0;
    BF_Block * blockvar;

    //BF_Block_Init(&block0);
    BF_Block_Init(&blockvar);
    int fileIndex= ht_info->fileDesc;
    if (BF_GetBlock( fileIndex , 0 ,blockvar)!=BF_OK)
        {fprintf( errorfile,"HT_ERROR11\n");return HT_ERROR;}

    char* data0;
    data0=BF_Block_GetData(blockvar);
    HT_info dummy_info;
    memcpy(&dummy_info ,data0, sizeof(HT_info));


    int indof =dummy_info.indexOffset;
    int bucketsnum=dummy_info.numBuckets;
    int capacity=dummy_info.maxRecords;

    
    // get hash value ,works with this from now on
    int hash_result= hash_mitsos_giorgos(* keyset, bucketsnum);

    int last_appearence_ofkey=-1;
    int blocks_inbuck=0;
    int total_appear=0;

    printf( " Hash result = %d \n",hash_result);
    fprintf( demofile," Hash result = %d \n",hash_result);


       if ( dummy_info .HT_INDEX_BUCKETS[hash_result]==-1){
        // in case the bucket has not entries

        printf( "This person does not exist in this file !\n\n");
        fprintf(demofile,"Key [%d] does not exist in this file !\n\n",*keyset);


        
        BF_UnpinBlock(blockvar);
        
        //BF_UnpinBlock(block0);
        
        return 0;

        }

        else{// in case bucket has at least 1 entry

            fprintf(demofile,"Searching bucket [ %d ] ...\n",hash_result );
            printf("\nSearching bucket [ %d ] ...\n",hash_result );

            char* currentblock;

                                            BF_UnpinBlock(blockvar);                                                   
            BF_GetBlock( fileIndex, dummy_info.HT_INDEX_BUCKETS[hash_result],blockvar);            
            currentblock=BF_Block_GetData(blockvar);
            HT_block_info index_var;
            memcpy(&index_var,currentblock + indof,sizeof(HT_block_info));
            int j =0;
            int temp_key=-1;
            int fl=0;

            // searching this bucket's list block by block
            while ( index_var.blockNum>0)
                {//just initial check
                 // flow is controled at the end

                        printf("Block [%d] :",index_var.blockNum);
                        fprintf(demofile,"Block [%d] :\n",index_var.blockNum);

                        //searching block now
                        for ( j=0 ; j<index_var.recordsCounter;j++ )
                            {
                                    memcpy( &temp_key , currentblock + ( j * sizeof(Record) ) + (16* sizeof(char) ) ,
                                     sizeof(int) );

                                    fprintf(demofile,"      Record [%d]\n",j);
                                    if ( temp_key==*keyset)
                                    {
                                        Record toprintRec;   
                                        memcpy(&toprintRec, currentblock + ( j*sizeof(Record) ),sizeof(Record));
                                        printfileRecord(toprintRec);
                                        last_appearence_ofkey=index_var.blockNum;
                                        total_appear++;
                                        fl=1;

                                    }
                            }
                        if (fl)
                        {
                            printf(" Found \n");

                        }
                        else
                        {
                            printf(" Not Found \n");
                        }
                        if( index_var.nextBlockID>0)
                            {// block list has more
                                BF_UnpinBlock(blockvar);
                                if (BF_GetBlock(fileIndex,index_var.nextBlockID,blockvar)!=BF_OK)
                                    {   
                                        BF_UnpinBlock(blockvar);
                                        fprintf(errorfile,"HT_ERROR14\n");
                                        return HT_ERROR;
                                    }
                                currentblock=BF_Block_GetData(blockvar);
                                memcpy(&index_var,currentblock + indof,sizeof(HT_block_info));
                                blocks_inbuck++;
                                fl=0;
                                //printf("index_var.nextBlockID = %d , index_var.blockNum = %d \n",index_var.nextBlockID,index_var.blockNum);
                            }
                        else{// last block in list
                                blocks_inbuck++;
                                
                                break;
                            }

                }
            

            printf( "Searched %d blocks ,found entry last time at block %d  .Total appearances : %d\n"
                ,blocks_inbuck,last_appearence_ofkey,total_appear);

            BF_UnpinBlock(blockvar);
            //BF_UnpinBlock(block0);
            

            return blocks_inbuck;
            }
}

int HashStatistics( char* filename)
{
    BF_Block* block0;
    BF_Block_Init(&block0);
    int fileIndex;

    if(BF_OpenFile(filename, &fileIndex) != BF_OK)
        {fprintf( errorfile,"HT_ERROR29\n");return HT_ERROR;}
    
    if ( BF_GetBlock( fileIndex, 0, block0)!=BF_OK )
        {return -1;}
    char* data0;
    data0=BF_Block_GetData(block0);
    
    if (data0==NULL)
        {
            fprintf(errorfile," ErrorStatistics\n");
            return -1;
        }
    HT_info hinfo;
    memcpy( &hinfo, data0,sizeof(HT_info));
    printf(" _______________________   ");
    printf("\n    Stats of file :   %s \n",filename);
    printf(" _______________________   \n");


    int average_blocks_perbucket[hinfo.numBuckets];
    int min=616614146;
    int max=0;
    int sum_blocks=0;
    int sum_records=0;
    int ovrfl_buckets_counter=0;

    for ( int i =0 ; i < hinfo.numBuckets; i++)
    {// for every bucket
        int max_block=-1;
        
        int recs=0;
        max_block=hinfo.HT_INDEX_BUCKETS[i];
       
        if ( max_block >0)
            {   
                recs =hinfo.HT_INDEX_RECS[i];
                max_block= recs/hinfo.maxRecords +1;

                printf( "Bucket [%d] contains %d records  in %d  blocks . ", i , recs ,max_block);
                if ( max_block>1)
                    {
                        printf( " %d are overflowing blocks\n", max_block-1);
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
    

    printf( " Average |records_per_bucket| = %0.2lf \n",(double)sum_records/hinfo.numBuckets);

    printf(" Maximum |records_per_bucket| = %d\n",max);

    printf(" %d of its' buckets are overflowing \n\n\n",ovrfl_buckets_counter);

    BF_UnpinBlock(block0);
    if ( BF_CloseFile(fileIndex)!=BF_OK)
        {fprintf( errorfile,"HT_ERROR95\n");return HT_ERROR;}
    
    
    return 0;

}


void userinpt()
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
                    printf( " Size of ht block info:  %lu\n" ,sizeof(HT_block_info));
                    printf( " Size of Record: %lu\n" ,sizeof(Record));
                    printf( " Size of usable space per block: %lu / %d  bytes\n",maxrecords*sizeof(Record),BF_BLOCK_SIZE);
                    printf( " Records capacity per block : %d\n\n" ,maxrecords);

                    fl=1;
                    continue;
              default:
                    fl=0;
                    break;

          }

    }
}

