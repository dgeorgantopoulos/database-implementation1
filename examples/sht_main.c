#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"



//########CONFIG HERE#######
#define RECORDS_NUM 500 // you can change it if you want
#define FILE_NAME "data.db"
#define INDEX_NAME "index.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }


int main() {
    
    if ( RECORDS_NUM > 500000)
    {
      printf( " Tooo many records asked try up to 500000 ,remember it is a demo prgramm and our time valuable\n");
      return -1;
    }
    errorfile=fopen("error.txt","a+");
    demofile=fopen("demo.txt","a+");
    //secondarydemofile=fopen("secondarydemo.txt","a+");

    BF_Init(LRU);
    // Αρχικοποιήσεις
    
    //########CONFIG HERE#######
    HT_CreateFile(FILE_NAME,10);
    HT_info* info = HT_OpenFile(FILE_NAME);


    //########CONFIG HERE#######
    SHT_CreateSecondaryIndex(INDEX_NAME,10,FILE_NAME);
    SHT_info* index_info = SHT_OpenSecondaryIndex(INDEX_NAME);


    // Θα ψάξουμε στην συνέχεια το όνομα searchName
    srand(12569874);
    Record record=randomRecord();
    char searchName[15];
    strcpy(searchName, record.name);

    

    
    // Κάνουμε εισαγωγή τυχαίων εγγραφών τόσο στο αρχείο κατακερματισμού τις οποίες προσθέτουμε και στο δευτερεύον ευρετήριο
    printf("\nInsert Entries\n");
    for (int id = 0; id < RECORDS_NUM; ++id) {
        printf( " ************ \n");
        record = randomRecord();
        int block_id = HT_InsertEntry(info, record);
        SHT_SecondaryInsertEntry(index_info, record, block_id);
        printf("\n");
    }


    // Τυπώνουμε όλες τις εγγραφές με όνομα searchName
    printf("RUN PrintAllEntries for name %s\n",searchName);
    SHT_SecondaryGetAllEntries(info,index_info,searchName);
      
    //printf( " record capacity %d\n",index_info->maxRecords);
    // Κλείνουμε το αρχείο κατακερματισμού και το δευτερεύον ευρετήριο
    //printf(" h filedesc = %d sht filedec =%d", info->fileDesc,index_info->fileDesc);

    int returntype;
    returntype=SHT_CloseSecondaryIndex(index_info);
    printf( "\nSHT_CloseFile returned  S");printfileexit(returntype);printf("  <====\n\n");
    

    returntype=HT_CloseFile(info);
    printf( "\nHT_CloseFile returned  ");printfileexit(returntype);printf("  <====\n\n");
    
    

    
    sHashStatistics(INDEX_NAME);

    //
    BF_Close();
    

    userinptsht();
    fclose(errorfile);
    fclose(demofile);
    
}
