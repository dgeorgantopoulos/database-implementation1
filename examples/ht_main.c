#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include <sys/time.h>


#define RECORDS_NUM 200 // you can change it if you want
#define FILE_NAME "data.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }


int main() {

  errorfile=fopen("error.txt","a+");
  demofile=fopen("demo.txt","a+");

  BF_Init(LRU);

  HT_CreateFile(FILE_NAME,10);
  HT_info* info = HT_OpenFile(FILE_NAME);

  Record record;
  srand(12569874);
  int returntype;
  printf("Insert Entries\n\n\n");

  for (int id = 0; id < RECORDS_NUM; ++id) {
    record = randomRecord();
    printf("\n Inserting Record :");
    

    printRecord(record);
    

    if ( (returntype =HT_InsertEntry(info, record) ) <=0)
      printf( " FAIL Insert of \n");
    else
      printf( " Succesfoul insert of :\n");
     
    printRecord(record);
    printf("___________________________\n");
      
    
    
  }

  for(  int i =0; i<6 ; i++) 
  {
    int id = rand() % RECORDS_NUM;
    HT_GetAllEntries(info, &id);
  }

  
  

  

  HashStatistics(FILE_NAME);

  userinpt();
  

  returntype=HT_CloseFile(info);
  printf( "\nHT_CloseFile returned  ");printfileexit(returntype);printf("  <====\n\n");
  fclose(errorfile);
  fclose(demofile);
  BF_Close();
}

 