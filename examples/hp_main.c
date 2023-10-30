#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hp_file.h"

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

  BF_Init(LRU);

  HP_CreateFile(FILE_NAME);
  HP_info* info = HP_OpenFile(FILE_NAME);

  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    record = randomRecord();
    printf ( " #######   RECORD    [ %d  ] \n", id+1);
    //printf( " old last block id = %d \n",info->lastBlockid);
    HP_InsertEntry(info, record);
    //printf( " new last block id = %d \n",info->lastBlockid);
  }

  printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;
  printf("\nSearching for: %d\n",id);
  
  HP_GetAllEntries ( info, id);
  

  printf( "Hp_CloseFile returned  ==== ");
  printfileexit(HP_CloseFile(info));
  printf( "\n");

  BF_Close();
}
