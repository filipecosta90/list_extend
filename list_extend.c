 /*************************************************************************
 * 
 * Redis 5.X under the hood: 3 - Writing a Redis Module
 * ____________________________________________________
 * 
 *  [2019] @fcosta_oliveira 
 *  
 */

#include "redismodule.h"
#include <limits.h>

int ListExtendFilter_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // LIST_EXTEND.FILTER source_list destination_list low_value high_value
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  // Open field/value list keys
  RedisModuleKey *sourceListKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  int sourceListKeyType = RedisModule_KeyType(sourceListKey);

  if (sourceListKeyType != REDISMODULE_KEYTYPE_LIST &&
      sourceListKeyType != REDISMODULE_KEYTYPE_EMPTY) {
    RedisModule_CloseKey(sourceListKey);
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  // Open destination key
  RedisModuleKey *destinationListKey = RedisModule_OpenKey(ctx, argv[2], REDISMODULE_WRITE);
  RedisModule_DeleteKey(destinationListKey);

  // Get length of list
  size_t sourceListLength = RedisModule_ValueLength(sourceListKey);

  if (sourceListLength == 0) {
    RedisModule_ReplyWithLongLong(ctx, 0L);
    return REDISMODULE_OK;
  }

  /* Rotate and increment. */
  size_t added = 0;
  for (size_t pos = 0; pos < sourceListLength; pos++) {
    RedisModuleString *ele = RedisModule_ListPop(sourceListKey, REDISMODULE_LIST_TAIL);
    RedisModule_ListPush(sourceListKey, REDISMODULE_LIST_HEAD, ele);
    char strLowerLimit[] = "-inf";
    char strUpperLimit[] = "+inf";
    long long val;
    long long lowerLimit;
    long long upperLimit;
    RedisModuleString *expectedMinusInf, *expectedPlusInf;
    expectedMinusInf = RedisModule_CreateString(ctx, strLowerLimit, 4);
    expectedPlusInf = RedisModule_CreateString(ctx, strUpperLimit, 4);
    int lowerLimitOk = 0;
    int upperLimitOk = 0;
    char *eptr;

    lowerLimit = strtol(RedisModule_StringPtrLen(argv[3], NULL), &eptr, 10);

    if (RedisModule_StringCompare(argv[3], expectedMinusInf) == 0) {
      lowerLimit = LONG_MIN;
      lowerLimitOk = 1;
    } else {
      if (RedisModule_StringToLongLong(argv[3], &lowerLimit) == REDISMODULE_OK)
        lowerLimitOk = 1;
    }

    if (RedisModule_StringCompare(argv[4], expectedPlusInf) == 0) {
      upperLimit = LONG_MAX;
      upperLimitOk = 1;
    } else {
      if (RedisModule_StringToLongLong(argv[4], &upperLimit) == REDISMODULE_OK)
        upperLimitOk = 1;
    }
    if ((RedisModule_StringToLongLong(ele, &val) == REDISMODULE_OK) &&
        lowerLimitOk == 1 && upperLimitOk == 1) {
      if (val >= lowerLimit && val <= upperLimit) {
        
        // RedisModule_Log(ctx, "notice", "element within range %d %d %d", lowerLimit, val, upperLimit );
        RedisModuleString *newele = RedisModule_CreateStringFromLongLong(ctx, val);

        // push to destination_list
        if (RedisModule_ListPush(destinationListKey, REDISMODULE_LIST_HEAD, newele) == REDISMODULE_ERR) {
          return REDISMODULE_ERR;
        }
        added++;
      }
    }
  }

  RedisModule_ReplyWithLongLong(ctx, added);

  // propagate the command to the slaves and AOF file exactly as it was called
  RedisModule_ReplicateVerbatim(ctx);

  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (RedisModule_Init(ctx, "list_extend", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  // "write": The command may modify the data set (it may also read from it).
  // "deny-oom": The command may use additional memory and should be denied during out of memory conditions.
  if (RedisModule_CreateCommand(ctx, "list_extend.filter", ListExtendFilter_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}