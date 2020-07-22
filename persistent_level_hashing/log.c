#include "log.h"
#include "level_hashing.h"
/*
Function: log_create() 
        Create a log;
*/
static TOID(level_log)
log_create(uint64_t log_length, TOID(struct root) r)
{
    TOID(log_entry) entry;
    TOID(log_entry_insert) entry_insert;

    r->level_log_r = TX_ZNEW(level_log);
    level_log *log = D_RW(r->level_log_r);
    //level_log* log = pmalloc(sizeof(level_log));
    if (!log)
    {
        printf("Log creation fails: 1\n");
        exit(1);
    }
    entry = TX_ZNEW(log_length*sizeof(log_entry));
    log->entry = D_RW(entry);
    //log->entry = pmalloc(log_length*sizeof(log_entry));
    if (!log->entry)
    {
        printf("Log creation fails: 2");
        exit(1);
    }

    log->log_length = log_length;
    log->current = 0;

    entry_insert = TX_ZNEW(log_length*sizeof(log_entry_insert));
    log->entry_insert = D_RW(entry_insert);
    //log->entry_insert = pmalloc(log_length*sizeof(log_entry_insert));
    if (!log->entry_insert)
    {
        printf("Log creation fails: 3");
        exit(1);
    }

    log->current_insert= 0;
    
    return log;
}

/*
Function: log_write() 
        Write a log entry;
*/
void log_write(level_log *log, uint8_t *key, uint8_t *value)
{
    memcpy(log->entry[log->current].key, key, KEY_LEN);
    memcpy(log->entry[log->current].value, value, VALUE_LEN);
    pmemobj_persist((uint64_t *)&log->entry[log->current].key, 8);
    pmemobj_persist((uint64_t *)&log->entry[log->current].value, 8);
    asm_mfence();
    
    log->entry[log->current].flag = 1;
    pmemobj_persist((uint64_t *)&log->entry[log->current].flag, 8);
    asm_mfence();
}

/*
Function: log_clean() 
        Clean up a log entry;
*/
void log_clean(level_log *log)
{
    log->entry[log->current].flag = 0;
    pmemobj_persist((uint64_t *)&log->entry[log->current].flag, 8);
    asm_mfence();

    log->current ++;
    if(log->current == log->log_length)
        log->current = 0;
    pmemobj_persist((uint64_t *)&log->current, 8);
    asm_mfence();
}

/*
Function: log_insert_write() 
        Write an entry in the insert log;
*/
void log_insert_write(level_log * log,log_entry_insert entry)
{
    log->entry_insert[log->current_insert] = entry;
    pmemobj_persist((uint64_t *)&log->entry_insert[log->current_insert], 8);
    asm_mfence();
}

/*
Function: log_insert_sclean() 
        Clean up an entry in the insert log;
*/
void log_insert_clean(level_log *log)
{
    log->entry_insert[log->current_insert].flag = 0;
    pmemobj_persist((uint64_t *)&log->entry_insert[log->current_insert], 8);
    asm_mfence();

    log->current_insert++;
    if(log->current_insert== log->log_length)
        log->current_insert= 0;
    pmemobj_persist((uint64_t *)&log->current_insert, 8);
    asm_mfence();
}