#include "pmemobj_test.h"


#define MIN_LEVEL_SIZE 1024

POBJ_LAYOUT_BEGIN(levelhash);
POBJ_LAYOUT_ROOT(levelhash, struct my_root);
POBJ_LAYOUT_END(levelhash);
/*
Function: F_HASH()
        Compute the first hash value of a key-value item
*/
uint64_t F_HASH(level_hash *level, const uint8_t *key) {
    return (hash((void *)key, strlen(key), level->f_seed));
}

/*
Function: S_HASH() 
        Compute the second hash value of a key-value item
*/
uint64_t S_HASH(level_hash *level, const uint8_t *key) {
    return (hash((void *)key, strlen(key), level->s_seed));
}

/*
Function: F_IDX() 
        Compute the second hash location
*/
uint64_t F_IDX(uint64_t hashKey, uint64_t capacity) {
    return hashKey % (capacity / 2);
}

/*
Function: S_IDX() 
        Compute the second hash location
*/
uint64_t S_IDX(uint64_t hashKey, uint64_t capacity) {
    return hashKey % (capacity / 2) + capacity / 2;
}

/*
Function: generate_seeds() 
        Generate two randomized seeds for hash functions
*/
void generate_seeds(level_hash *level)
{
    srand(time(NULL));
    do
    {
        level->f_seed = rand();
        level->s_seed = rand();
        level->f_seed = level->f_seed << (rand() % 63);
        level->s_seed = level->s_seed << (rand() % 63);
    } while (level->f_seed == level->s_seed);
}

/*
Function: is_in_one_cache_line
          determine whether x and y are in the same cache line
*/
static inline bool is_in_one_cache_line(void* x, void* y)
{
    if((char*)x + 64 <= (char*)y)
    {
        return false;
    }
    else
    {
        return true;
    }
}

/*
Function: level_slot_flush
          write the j-th slot in the bucket
*/
static inline void level_slot_flush(PMEMobjpool *pop, level_bucket* bucket, uint64_t j)
{
    // When the key-value item and token are in the same cache line, only one flush is acctually executed.
    if(is_in_one_cache_line(&bucket->slot[j], &bucket->token))
    {
        SET_BIT(bucket->token, j, 1);
        pmemobj_persist(pop, (uint64_t *)&bucket->slot[j].key, 8);
        pmemobj_persist(pop, (uint64_t *)&bucket->slot[j].value, 8);
    }
    else
    {
        pmemobj_persist(pop, (uint64_t *)&bucket->slot[j].key, 8);
        pmemobj_persist(pop, (uint64_t *)&bucket->slot[j].value, 8);
        asm_mfence();
        SET_BIT(bucket->token, j, 1);                   
    }
    pmemobj_persist(pop, (uint64_t *)&bucket->token, 8);
}

/*
Function: level_init() 
        Initialize a level hash table
*/
static TOID(level_hash)
level_init(PMEMobjpool *pop, uint64_t level_size, TOID(struct root) r)
{
    TOID(level_bucket) bucket;

    r->level_hash_r = TX_ZNEW(level_hash);
    level_hash *level = D_RW(r->level_hash_r);
    //level_hash *level = pmalloc(sizeof(level_hash));
    if (!level)
    {
        printf("The level hash table initialization fails:1\n");
        exit(1);
    }

    level->level_size = level_size;
    level->addr_capacity = pow(2, level_size);
    level->total_capacity = pow(2, level_size) + pow(2, level_size - 1);
    generate_seeds(level);

    bucket = TX_ZNEW(pow(2, level_size)*sizeof(level_bucket));
    level->buckets[0] = D_RW(bucket);
    //level->buckets[0] = pmalloc(pow(2, level_size)*sizeof(level_bucket));
    bucket = TX_ZNEW(pow(2, level_size - 1)*sizeof(level_bucket));
    level->buckets[1] = D_RW(bucket);
    //level->buckets[1] = pmalloc(pow(2, level_size - 1)*sizeof(level_bucket));

    level->interim_level_buckets = NULL;
    level->level_item_num[0] = 0;
    level->level_item_num[1] = 0;
    level->level_expand_time = 0;
    level->resize_state = 0;

    if (!level->buckets[0] || !level->buckets[1])
    {
        printf("The level hash table initialization fails:2\n");
        exit(1);
    }
    //TODO
    level->log = log_create(1024, r);

    uint64_t *ptr = (uint64_t *)&level;
    for(; ptr < (uint64_t *)&level + sizeof(level_hash); ptr += 8)
        pmemobj_persist(pop, (uint64_t *)&ptr, 8);

    printf("Level hashing: ASSOC_NUM %d, KEY_LEN %d, VALUE_LEN %d \n", ASSOC_NUM, KEY_LEN, VALUE_LEN);
    printf("The number of top-level buckets: %ld\n", level->addr_capacity);
    printf("The number of all buckets: %ld\n", level->total_capacity);
    printf("The number of all entries: %ld\n", level->total_capacity*ASSOC_NUM);
    printf("The level hash table initialization succeeds!\n");
    return level;
}

int main(int argc, char* argv[])                        
{
    PMEMobjpool *pop = pmemobj_create(argv[1], POBJ_LAYOUT_NAME(levelhash), PMEMOBJ_MIN_POOL, 0666);
	if (pop == NULL) {
		perror("pmemobj_create");
		return 1;
	}
    TOID(struct root) r = POBJ_ROOT(pop, struct root);


    int level_size = atoi(argv[1]);                     // INPUT: the number of addressable buckets is 2^level_size
    int insert_num = atoi(argv[2]);                     // INPUT: the number of items to be inserted
    int write_latency = atoi(argv[3]);                  // INPUT: the injected write latency
    
    level_hash *level = level_init(pop, level_size, r);

    pmemobj_close(pop);
    return 0;
}