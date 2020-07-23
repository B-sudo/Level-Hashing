#include <stdio.h>
#include <string.h>
#include <libpmemobj.h>
 
#define LAYOUT_NAME "intro_1"
#define MAX_BUF_LEN 10
 
struct my_root {
	size_t len;
	char buf[MAX_BUF_LEN];
};
 
int
main(int argc, char *argv[])
{
	if (argc != 2) {
		printf("usage: %s file-name\n", argv[0]);
		return 1;
	}
    // 创建内存池
	PMEMobjpool *pop = pmemobj_create(argv[1], LAYOUT_NAME,
				PMEMOBJ_MIN_POOL, 0666);
 
	if (pop == NULL) {
		perror("pmemobj_create");
		return 1;
	}
    // 创建根对象
	PMEMoid root = pmemobj_root(pop, sizeof(struct my_root));
    // 获取根对象的直接指针
	struct my_root *rootp = pmemobj_direct(root);
 
	char buf[MAX_BUF_LEN] = {0};
	if (scanf("%9s", buf) == EOF) {
		fprintf(stderr, "EOF\n");
		return 1;
	}
 
	rootp->len = strlen(buf);
    // 持久化长度
	pmemobj_persist(pop, &rootp->len, sizeof(rootp->len));
    // 持久化内容
	pmemobj_memcpy_persist(pop, rootp->buf, buf, rootp->len);
    // 关闭pool
	pmemobj_close(pop);
	return 0;
}