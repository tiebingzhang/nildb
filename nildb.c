/* nildb
 *
 */

#define _FILE_OFFSET_BITS 64

#include "nildb.h"

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/stat.h>
#define __USE_UNIX98
#include <unistd.h>

#define SIZE_OFFSET sizeof(uint32_t)
#define NILDB_HEADER_SIZE ((SIZE_OFFSET * 3) + 4)
#define META_SIZE	(1+SIZE_OFFSET)
#define NULL_OFFSET	0xFFFFFFFF

/* djb2 hash function */
static uint64_t nildb_hash(const void *b,unsigned long len)
{
	unsigned long i;
	uint64_t hash = 5381;
	for(i=0;i<len;++i)
		hash = ((hash << 5) + hash) + (uint64_t)(((const uint8_t *)b)[i]);
	return hash;
}

int nildb_open( nildb *db, const char *path, uint32_t hash_table_size, uint32_t key_size, uint32_t value_size) {
	uint8_t buf[128];
	uint32_t tmp32;

	if ((!hash_table_size)||(!key_size)||(!value_size)) {
		printf("Invalid hash_table_size:%u or key_size:%u or value_size:%u\n",hash_table_size,key_size,value_size);
		return -1;
	}
	db->f = fopen(path,"r+b");
	if (!db->f) {
		int i;
		db->f = fopen(path,"w+b");
		if (!db->f)
			return NILDB_ERROR_IO;

		/* write header if not already present */
		buf[0] = 'N'; buf[1] = 'D'; buf[2] = 'B'; buf[3] = NILDB_VERSION;
		tmp32 = htobe32(hash_table_size);
		memcpy(buf+4,&tmp32,4);
		tmp32 = htobe32(key_size);
		memcpy(buf+8,&tmp32,4);
		tmp32 = htobe32(value_size);
		memcpy(buf+12,&tmp32,4);
		if (fwrite(buf,4,4,db->f) != 4) { 
			fclose(db->f); 
			return NILDB_ERROR_IO; 
		}
		for (i=0;i<hash_table_size;i++){
			tmp32=htobe32(NULL_OFFSET);
			fwrite(&tmp32,SIZE_OFFSET,1,db->f);
		}
		fflush(db->f);
	}else{
		if (fread(buf,4,4,db->f) != 4) { 
			printf("Error reading header. Invalid database file\n");
			fclose(db->f); 
			return NILDB_ERROR_IO; 
		}
		if ((buf[0] != 'N')||(buf[1] != 'D')||(buf[2] != 'B')||(buf[3] != NILDB_VERSION)) {
			printf("Error, Invalid magic header. Invalid database file\n");
			fclose(db->f);
			return NILDB_ERROR_CORRUPT_DBFILE;
		}
		memcpy(&tmp32,buf+4,4);
		hash_table_size = be32toh(tmp32);
		//printf("hash_table_size=%d\n",hash_table_size);

		memcpy(&tmp32,buf+8,4);
		key_size = be32toh(tmp32);

		memcpy(&tmp32,buf+12,4);
		value_size = be32toh(tmp32);
	}

	db->hash_table_size = hash_table_size;
	db->key_size = key_size;
	db->value_size = value_size;
	db->hash_table_size_bytes = SIZE_OFFSET * hash_table_size;
	printf("keysize=%d value_size=%d\n",key_size,value_size);

	return 0;
}

void nildb_close(nildb *db) {
	if (db->f)
		fclose(db->f);
	memset(db,0,sizeof(nildb));
}

int nildb_get(nildb *db,const void *key,void *vbuf) {
	uint8_t buf[4096];
	unsigned long klen;
	uint32_t hash_index= (uint32_t)(nildb_hash(key,db->key_size) % (uint64_t)db->hash_table_size);
	off_t offset;
	size_t n;
	uint32_t tmp32,offset32;
	int fd=fileno(db->f);

	/* read the first data location */
	offset = NILDB_HEADER_SIZE + (uint64_t)hash_index*SIZE_OFFSET;
	n = pread(fd,&tmp32,SIZE_OFFSET,offset);
	if (n<SIZE_OFFSET){
		printf("Error reading first data loation.\n");
		return -1;
	}

	offset32=be32toh(tmp32);
	if (offset32==NULL_OFFSET){
		printf("No entry found\n");
		return -1;
	}

	klen = db->key_size;

	offset=NILDB_HEADER_SIZE + db->hash_table_size_bytes;
	while(1){
		offset += offset32;

		n = pread(fd,buf,META_SIZE+klen,offset);
		if (n<(META_SIZE+klen)){
			perror("pread get");
			return -1; // not found
		}

		memcpy(&tmp32,buf+1,SIZE_OFFSET);
		offset32=be32toh(tmp32);

		if ((buf[0]&0x01)==0x01 && memcmp(key,buf+META_SIZE,klen)==0){
			offset += n;
			if (pread(fd,vbuf,db->value_size,offset) == db->value_size)
				return 0; /* success */
			else{
				perror("pread 2 get");
				return -1;
			}
		}else{
			if (offset32==0){
				//printf("get: reached end\n");
				return -1;
			}
		}
	}
}

int delete_entry(nildb *db){
	uint8_t buf[4];
	buf[0]=0;
	fwrite(buf,1,1,db->f);
	fflush(db->f);
	return 0;
}

/* write an entry to the current file position.
 * if update_next_offset is 0, the offset field will not be written*/
int write_entry(nildb *db,const void *key,const void *value, int update_next_offset,uint32_t next_offset_be){
	uint8_t buf[8];
	buf[0]=0x01;
	if (update_next_offset){
		memcpy(buf+1,&next_offset_be,SIZE_OFFSET);
		fwrite(buf,1,5,db->f);
	}else{
		fwrite(buf,1,1,db->f);
		fseek(db->f,SIZE_OFFSET,SEEK_CUR);
	}
	fwrite(key,1,db->key_size,db->f);
	fwrite(value,1,db->value_size,db->f);
	fflush(db->f);
	return 0;
}

/* append an entry and update the parent next offset field */
int append_entry(nildb *db,const void *key,const void *value,uint64_t parent_offset){
	int n;
	off_t start_offset = NILDB_HEADER_SIZE + db->hash_table_size_bytes;
	fseeko(db->f,0,SEEK_END);
	off_t end_offset = ftello(db->f);
	write_entry(db,key,value,1,0);

	if (parent_offset < start_offset){
		/* update the offset in the hash table */
		uint32_t parent_next_offset_be=htobe32(end_offset-start_offset);
		n=pwrite(fileno(db->f),&parent_next_offset_be,SIZE_OFFSET,parent_offset);
	}else{
		/* update the offset in the link list */
		uint32_t parent_next_offset_be=htobe32(end_offset-parent_offset);
		n=pwrite(fileno(db->f),&parent_next_offset_be,SIZE_OFFSET,parent_offset + 1);
	}
	if (n!=SIZE_OFFSET){
		printf("Error updating offset.\n");
		return -1;
	}
	fflush(db->f);
	return 0;
}

static int do_nildb_put(nildb *db,const void *key,const void *value, int delete) {
	uint8_t buf[4096];
	const uint8_t *kptr;
	unsigned long klen;
	uint32_t hash_index=(nildb_hash(key,db->key_size) % (uint64_t)db->hash_table_size);
	off_t parent_offset;
	size_t n;
	uint32_t tmp32,offset32;
	int fd=fileno(db->f);


	/* read the first has index*/
	parent_offset = NILDB_HEADER_SIZE + hash_index*SIZE_OFFSET;
	n = pread(fd,&tmp32,SIZE_OFFSET,parent_offset);
	if (n<SIZE_OFFSET){
		printf("Error reading first data loation.\n");
		return -1;
	}
	offset32=be32toh(tmp32);
	if (offset32==NULL_OFFSET){
		if (!delete){
			append_entry(db,key,value,parent_offset);
		}
		return 0;
	}

	kptr = (const uint8_t *)key;
	klen = db->key_size;

	parent_offset =  NILDB_HEADER_SIZE + db->hash_table_size_bytes;
	uint64_t freeslot=0;
	while(1){
		parent_offset += offset32;

		n = (long)pread(fd,buf,META_SIZE+klen,parent_offset);
		if (n<(META_SIZE+klen)){
			perror("put fread");
			return -1; // not found
		}

		memcpy(&tmp32,buf+1,SIZE_OFFSET);
		offset32=be32toh(tmp32);

		if ((buf[0]&0x01)==0x01){
			//printf("found active entry\n");
			if (memcmp(kptr,buf+META_SIZE,klen)==0){
				//update, leave offset alone
				//printf("same key, update it, delete=%d\n",delete);
				fseeko(db->f,parent_offset,SEEK_SET);
				if (delete){
					delete_entry(db);
				}else{
					write_entry(db,key,value,0,0);
				}
				return 0;
			}else{
				//printf("not our key\n");
				if (offset32==0){ //last entry. update or append to the end of file, and update this index
					if (delete)
						return 0;
					if (freeslot==0){
						//printf("this is the end, append\n");
						append_entry(db,key,value,parent_offset);
					}else{
						//printf("this is the end, write to the first free slot\n");
						fseeko(db->f,freeslot,SEEK_SET);
						write_entry(db,key,value,0,0);
					}
					return 0;
				}else{
					//find next entry
					continue;
				}
			}
		}else{
			if (freeslot==0){
				freeslot=parent_offset;
			}
		}
	}
}
int nildb_put(nildb *db,const void *key,const void *value) {
	return do_nildb_put(db,key,value,0);
}
int nildb_delete(nildb *db,const void *key) {
	return do_nildb_put(db,key,NULL,1);
}

#ifdef NILDB_TEST

#include <inttypes.h>
#define NUMENTRY 20000
int main(int argc,char **argv)
{
	uint64_t i,j;
	uint64_t v[8];
	nildb db;
	int q;

	printf("Opening new empty database test.db...\n");

	if (nildb_open(&db,"test.db",1024,8,sizeof(v))) {
		printf("nildb_open failed\n");
		return 1;
	}

	printf("Adding and then re-getting NUMENTRY 64-byte values...\n");

	for(i=1;i<=NUMENTRY;++i) {
		for(j=0;j<8;++j)
			v[j] = i;
		//printf("adding %lld\n",i);
		if (nildb_put(&db,&i,v)) {
			printf("nildb_put failed (%"PRIu64")\n",i);
			return 1;
		}
	}

	for(i=1;i<=NUMENTRY;++i) {
		//printf("reading %lld\n",i);
		if ((q = nildb_get(&db,&i,v))) {
			printf("nildb_get (1) failed (%"PRIu64") (%d)\n",i,q);
			return 1;
		}
		for(j=0;j<8;++j) {
			if (v[j] != i) {
				printf("nildb_get (1) failed, bad data (%"PRIu64")\n",i);
				return 1;
			}
		}
	}

	nildb_close(&db);
	printf("All tests OK!\n");
	return 0;
}
#endif
