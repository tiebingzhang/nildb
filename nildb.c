/* nildb
 *
 */

#define _FILE_OFFSET_BITS 64

#include "nildb.h"

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/stat.h>

#ifdef _WIN32
#define fseeko _fseeki64
#define ftello _ftelli64
#endif

#define SIZE_OFFSET sizeof(uint32_t)
#define nildb_HEADER_SIZE ((SIZE_OFFSET * 3) + 4)
#define ATTR_SIZE	(1+SIZE_OFFSET)
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

int nildb_open( nildb *db, const char *path, int mode,
				uint32_t hash_table_size, uint32_t key_size, uint32_t value_size) {
	off_t pos;
	uint8_t buf[4];
	uint32_t tmp32;

#ifdef _WIN32
	db->f = (FILE *)0;
	fopen_s(&db->f,path,((mode == nildb_OPEN_MODE_RWREPLACE) ? "w+b" : (((mode == nildb_OPEN_MODE_RDWR)||(mode == nildb_OPEN_MODE_RWCREAT)) ? "r+b" : "rb")));
#else
	db->f = fopen(path,((mode == nildb_OPEN_MODE_RWREPLACE) ? "w+b" : (((mode == nildb_OPEN_MODE_RDWR)||(mode == nildb_OPEN_MODE_RWCREAT)) ? "r+b" : "rb")));
#endif
	if (!db->f) {
		if (mode == nildb_OPEN_MODE_RWCREAT) {
#ifdef _WIN32
			db->f = (FILE *)0;
			fopen_s(&db->f,path,"w+b");
#else
			db->f = fopen(path,"w+b");
#endif
		}
		if (!db->f)
			return nildb_ERROR_IO;
	}

	struct stat mystat;
	fstat(fileno(db->f),&mystat);
	pos=mystat.st_size;
	if (pos < nildb_HEADER_SIZE) {
		/* write header if not already present */
		if ((hash_table_size)&&(key_size)&&(value_size)) {
			int i;
			buf[0] = 'N'; buf[1] = 'D'; buf[2] = 'B'; buf[3] = nildb_VERSION;
			if (fwrite(buf,4,1,db->f) != 1) { fclose(db->f); return nildb_ERROR_IO; }
			tmp32 = htobe32(hash_table_size);
			if (fwrite(&tmp32,sizeof(uint32_t),1,db->f) != 1) { fclose(db->f); return nildb_ERROR_IO; }
			tmp32 = htobe32(key_size);
			if (fwrite(&tmp32,sizeof(uint32_t),1,db->f) != 1) { fclose(db->f); return nildb_ERROR_IO; }
			tmp32 = htobe32(value_size);
			if (fwrite(&tmp32,sizeof(uint32_t),1,db->f) != 1) { fclose(db->f); return nildb_ERROR_IO; }
			for (i=0;i<hash_table_size;i++){
				tmp32=htobe32(NULL_OFFSET);
				fwrite(&tmp32,sizeof(uint32_t),1,db->f);
			}
			fflush(db->f);
		} else {
			fclose(db->f);
			return nildb_ERROR_INVALID_PARAMETERS;
		}
	} else {
		if (fread(buf,4,1,db->f) != 1) { fclose(db->f); return nildb_ERROR_IO; }
		if ((buf[0] != 'N')||(buf[1] != 'D')||(buf[2] != 'B')||(buf[3] != nildb_VERSION)) {
			fclose(db->f);
			return nildb_ERROR_CORRUPT_DBFILE;
		}
		if (fread(&tmp32,sizeof(uint32_t),1,db->f) != 1) { fclose(db->f); return nildb_ERROR_IO; }
		if (!tmp32) {
			fclose(db->f);
			return nildb_ERROR_CORRUPT_DBFILE;
		}
		hash_table_size = be32toh(tmp32);
		printf("hash_table_size=%d\n",hash_table_size);

		if (fread(&tmp32,sizeof(uint32_t),1,db->f) != 1) { fclose(db->f); return nildb_ERROR_IO; }
		if (!tmp32) {
			fclose(db->f);
			return nildb_ERROR_CORRUPT_DBFILE;
		}
		key_size = be32toh(tmp32);

		if (fread(&tmp32,sizeof(uint32_t),1,db->f) != 1) { fclose(db->f); return nildb_ERROR_IO; }
		if (!tmp32) {
			fclose(db->f);
			return nildb_ERROR_CORRUPT_DBFILE;
		}
		value_size = be32toh(tmp32);
	}

	db->hash_table_size = hash_table_size;
	db->key_size = key_size;
	db->value_size = value_size;
	db->hash_table_size_bytes = sizeof(uint32_t) * hash_table_size;
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
	uint64_t offset;
	size_t n;
	uint32_t tmp32,offset32;

	/* read the first data location */
	offset = nildb_HEADER_SIZE + (uint64_t)hash_index*SIZE_OFFSET;
	if (fseeko(db->f,offset,SEEK_SET))
		return nildb_ERROR_IO;
	n = fread(&tmp32,1,sizeof(uint32_t),db->f);
	offset32=be32toh(tmp32);
	if (offset32==NULL_OFFSET){
		printf("No entry found\n");
		return -1;
	}

	klen = db->key_size;

	fseeko(db->f,nildb_HEADER_SIZE + db->hash_table_size_bytes, SEEK_SET);
	while(1){
		if (offset32 != 0 && fseeko(db->f,offset32,SEEK_CUR)){
			perror("fseeko get");
			return nildb_ERROR_IO;
		}

		n = fread(buf,1,ATTR_SIZE+klen,db->f);
		if (n<(ATTR_SIZE+klen)){
			perror("fread get");
			return -1; // not found
		}

		memcpy(&tmp32,buf+1,sizeof(uint32_t));
		offset32=be32toh(tmp32);

		if ((buf[0]&0x01)==0x01 && memcmp(key,buf+ATTR_SIZE,klen)==0){
			if (fread(vbuf,db->value_size,1,db->f) == 1)
				return 0; /* success */
			else{
				perror("fread 2 get");
				return -1;
			}
		}else{
			if (offset32==0){
				//printf("get: reached end\n");
				return -1;
			}
			offset32 -=(ATTR_SIZE+klen);
		}
	}
}

int delete_entry(nildb *db){
	uint8_t buf[8];
	memset(buf,0x0,sizeof(buf));
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
	fseeko(db->f,0,SEEK_END);
	uint64_t end_offset = ftello(db->f);
	uint64_t start_offset = nildb_HEADER_SIZE + db->hash_table_size_bytes;
	write_entry(db,key,value,1,0);

	if (parent_offset < start_offset){
		uint64_t parent_next_offset_be=htobe32(end_offset-start_offset);
		fseeko(db->f,parent_offset,SEEK_SET);
		fwrite(&parent_next_offset_be,1,SIZE_OFFSET,db->f);
	}else{
		uint64_t parent_next_offset_be=htobe32(end_offset-parent_offset);
		fseeko(db->f,parent_offset+1,SEEK_SET);
		fwrite(&parent_next_offset_be,1,SIZE_OFFSET,db->f);
	}
	fflush(db->f);
	return 0;
}

static int do_nildb_put(nildb *db,const void *key,const void *value, int delete) {
	uint8_t buf[4096];
	const uint8_t *kptr;
	unsigned long klen;
	uint32_t hash_index=(nildb_hash(key,db->key_size) % (uint64_t)db->hash_table_size);
	uint64_t offset;
	size_t n;
	uint32_t tmp32,offset32;


	/* read the first has index*/
	offset = nildb_HEADER_SIZE + hash_index*sizeof(uint32_t);
	//printf("start reading from hash table offset %lld\n",offset);
	if (fseeko(db->f,offset,SEEK_SET)){
		perror("put fseeko");
		return nildb_ERROR_IO;
	}
	uint64_t parent_offset=ftello(db->f);
	n = fread(&tmp32,1,sizeof(uint32_t),db->f);
	offset32=be32toh(tmp32);
	if (offset32==NULL_OFFSET){
		if (!delete){
			append_entry(db,key,value,parent_offset);
		}
		return 0;
	}

	kptr = (const uint8_t *)key;
	klen = db->key_size;

	fseeko(db->f,nildb_HEADER_SIZE + db->hash_table_size_bytes, SEEK_SET);
	uint64_t freeslot=0;
	while(1){
		uint32_t offset0;
		//printf("target offset=%u\n",offset32);
		if (fseeko(db->f,offset32,SEEK_CUR)){
			perror("put fseeko 32");
			return nildb_ERROR_IO;
		}
		parent_offset=ftello(db->f);

		n = (long)fread(buf,1,ATTR_SIZE+klen,db->f);
		if (n<(ATTR_SIZE+klen)){
			perror("put fread");
			return -1; // not found
		}

		memcpy(&tmp32,buf+1,sizeof(uint32_t));
		offset0=be32toh(tmp32);
		offset32 = offset0-(ATTR_SIZE+klen);

		if ((buf[0]&0x01)==0x01){
			//printf("found active entry\n");
			if (memcmp(kptr,buf+ATTR_SIZE,klen)==0){
				//update, leave offset alone
				//printf("same key, update it, delete=%d\n",delete);
				fseek(db->f,-1*(ATTR_SIZE+klen),SEEK_CUR);
				if (delete){
					delete_entry(db);
				}else{
					write_entry(db,key,value,0,0);
				}
				return 0;
			}else{
				//printf("not our key\n");
				if (offset0==0){ //last entry. update or append to the end of file, and update this index
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

#if 1
	printf("Opening new empty database test.db...\n");

	if (nildb_open(&db,"test.db",nildb_OPEN_MODE_RWREPLACE,1024,8,sizeof(v))) {
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
		//printf("reading %lld\n",i);
		memset(v,0,sizeof(v));
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

	printf("Getting %d 64-byte values...\n", NUMENTRY);

	for(i=1;i<=NUMENTRY;++i) {
		if ((q = nildb_get(&db,&i,v))) {
			printf("nildb_get (2) failed (%"PRIu64") (%d)\n",i,q);
			return 1;
		}
		for(j=0;j<8;++j) {
			if (v[j] != i) {
				printf("nildb_get (2) failed, bad data (%"PRIu64")\n",i);
				return 1;
			}
		}
	}
#endif

	printf("Closing and re-opening database in read-only mode...\n");

#if 0
	i=1;
	nildb_delete(&db,&i);
	if ((q = nildb_get(&db,&i,v))) {
		printf("nildb_get (2) failed (%"PRIu64") (%d)\n",i,q);
		return 1;
	}
	nildb_close(&db);
#endif


#if 1
	if (nildb_open(&db,"test.db",nildb_OPEN_MODE_RDONLY,1024,8,sizeof(v))) {
		printf("nildb_open failed\n");
		return 1;
	}

	printf("Getting %d 64-byte values...\n", NUMENTRY);
	for(i=1;i<=NUMENTRY;++i) {
		if ((q = nildb_get(&db,&i,v))) {
			printf("nildb_get (3) failed (%"PRIu64") (%d)\n",i,q);
			return 1;
		}
		for(j=0;j<8;++j) {
			if (v[j] != i) {
				printf("nildb_get (3) failed, bad data (%"PRIu64")\n",i);
				return 1;
			}
		}
	}
#endif

	nildb_close(&db);
	printf("All tests OK!\n");
	return 0;
}
#endif
