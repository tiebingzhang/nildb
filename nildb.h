/* nildb
 *
 * Tiebing Zhang <tiebing@meetcircle.com>,Adam Ierymenko <adam.ierymenko@zerotier.com>
 * NILDB is in the public domain and is distributed with NO WARRANTY.
 *
 */

#ifndef ___nildb_H
#define ___nildb_H

#include <stdio.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Version: 1
 *
 * This is the file format identifier, and changes any time the file
 * format changes. The code version will be this dot something, and can
 * be seen in tags in the git repository.
 */
#define nildb_VERSION 1

	//Key,data,next_offset,active/not
/**
 * nildb database state
 *
 * These fields can be read by a user, e.g. to look up key_size and
 * value_size, but should never be changed.
 */
typedef struct {
	unsigned long hash_table_size;
	unsigned long key_size;
	unsigned long value_size;
	unsigned long hash_table_size_bytes;
	FILE *f;
} nildb;

/**
 * I/O error or file not found
 */
#define nildb_ERROR_IO -2

/**
 * Out of memory
 */
#define nildb_ERROR_MALLOC -3

/**
 * Invalid paramters (e.g. missing _size paramters on init to create database)
 */
#define nildb_ERROR_INVALID_PARAMETERS -4

/**
 * Database file appears corrupt
 */
#define nildb_ERROR_CORRUPT_DBFILE -5

/**
 * Open mode: read only
 */
#define nildb_OPEN_MODE_RDONLY 1

/**
 * Open mode: read/write
 */
#define nildb_OPEN_MODE_RDWR 2

/**
 * Open mode: read/write, create if doesn't exist
 */
#define nildb_OPEN_MODE_RWCREAT 3

/**
 * Open mode: truncate database, open for reading and writing
 */
#define nildb_OPEN_MODE_RWREPLACE 4

/**
 * Open database
 *
 * The three _size parameters must be specified if the database could
 * be created or re-created. Otherwise an error will occur. If the
 * database already exists, these parameters are ignored and are read
 * from the database. You can check the struture afterwords to see what
 * they were.
 *
 * @param db Database struct
 * @param path Path to file
 * @param mode One of the nildb_OPEN_MODE constants
 * @param hash_table_size Size of hash table in 64-bit entries (must be >0)
 * @param key_size Size of keys in bytes
 * @param value_size Size of values in bytes
 * @return 0 on success, nonzero on error
 */
extern int nildb_open(
	nildb *db,
	const char *path,
	int mode,
	uint32_t hash_table_size,
	uint32_t key_size,
	uint32_t value_size);

/**
 * Close database
 *
 * @param db Database struct
 */
extern void nildb_close(nildb *db);

/**
 * Get an entry
 *
 * @param db Database struct
 * @param key Key (key_size bytes)
 * @param vbuf Value buffer (value_size bytes capacity)
 * @return -1 on I/O error, 0 on success, 1 on not found
 */
extern int nildb_get(nildb *db,const void *key,void *vbuf);

/**
 * Put an entry (overwriting it if it already exists)
 *
 * In the already-exists case the size of the database file does not
 * change.
 *
 * @param db Database struct
 * @param key Key (key_size bytes)
 * @param value Value (value_size bytes)
 * @return -1 on I/O error, 0 on success
 */
extern int nildb_put(nildb *db,const void *key,const void *value);

#ifdef __cplusplus
}
#endif

#endif

