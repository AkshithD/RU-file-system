/*
 *  Copyright (C) 2024 CS416/CS518 Rutgers CS
 *	Tiny File System
 *	File:	rufs.c
 *
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <limits.h>

#include "block.h"
#include "rufs.h"

char diskfile_path[PATH_MAX];

// Declare your in-memory data structures here
struct superblock sb;
bitmap_t i_bitmap;
bitmap_t d_bitmap;

/* 
 * Get available inode number from bitmap
 */
int get_avail_ino() {
	// Step 1: Read inode bitmap from disk
	// Step 2: Traverse inode bitmap to find an available slot
	int next_ino = -1;
	for (int i = 0; i < MAX_INUM; i++) {
		if(get_bitmap(i_bitmap, i) == 0) {
			next_ino = i;
			break;
		}
	}

	// Step 3: Update inode bitmap and write to disk 
	if(next_ino != -1) {
		set_bitmap(i_bitmap, next_ino);
		bio_write(sb.i_bitmap_blk, i_bitmap);
		return next_ino;
	}
	return -1;
}

/* 
 * Get available data block number from bitmap
 */
int get_avail_blkno() {
	// Step 1: Read data block bitmap from disk
	// Step 2: Traverse data block bitmap to find an available slot
	int next_blkno = -1;
	for (int i = 0; i < MAX_DNUM; i++) {
		if(get_bitmap(d_bitmap, i) == 0) {
			next_blkno = i;
			break;
		}
	}

	// Step 3: Update data block bitmap and write to disk 
	if(next_blkno != -1) {
		set_bitmap(d_bitmap, next_blkno);
		bio_write(sb.d_bitmap_blk, d_bitmap);
		return next_blkno;
	}

	return -1;
}

/* 
 * inode operations
 */
int readi(uint16_t ino, struct inode *inode) {
	// Step 1: Get the inode's on-disk block number
	// # of inodes per block is ino / innodes per block
	int block_num = ino / (BLOCK_SIZE / sizeof(struct inode));
	// Step 2: Get offset of the inode in the inode on-disk block
	int offset = (ino % (BLOCK_SIZE / sizeof(struct inode))) * sizeof(struct inode);
	// Step 3: Read the block from disk and then copy into inode structure
	char buf[BLOCK_SIZE];
	bio_read(block_num, buf);
	memcpy(inode, buf + offset, sizeof(struct inode));
	return 0;
}

int writei(uint16_t ino, struct inode *inode) {
	// Step 1: Get the block number where this inode resides on disk
	int block_num = ino / (BLOCK_SIZE / sizeof(struct inode));
	// Step 2: Get the offset in the block where this inode resides on disk
	int offset = (ino % (BLOCK_SIZE / sizeof(struct inode))) * sizeof(struct inode);
	// Step 3: Write inode to disk 
	char buf[BLOCK_SIZE];
	bio_read(block_num, buf);
	time_t current_time;
	time(&current_time);
	inode->vstat.st_mtime = current_time;
	memcpy(buf + offset, inode, sizeof(struct inode));
	bio_write(block_num, buf);
	return 0;
}


/* 
 * directory operations
 */
int dir_find(uint16_t ino, const char *fname, size_t name_len, struct dirent *dirent) {
	// Step 1: Call readi() to get the inode using ino (inode number of current directory)
	struct inode dir_inode;
	readi(ino, &dir_inode);
	// Step 2: Get data block of current directory from inode
	char buf[BLOCK_SIZE];
	int num_of_blocks = dir_inode.size / BLOCK_SIZE; // number of blocks for this directory

	// Step 3: Read directory's data block and check each directory entry.
	//If the name matches, then copy directory entry to dirent structure
	for (int i = 0; i < num_of_blocks; i++) {
		bio_read(dir_inode.direct_ptr[i], buf);
		struct dirent *entry = (struct dirent *) buf;
		for (int j = 0; j < BLOCK_SIZE / sizeof(struct dirent); j++) {
			if (entry[j].valid == 1 && entry[j].len == name_len && strncmp(entry[j].name, fname, name_len) == 0) {
				memcpy(dirent, &entry[j], sizeof(struct dirent));
				return 0;
			}
		}
	}
	return -1;
}

int dir_add(struct inode dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {
	// Step 1: Read dir_inode's data block and check each directory entry of dir_inode
	char buf[BLOCK_SIZE];
	int num_of_blocks = dir_inode.size / BLOCK_SIZE; // number of blocks for this directory
	
	// Step 2: Check if fname (directory name) is already used in other entries
	for (int i = 0; i < num_of_blocks; i++) {
		bio_read(dir_inode.direct_ptr[i], buf);
		struct dirent *entry = (struct dirent *) buf;
		for (int j = 0; j < BLOCK_SIZE / sizeof(struct dirent); j++) {
			if (entry[j].valid == 1 && entry[j].len == name_len && strncmp(entry[j].name, fname, name_len) == 0) {
				return -1;
			}
		}
	}

	// Step 3: Add directory entry in dir_inode's data block and write to disk
	for (int i = 0; i < num_of_blocks; i++) {
		bio_read(dir_inode.direct_ptr[i], buf);
		struct dirent *entry = (struct dirent *) buf;
		for (int j = 0; j < BLOCK_SIZE / sizeof(struct dirent); j++) {
			if (entry[j].valid == 0) {
				entry[j].ino = f_ino;
				entry[j].valid = 1;
				entry[j].len = name_len;
				memcpy(entry[j].name, fname, name_len);
				bio_write(dir_inode.direct_ptr[i], buf);
				return 0;
			}
		}
	}
	// Allocate a new data block for this directory if it does not exist
	int new_block = get_avail_blkno();
	dir_inode.direct_ptr[num_of_blocks] = new_block;
	dir_inode.size += BLOCK_SIZE;
	// put the new entry in the new block
	struct dirent *entry = (struct dirent *) buf;
	entry[0].ino = f_ino;
	entry[0].valid = 1;
	entry[0].len = name_len;
	memcpy(entry[0].name, fname, name_len);
	// initialize the rest of the block
	for (int i = 1; i < BLOCK_SIZE / sizeof(struct dirent); i++) {
		entry[i].valid = 0;
		memcpy(buf + i * sizeof(struct dirent), &entry[i], sizeof(struct dirent));
	}
	// Update directory inode
	writei(dir_inode.ino, &dir_inode);
	// Write directory entry
	bio_write(dir_inode.direct_ptr[num_of_blocks], buf);
	return 0;
}

// Required for 518
int dir_remove(struct inode dir_inode, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and checks each directory entry of dir_inode
	
	// Step 2: Check if fname exist

	// Step 3: If exist, then remove it from dir_inode's data block and write to disk

	return 0;
}
/* 
 * namei operation
 */
int resolve_path(const char *path, uint16_t ino, struct inode *inode) {
    // Assume this function uses dir_find and readi to traverse the final resolved path
    char *token = strtok((char *)path, "/");
	struct dirent *entry = malloc(sizeof(struct dirent));
    while (token) {
        int res = dir_find(ino, token, strlen(token), entry);
        if (res == -1) {
            return -1; // Directory or file not found
        }
        ino = entry->ino; // Update inode number
        token = strtok(NULL, "/");
    }
	free(entry);
    return readi(ino, inode);
}
int get_node_by_path(const char *path, uint16_t ino, struct inode *inode) {
    if (path == NULL || inode == NULL) return -1; // Check for valid input

    char *path_copy = strdup(path); // Make a mutable copy of the path
    char *token = strtok(path_copy, "/");
    char *new_path = malloc(strlen(path) + 1); // Allocate space for the modified path
    new_path[0] = '\0'; // Start with an empty new path

    while (token) {
        if (strcmp(token, ".") == 0) {
            // Ignore "." which represents current directory
        } else if (strcmp(token, "..") == 0) {
            // Handle ".." by removing the last directory from new_path
            char *last_slash = strrchr(new_path, '/');
            if (last_slash) {
                *last_slash = '\0'; // Cut the path at the last slash
            } else {
                // If there's no slash, path goes to root or stays empty
                strcpy(new_path, "");
            }
        } else {
            // Regular token, append to the path
            if (strlen(new_path) > 0) {
                strcat(new_path, "/");
            }
            strcat(new_path, token);
        }
        token = strtok(NULL, "/");
    }

    // Now, new_path should contain the resolved path
    int result = resolve_path(new_path, ino, inode);
    free(new_path);
    free(path_copy);
    return result;
}




/* 
 * Make file system
 */
int rufs_mkfs() {

	// Call dev_init() to initialize (Create) Diskfile
	dev_init(diskfile_path);

	// write superblock information
	struct superblock sb;
	sb.magic_num = MAGIC_NUM;
	sb.max_inum = MAX_INUM;
	sb.max_dnum = MAX_DNUM;
	sb.i_bitmap_blk = 1;
	sb.d_bitmap_blk = 2;
	sb.i_start_blk = 3;
	sb.d_start_blk = 3 + (MAX_INUM * sizeof(struct inode) / BLOCK_SIZE);
	bio_write(0, &sb);

	// initialize inode bitmap
	bitmap_t i_bitmap = (bitmap_t) malloc(MAX_INUM / 8);
	memset(i_bitmap, 0, MAX_INUM / 8);

	// initialize data block bitmap
	bitmap_t d_bitmap = (bitmap_t) malloc(MAX_DNUM / 8);
	memset(d_bitmap, 0, MAX_DNUM / 8);

	// update bitmap information for root directory
	set_bitmap(i_bitmap, 0);
	set_bitmap(d_bitmap, 0);
	bio_write(sb.i_bitmap_blk, i_bitmap);
	bio_write(sb.d_bitmap_blk, d_bitmap);

	// update inode for root directory
	struct inode root_inode;
	root_inode.ino = 0;
	root_inode.valid = 1;
	root_inode.size = BLOCK_SIZE;
	root_inode.type = S_IFDIR;
	root_inode.direct_ptr[0] = sb.d_start_blk;
	//set up empty dirents (re check this. or lowk call the dir_add function)
	char buf[BLOCK_SIZE];
	struct dirent entry;
	for (int i = 0; i < BLOCK_SIZE / sizeof(struct dirent); i++) {
		entry.valid = 0;
		memcpy(buf + i * sizeof(struct dirent), &entry, sizeof(struct dirent));
	}
	bio_write(root_inode.direct_ptr[0], buf);
	writei(0, &root_inode);

	return 0;
}


/* 
 * FUSE file operations
 */
static void *rufs_init(struct fuse_conn_info *conn) {
	// Step 1a: If disk file is not found, call mkfs
	if (dev_open(diskfile_path) == -1) {
		rufs_mkfs();
	}
	// Step 1b: If disk file is found, just initialize in-memory data structures
	// and read superblock from disk
	char buf[BLOCK_SIZE];
	bio_read(0, buf);
	memcpy(&sb, buf, sizeof(struct superblock));
	i_bitmap = (bitmap_t) malloc(MAX_INUM / 8);
	d_bitmap = (bitmap_t) malloc(MAX_DNUM / 8);
	bio_read(sb.i_bitmap_blk, buf);
	memcpy(i_bitmap, buf, MAX_INUM / 8);
	bio_read(sb.d_bitmap_blk, buf);
	memcpy(d_bitmap, buf, MAX_DNUM / 8);

	return NULL;
}

static void rufs_destroy(void *userdata) {
	// Step 1: De-allocate in-memory data structures
	free(i_bitmap);
	free(d_bitmap);

	// Step 2: Close diskfile
	dev_close();
}

static int rufs_getattr(const char *path, struct stat *stbuf) {
	// check this function
	// Step 1: call get_node_by_path() to get inode from path
	printf("getattr %s\n", path);
	struct inode curr_inode;
	int res = get_node_by_path(path, 0, &curr_inode);
	if (res == -1) {
		return -1;
	}
	printf("got inode %d\n", curr_inode.ino);

	// Step 2: fill attribute of file into stbuf from inode
		stbuf->st_uid = getuid();
		stbuf->st_gid = getgid();
		stbuf->st_size = curr_inode.size;
		stbuf->st_mode = curr_inode.vstat.st_mode; //check
		stbuf->st_nlink  = 2; //check
		time(&stbuf->st_mtime);
	printf("filled stbuf\n");
	return 0;
}

static int rufs_opendir(const char *path, struct fuse_file_info *fi) {
	// Step 1: Call get_node_by_path() to get inode from path
	struct inode curr_inode;
	int res = get_node_by_path(path, 0, &curr_inode);
	if (res == -1) {
		return -1;
	}
	// Step 2: If not find, return -1
	if (curr_inode.type != S_IFDIR){
		return -1;
	}
	fi->fh = curr_inode.ino; //check
    return 0;
}

static int rufs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
	// Step 1: Call get_node_by_path() to get inode from path
    struct inode curr_inode;
    int res = get_node_by_path(path, 0, &curr_inode);
    if (res == -1) {
        return -1;
    }
	fi -> fh = curr_inode.ino; // check
	// Step 2: Read directory entries from its data blocks, and copy them to filler
    char buf[BLOCK_SIZE];
    off_t entry_offset = offset / sizeof(struct dirent);  // Calculate starting index based on offset
    for (int i = entry_offset; i < curr_inode.size / sizeof(struct dirent); i++) {
        bio_read(curr_inode.direct_ptr[i / (BLOCK_SIZE / sizeof(struct dirent))], buf);
        struct dirent *entry = (struct dirent *)buf;
        int entry_index = i % (BLOCK_SIZE / sizeof(struct dirent));

        if (entry[entry_index].valid == 1) {
            filler(buffer, entry[entry_index].name, NULL, 0);
        }
    }
	
    return 0;
}



static int rufs_mkdir(const char *path, mode_t mode) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name
    char *parent_dir = dirname((char*)path);
	char *target_dir = basename((char*)path);

	// Step 2: Call get_node_by_path() to get inode of parent directory
	struct inode parent_inode;
	int res = get_node_by_path(parent_dir, 0, &parent_inode);
	if (res == -1) {
		return -1;
	}
	// Step 3: Call get_avail_ino() to get an available inode number
	int next_ino = get_avail_ino();
	// Step 4: Call dir_add() to add directory entry of target directory to parent directory
	res = dir_add(parent_inode, next_ino, target_dir, strlen(target_dir));
	if (res == -1) {
		return -1;
	}
	// Step 5: Update inode for target directory
	struct inode target_inode;
	target_inode.ino = next_ino;
	target_inode.valid = 1;
	target_inode.size = BLOCK_SIZE;
	target_inode.type = S_IFDIR;
	target_inode.direct_ptr[0] = get_avail_blkno();
	target_inode.vstat.st_mode = mode;
	// Step 6: Call writei() to write inode to disk
	writei(next_ino, &target_inode);
	//how do i add mode
	return 0;
}

// Required for 518
static int rufs_rmdir(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of target directory

	// Step 3: Clear data block bitmap of target directory

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target directory in its parent directory

	return 0;
}

static int rufs_releasedir(const char *path, struct fuse_file_info *fi) {
// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name
	char *parent_dir = ((char*)path);
	char *target_file = ((char*)path);
	// Step 2: Call get_node_by_path() to get inode of parent directory
	struct inode parent_inode;
	int res = get_node_by_path(parent_dir, 0, &parent_inode);
	if (res == -1) {
		return -1;
	}
	// Step 3: Call get_avail_ino() to get an available inode number
	int next_ino = get_avail_ino();
	// Step 4: Call dir_add() to add directory entry of target file to parent directory
	res = dir_add(parent_inode, next_ino, target_file, strlen(target_file));
	if (res == -1) {
		return -1;
	}
	// Step 5: Update inode for target file
	struct inode target_inode;
	target_inode.ino = next_ino;
	target_inode.valid = 1;
	target_inode.size = 0;
	target_inode.type = S_IFREG;
	target_inode.vstat.st_mode = mode;
	//add link here
	// how do i do fh
	// Step 6: Call writei() to write inode to disk
	writei(next_ino, &target_inode);
	return 0;
}

static int rufs_open(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path
	struct inode curr_inode;
	int res = get_node_by_path(path, 0, &curr_inode);
	if (res == -1) {
		return -1;
	}
	// Step 2: If not find, return -1
	if (curr_inode.type != S_IFREG){
		return -1;
	}
	fi->fh = curr_inode.ino; //check

	return 0;
}

static int rufs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

	// Step 1: You could call get_node_by_path() to get inode from path
	struct inode curr_inode;
	int res = get_node_by_path(path, 0, &curr_inode);
	if (res == -1 || curr_inode.type != S_IFREG || size < 0 || offset < 0 || offset > curr_inode.size) {
		return -1;
	}
	// Step 2: Based on size and offset, read its data blocks from disk
	int start_block = offset / BLOCK_SIZE;
	int start_offset = offset % BLOCK_SIZE;
	int end_block;
	int end_offset;
	int bytes_read = 0;
	if (size + offset > curr_inode.size) {
		end_block = curr_inode.size / BLOCK_SIZE;
		end_offset = curr_inode.size % BLOCK_SIZE;
	} else {
		end_block = (size + offset) / BLOCK_SIZE;
		end_offset = (size + offset) % BLOCK_SIZE;
	}
	char buf[BLOCK_SIZE];
	for (int i = start_block; i <= end_block; i++) {
		bio_read(curr_inode.direct_ptr[i], buf);
		if (i == start_block && i == end_block) {
			memcpy(buffer, buf + start_offset, end_offset - start_offset);
			bytes_read += end_offset - start_offset;
		} else if (i == start_block) {
			memcpy(buffer, buf + start_offset, BLOCK_SIZE - start_offset);
			bytes_read += BLOCK_SIZE - start_offset;
		} else if (i == end_block) {
			memcpy(buffer, buf, end_offset);
			bytes_read += end_offset;
		} else {
			memcpy(buffer, buf, BLOCK_SIZE);
			bytes_read += BLOCK_SIZE;
		}
	}


	// Note: this function should return the amount of bytes you copied to buffer
	return bytes_read;
}

static int rufs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	// Step 1: You could call get_node_by_path() to get inode from path
	struct inode curr_inode;
	int res = get_node_by_path(path, 0, &curr_inode);
	if (res == -1 || curr_inode.type != S_IFREG || size < 0 || offset < 0 || offset > curr_inode.size) {
		return -1;
	}
	// Step 2: Based on size and offset, read its data blocks from disk
	int start_block = offset / BLOCK_SIZE;
	int start_offset = offset % BLOCK_SIZE;
	// from the start offset write the buffer and if the buffer is larger than the block size then write the rest of the buffer to the next block
	// if we runout of data blocks then allocate a new one and write the rest of the buffer to that block and so on
	if (size + offset > curr_inode.size) {
		int new_size = size - (curr_inode.size - offset);
		int old_size = curr_inode.size;
		curr_inode.size += new_size;
		int num_of_new_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
		int old_num_of_blocks = (old_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
		for (int i = old_num_of_blocks; i < old_num_of_blocks + num_of_new_blocks; i++) {
			curr_inode.direct_ptr[i] = get_avail_blkno();
			if (curr_inode.direct_ptr[i] == -1) {
				return -1;
			}
		}
	}

	// Step 3: Write the correct amount of data from offset to disk
	int end_block = (size + offset) / BLOCK_SIZE;
	int end_offset = (size + offset) % BLOCK_SIZE;
	char buf[BLOCK_SIZE];
	for (int i = start_block; i <= end_block; i++) {
		bio_read(curr_inode.direct_ptr[i], buf);
		if (i == start_block && i == end_block) {
			memcpy(buf + start_offset, buffer, end_offset - start_offset);
		} else if (i == start_block) {
			memcpy(buf + start_offset, buffer, BLOCK_SIZE - start_offset);
		} else if (i == end_block) {
			memcpy(buf, buffer, end_offset);
		} else {
			memcpy(buf, buffer, BLOCK_SIZE);
		}
		bio_write(curr_inode.direct_ptr[i], buf);
	}
	// Step 4: Update the inode info and write it to disk
	writei(curr_inode.ino, &curr_inode);
	return size;
}

// Required for 518

static int rufs_unlink(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of target file

	// Step 3: Clear data block bitmap of target file

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target file in its parent directory

	return 0;
}

static int rufs_truncate(const char *path, off_t size) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_release(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int rufs_flush(const char * path, struct fuse_file_info * fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_utimens(const char *path, const struct timespec tv[2]) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}


static struct fuse_operations rufs_ope = {
	.init		= rufs_init,
	.destroy	= rufs_destroy,

	.getattr	= rufs_getattr,
	.readdir	= rufs_readdir,
	.opendir	= rufs_opendir,
	.releasedir	= rufs_releasedir,
	.mkdir		= rufs_mkdir,
	.rmdir		= rufs_rmdir,

	.create		= rufs_create,
	.open		= rufs_open,
	.read 		= rufs_read,
	.write		= rufs_write,
	.unlink		= rufs_unlink,

	.truncate   = rufs_truncate,
	.flush      = rufs_flush,
	.utimens    = rufs_utimens,
	.release	= rufs_release
};


int main(int argc, char *argv[]) {
	int fuse_stat;

	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");

	fuse_stat = fuse_main(argc, argv, &rufs_ope, NULL);

	return fuse_stat;
}

