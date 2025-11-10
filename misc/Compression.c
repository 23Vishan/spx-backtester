#define _CRT_SECURE_NO_WARNINGS

#include <stdio.h>
#include <stdlib.h>

#include <dirent.h>
#include <zlib.h>
#include <string.h>
#include <time.h>
#include <windows.h>
#include <uthash.h>

#define CHUNK 8192
#define PATH_MAX 256
#define BUFFER_SIZE 1024

#define GREEN "\x1b[32m"
#define YELLOW "\x1b[33m"
#define CYAN "\x1b[36m"
#define RESET "\x1b[0m"

struct Data {
	int time;
	float mid;
};

struct MappedFile {
	HANDLE hMap;
	LPVOID lpBasePtr;
	LONGLONG size;
	struct Data* ptr;
};

struct PriceHash {
	float midprice;     // value
	char filename[256]; // key
	UT_hash_handle hh;
};

void add_midprice(struct PriceHash** midprices, const char* filename, float midprice) {
	// check if it's already in hash table
	struct PriceHash* tmp;
	HASH_FIND_STR(*midprices, filename, tmp);

	// if not, add it
	if (tmp == NULL) {
		tmp = (struct PriceHash*)malloc(sizeof(struct PriceHash));
		strcpy(tmp->filename, filename);
		HASH_ADD_STR(*midprices, filename, tmp);
		tmp->midprice = midprice;
	}
}

float find_midprice(struct PriceHash** midprices, const char* filename) {
	struct PriceHash* tmp;

	// return midprice of filename
	HASH_FIND_STR(*midprices, filename, tmp);
	if (tmp) {
		return tmp->midprice;
	} else {
		return -1;
	}
}

void delete_price_hash(struct PriceHash** midprices) {
	struct PriceHash* current_entry, * tmp;

	// delete all entries
	HASH_ITER(hh, *midprices, current_entry, tmp) {
		HASH_DEL(*midprices, current_entry);
		free(current_entry);
	}
}

// Skip special file entries
int is_special_entry(const char* name) {
	return name[0] == '.';
}

// Split row into tokens
void tokenize(char* buffer, char* tokens[14]) {
	/*
	* Parameters:
	* - buffer: Row of data
	* - tokens: Array to hold tokens
	*/

	int i = 0;
	char* token = strtok(buffer, ",");

	// for each column
	while (i < 14) {
		if (token != NULL) {
			// column has data
			tokens[i] = token;
			token = strtok(NULL, ",");
		}
		else {
			// column does not have data
			tokens[i] = "";
		}

		i++;
	}
}

// Add data to array of data structures
struct Data* add_data(struct Data* data, char* tokens[], int* count, FILE* file) {
	/*
	* Parameters:
	* - data: Array of data structures
	* - tokens: Array of tokens
	* - count: Number of data structures
	* - file: File to get data from
	* 
	* Returns:
	* - Updated array of data structures
	*/

	int time = (tokens[1] && tokens[1][0] != '\0') ? atoi(tokens[1]) : 0;
	float ask, bid;

	// adjustments
	if (*count > 0) {
		int previous_time = data[*count - 1].time;

		// if time is same as previous time, overwrite previous data
		if (previous_time == time) {
			//assign time and ask
			if (strcmp(tokens[7], "A") == 0) {
				ask = (tokens[8] && tokens[8][0] != '\0') ? atof(tokens[8]) : 0.0f;

				// get bid
				char buffer[BUFFER_SIZE];
				if (fgets(buffer, sizeof(buffer), file) != NULL) {
					char* tokens[14];
					tokenize(buffer, tokens);

					// if next row is bid
					if (strcmp(tokens[7], "B") == 0) {
						bid = (tokens[8] && tokens[8][0] != '\0') ? atof(tokens[8]) : 0.0f;
					}
					else {
						return data;
					}
				}
				else {
					return data;
				}

				// calculate mid price
				data[*count - 1].mid = (ask + bid) / 2.0f;
				return data;
			}
		}

		//// skip times that are less than 0.25 seconds apart
		//if (time - previous_time < 250) {
		//	return data;
		//}

		// skip times that are less than 0.5 seconds apart
		if (time - previous_time < 500) {
			return data;
		}

		// skip times that are less than 1 seconds apart
		//if (time - previous_time < 1000) {
		//	return data;
		//}
	}

	// only include data after 9:30 am
	if (time > 93000000 && (strcmp(tokens[7], "A") == 0)) {
		ask = (tokens[8] && tokens[8][0] != '\0') ? atof(tokens[8]) : 0.0f;

		// get bid
		char buffer[BUFFER_SIZE];
		if (fgets(buffer, sizeof(buffer), file) != NULL) {
			char* tokens[14];
			tokenize(buffer, tokens);

			// if next row is bid
			if (strcmp(tokens[7], "B") == 0) {
				bid = (tokens[8] && tokens[8][0] != '\0') ? atof(tokens[8]) : 0.0f;
			}
			else {
				return data;
			}
		}
		else {
			return data;
		}

		// increase capacity
		struct Data* temp = realloc(data, (*count + 1) * sizeof(struct Data));
		if (temp != NULL) {
			data = temp;
			data[*count].time = time;

			// calculate mid price
			data[*count].mid = (ask + bid) / 2.0f;
			(*count)++;

			// if mid price equals previous mid price, do not include
			if (*count > 1 && data[*count - 1].mid == data[*count - 2].mid) {
				(*count)--;
			}
		}
		else {
			printf("error: could not reallocate memory\n");
			free(temp);
			return NULL;
		}
	}

	return data;
}

struct Data* change_resolution_sync(struct Data* old_data, int* count) {
	// 0.5 second resolution
	int cur_tick = 93000000;
	int resolution = 500;

	// stores synchronized data
	int new_size = 10;
	struct Data* new_data = malloc(new_size * sizeof(struct Data));
	if (new_data == NULL || old_data == NULL) {
		return NULL;
	}

	// initalize
	int new_count = 0;
	int old_index = 0, new_index = 0;
	int cur_time = old_data[old_index].time;
	int cur_price = old_data[old_index].mid;

	while (cur_time < 160000000 && old_index < *count) {
		// first tick
		if (old_index == 0) {
			// add data
			new_data[new_index].time = cur_tick;
			new_data[new_index].mid = cur_price;

			// update
			new_count++;
			new_index++;
			old_index++;
			cur_tick += resolution;
			if (old_index < *count) {
				cur_time = old_data[old_index].time;
				cur_price = old_data[old_index].mid;
			}
			continue;
		}

		if (cur_time <= cur_tick) {
			// overwrite
			while (cur_time <= cur_tick && old_index < *count) {
				// add data
				new_data[new_index].time = cur_tick;
				new_data[new_index].mid = cur_price;

				// update
				old_index++;
				if (old_index < *count) {
					cur_time = old_data[old_index].time;
					cur_price = old_data[old_index].mid;
				}
			}

			// update
			new_count++;
			new_index++;
			if (new_index >= new_size) {
				new_size *= 2;
				struct Data* temp = realloc(new_data, new_size * sizeof(struct Data));
				if (temp == NULL) {
					free(new_data);
					return NULL;
				}
				new_data = temp;
			}
			if (cur_tick % 10000000 >= 5999500) {
				int hour = (cur_tick / 10000000) + 1;
				cur_tick = hour * 10000000;
			}
			else {
				cur_tick += resolution;
			}
		}

		if (cur_time > cur_tick) {
			// add data
			new_data[new_index].time = cur_tick;
			new_data[new_index].mid = cur_price;

			// update
			new_count++;
			new_index++;
			if (new_index >= new_size) {
				new_size *= 2;
				struct Data* temp = realloc(new_data, new_size * sizeof(struct Data));
				if (temp == NULL) {
					free(new_data);
					return NULL;
				}
				new_data = temp;
			}
			if (cur_tick % 10000000 >= 5999500) {
				int hour = (cur_tick / 10000000) + 1;
				cur_tick = hour * 10000000;
			}
			else {
				cur_tick += resolution;
			}
		}
	}

	struct Data* temp = realloc(new_data, new_count * sizeof(struct Data));
	if (temp == NULL) {
		free(new_data);
		return NULL;
	}
	new_data = temp;

	printf("Count, %d\n", new_count);
	*count = new_count;
	return new_data;
}

// Compresses csv file into binary file
void process_file(char file_name[]) {
	// open source file
	FILE* file;
	errno_t err = fopen_s(&file, file_name, "r");

	// check for errors
	if (err != 0) {
		printf("error: could not open %s\n", file_name);
		return;
	}

	char buffer[BUFFER_SIZE];
	struct Data* data = NULL;
	int count = 0;

	// skip header
	if (fgets(buffer, sizeof(buffer), file) != NULL) {
		// for each row of data
		while (fgets(buffer, sizeof(buffer), file)) {
			// split data into tokens
			char* tokens[14];
			tokenize(buffer, tokens);

			// add data
			data = add_data(data, tokens, &count, file);
		}
	}

	fclose(file);

	// output file path
	char updated_name[PATH_MAX] = "./processed-data/";
	int len = strlen(file_name);

	// create updated file name
	if (len >= 12) {
		char* ptr = file_name + len - 12;
		char temp[9];
		strncpy(temp, ptr, 8);
		temp[8] = '\0';
		strcat(updated_name, temp);
	}
	strcat(updated_name, "/");
	if (len >= 18) {
		char* ptr = file_name + len - 18;
		char temp[6];
		strncpy(temp, ptr, 5);
		temp[5] = '\0';
		strcat(updated_name, temp);
	}

	// create compressed file
	gzFile file2 = gzopen(updated_name, "wb");
	if (file2 == NULL) {
		printf("error: could not open %s\n", updated_name);
		return;
	}

	// compression parameters
	gzsetparams(file2, 9, Z_DEFAULT_STRATEGY);

	// TESTING
	//data = change_resolution_sync(data, &count);

	// write data structs to output file in chunks
	int chunk_size = 100;
	for (int i = 0; i < count; i += chunk_size) {
		int size = (i + chunk_size <= count) ? chunk_size : count - i;
		gzwrite(file2, &data[i], size * sizeof(data[i]));
	}

	// write all data structures at once
	//gzwrite(file2, data, count * sizeof(struct Data));

	gzclose(file2);
	free(data);
}

// Thread function
DWORD WINAPI ThreadProcessFile(void* data) {
	process_file((char*)data);
	remove((char*)data);
	return 0;
}

// Decompresses gz file into csv file
static void decompress_file_gz(const char* src_path, const char* dst_path) {
	/*
	* Parameters:
	* - src_path: Path to source file
	* - dst_path: Path to destination file
	*/

	// open source
	gzFile file = gzopen(src_path, "rb");
	if (file == NULL) {
		printf("Error opening file %s\n", src_path);
		return;
	}

	// open destination
	FILE* dest = fopen(dst_path, "w");
	if (dest == NULL) {
		printf("Error creating file %s\n", dst_path);
		gzclose(file);
		return;
	}

	// initialize buffer
	unsigned char* buffer = (unsigned char*)malloc(CHUNK * sizeof(unsigned char));
	if (buffer == NULL) {
		printf("Error allocating memory for buffer\n");
		gzclose(file);
		fclose(dest);
		return;
	}

	// decompress file
	int uncompressedLength;
	while ((uncompressedLength = gzread(file, buffer, CHUNK)) > 0) {
		fwrite(buffer, 1, uncompressedLength, dest);
	}

	free(buffer);
	fclose(dest);
	gzclose(file);
}

// Process all files in directory
void process_directory() {
	// open main directory
	struct dirent* main_entry;
	char main_dir_path[PATH_MAX] = "./raw-data";
	DIR* main_dir = opendir(main_dir_path);

	if (main_dir)
	{
		// iterate over each folder in main directory
		while ((main_entry = readdir(main_dir)) != NULL)
		{
			// skip special entries
			if (is_special_entry(main_entry->d_name)) continue;
			printf("\n%sCurrent Folder: %s%s\n", YELLOW, RESET, main_entry->d_name);

			// create path to sub directory
			char sub_dir_path[PATH_MAX];
			snprintf(sub_dir_path, sizeof(sub_dir_path), "%s/%s", main_dir_path, main_entry->d_name);

			// open sub directory
			struct dirent* sub_entry;
			DIR* sub_dir = opendir(sub_dir_path);

			if (sub_dir)
			{
				// create name of output directory
				char output_dir[PATH_MAX] = "./processed-data/";
				int len = strlen(main_entry->d_name);

				// extract date from folder name, 20230412 from SPXW.20230412
				if (len >= 8) {
					char* ptr = main_entry->d_name + len - 8;
					char temp[9];
					strncpy(temp, ptr, 8);
					temp[8] = '\0';
					strcat(output_dir, temp);
				}

				// create directory to store decompressed files
				_mkdir(output_dir);

				// iterate over each file in sub directory
				while ((sub_entry = readdir(sub_dir)) != NULL)
				{
					// skip special entries
					if (is_special_entry(sub_entry->d_name)) continue;
					printf("%sProcessing: %s%s\n", GREEN, RESET, sub_entry->d_name);

					// remove .gz from file name
					char csv_name[PATH_MAX];
					strcpy(csv_name, sub_entry->d_name);
					if (strlen(csv_name) > 3) {
						csv_name[strlen(csv_name) - 3] = '\0';
					}

					// path to source file
					char src_path[PATH_MAX];
					snprintf(src_path, sizeof(src_path), "%s/%s", sub_dir_path, sub_entry->d_name);

					// path to destination file
					char dst_path[PATH_MAX];
					snprintf(dst_path, sizeof(dst_path), "./processed-data/%s", csv_name);

					// decompress to csv file
					decompress_file_gz(src_path, dst_path);

					HANDLE thread = CreateThread(NULL, 0, ThreadProcessFile, dst_path, 0, NULL);
					if (thread) {
						WaitForSingleObject(thread, INFINITE);
						CloseHandle(thread);
					}
				}
			}
			// close sub directory
			closedir(sub_dir);
		}
		// close main directory
		closedir(main_dir);
	}

	return;
}

// Accesses data from a compressed binary file using gzread
void read_normal(const char* date, const char* filename) {
	char source_filename[256];
	snprintf(source_filename, sizeof(source_filename), "./processed-data/%s/%s", date, filename);

	gzFile file = gzopen(source_filename, "rb");
	if (file == NULL) {
		printf("error: could not open %s\n", filename);
		return;
	}

	struct Data data;
	int count;

	while ((count = gzread(file, &data, sizeof(data))) > 0) {
		printf("Time: %d, Mid: %.1f\n", data.time, data.mid);
	}
	gzclose(file);
}

struct MappedFile* open_mapped_file(const char* date, const char* filename) {
	// initialize struct
	struct MappedFile* mf = malloc(sizeof(struct MappedFile));
	if (mf == NULL) {
		fprintf(stderr, "stderr: failed to allocate memory\n");
		return NULL;
	}

	// create source file name
	char source_filename[PATH_MAX];
	snprintf(source_filename, sizeof(source_filename), "./processed-data/%s/%s", date, filename);

	// open source file
	gzFile infile = gzopen(source_filename, "rb");
	if (!infile) {
		fprintf(stderr, "stderr: gzopen for %s failed\n", source_filename);
		free(mf);
		return NULL;
	}

	// initialize buffer
	char* buffer = malloc(CHUNK);
	if (buffer == NULL) {
		fprintf(stderr, "stderr: failed to allocate memory\n");
		gzclose(infile);
		free(mf);
		return NULL;
	}

	// read source file into buffer
	int num_read = 0, total_read = 0;
	while ((num_read = gzread(infile, buffer + total_read, CHUNK)) > 0) {
		// increase buffer size
		total_read += num_read;
		char* tmp = realloc(buffer, total_read + CHUNK);
		if (tmp == NULL) {
			free(buffer);
			fprintf(stderr, "stderr: failed to allocate memory\n");
			gzclose(infile);
			free(mf);
			return NULL;
		}
		buffer = tmp;
	}
	gzclose(infile);

	// create a memory-mapped file of size buffer
	mf->hMap = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, total_read, NULL);
	if (mf->hMap == NULL) {
		fprintf(stderr, "stderr: CreateFileMapping failed with error %d\n", GetLastError());
		free(buffer);
		free(mf);
		return NULL;
	}

	// map the memory mapped file into the process's address space
	mf->lpBasePtr = MapViewOfFile(mf->hMap, FILE_MAP_ALL_ACCESS, 0, 0, total_read);
	if (mf->lpBasePtr == NULL) {
		fprintf(stderr, "stderr: MapViewOfFile failed with error %d\n", GetLastError());
		CloseHandle(mf->hMap);
		free(buffer);
		free(mf);
		return NULL;
	}

	// copy buffer to the memory-mapped file
	CopyMemory(mf->lpBasePtr, buffer, total_read);

	// cast pointer to a data struct pointer
	mf->ptr = (struct Data*)mf->lpBasePtr;
	mf->size = total_read / sizeof(struct Data);
	free(buffer);

	return mf;
}

// Uses memory-mapped files to access data from a compressed binary file
void read_mmap(const char* date, const char* filename, const char* option, const char* value) {
	/* 
	* Parameters:
	* - date: Date of file
	* - filename: Name of file
	* - option: Option selected
	* - value: Parameter for option if required
	* 
	* Options:
	* - A: print all data
	* - B: print data for a specific record (value: (int) index of record)
	* - C: print data for a specific time (value: (int) timestamp)
	* - D: print number of data entries
	* 
	* Returns:
	* - Data for selected option
	*/

	// open file
	struct MappedFile* mf = open_mapped_file(date, filename);
	if (mf == NULL) {
		return NULL;
	}

	// get file attributes
	int i = mf->size;
	struct Data* ptr = mf->ptr;

	// perform operation based on the selected option
	if (strcmp(option, "A") == 0) {
		// iterate over each data structure and print price and time
		while (i-- > 0) {
			printf("Time: %d, Mid: %.3f\n", ptr->time, ptr->mid);
			ptr++;
		}
	}
	else if (strcmp(option, "B") == 0) {
		int record_num = atoi(value);

		// access a specific record
		if (record_num < i && record_num >= 0) {
			ptr = mf->ptr + record_num;
			printf("Time: %d, Mid: %.3f\n", ptr->time, ptr->mid);
		}
	}
	else if (strcmp(option, "C") == 0) {
		int time = atoi(value);
		struct Data* closest = NULL;

		// find closest data point to time
		while (i-- > 0) {
			if (ptr->time <= time) {
				closest = ptr;
			}
			ptr++;
		}

		if (closest != NULL) {
			printf("%d %.3f\n", closest->time, closest->mid);
		}
	}
	else if (strcmp(option, "D") == 0) {
		printf("Number of data entries: %i\n", i);
	}

	// unmap file
	UnmapViewOfFile(mf->lpBasePtr);
	CloseHandle(mf->hMap);
	free(mf);
}

// calculates at each time stamp
void mmap_stoploss(const char* date, const char* lower_file_name, const char* upper_file_name, int entry_time, float entry_credit, float stop_multiplier) {
	struct MappedFile* mf1 = open_mapped_file(date, lower_file_name);
	struct MappedFile* mf2 = open_mapped_file(date, upper_file_name);

	float starting_position = entry_credit * stop_multiplier;

	// are strikes call or put?
	int is_call = 0;
	if (lower_file_name[0] == 'C') {
		is_call = 1;
	}

	int index1 = 0, index2 = 0;
	float lastPrice1 = 0.0f, lastPrice2 = 0.0f;
	int lastTime1 = 0, lastTime2 = 0;

	// initalize with first data structure
	if (mf1->size > 0) {
		lastPrice1 = mf1->ptr[0].mid;
		lastTime1 = mf1->ptr[0].time;
		index1 = 1;
	}

	// initalzie with first data structure
	if (mf2->size > 0) {
		lastPrice2 = mf2->ptr[0].mid;
		lastTime2 = mf2->ptr[0].time;
		index2 = 1;
	}

	// while there are still data structures to read
	while (index1 < mf1->size || index2 < mf2->size) {

		// get next time if possible
		int time1 = index1 < mf1->size ? mf1->ptr[index1].time : INT_MAX;
		int time2 = index2 < mf2->size ? mf2->ptr[index2].time : INT_MAX;

		// update file1 parameters to next time, keep file2 parameters the same
		if (time1 <= time2) {
			lastPrice1 = mf1->ptr[index1].mid;
			lastTime1 = time1;
			index1++;
		}

		// update file2 parameters to next time, keep file1 parameters the same
		if (time2 <= time1) {
			lastPrice2 = mf2->ptr[index2].mid;
			lastTime2 = time2;
			index2++;
		}

		// ------------------------------------------
		// STOP LOSS ORDER LOGIC BELOW
		// ------------------------------------------

		// start after entry time
		if (lastTime1 > entry_time && lastTime2 > entry_time) {
			// calculate current position
			float current_position;
			if (is_call == TRUE) {
				// call, lowest - upper
				current_position = (lastPrice1 - lastPrice2);
			} else {
				// put, upper - lowest
				current_position = (lastPrice2 - lastPrice1);
			}

			//printf("lastTime1:  %d, lastPrice1: %.3f, lastTime2: %d, lastPrice2: %.3f, currentPos: %.3f\n", lastTime1, lastPrice1, lastTime2, lastPrice2, current_position);

			// check if stop loss has been hit
			if (current_position > starting_position) {
				// get lowest time
				int current_time = lastTime1 < lastTime2 ? lastTime1 : lastTime2;

				// print output
				printf("%d %.3f\n", current_time, current_position);
				return;
			}
		}
	}

	// free resources
	UnmapViewOfFile(mf1->lpBasePtr);
	CloseHandle(mf1->hMap);
	free(mf1);

	UnmapViewOfFile(mf2->lpBasePtr);
	CloseHandle(mf2->hMap);
	free(mf2);
}

// calculates at each time stamp
void mmap_stop_limit_order(const char* date, const char* lower_file_name, const char* upper_file_name, int entry_time, float stop_price, float limit_price) {
	struct MappedFile* mf1 = open_mapped_file(date, lower_file_name);
	struct MappedFile* mf2 = open_mapped_file(date, upper_file_name);

	int stop_limit_reached = 0;

	// are strikes call or put?
	int is_call = 0;
	if (lower_file_name[0] == 'C') {
		is_call = 1;
	}

	int index1 = 0, index2 = 0;
	float lastPrice1 = 0.0f, lastPrice2 = 0.0f;
	int lastTime1 = 0, lastTime2 = 0;

	// initalize with first data structure
	if (mf1->size > 0) {
		lastPrice1 = mf1->ptr[0].mid;
		lastTime1 = mf1->ptr[0].time;
		index1 = 1;
	}

	// initalzie with first data structure
	if (mf2->size > 0) {
		lastPrice2 = mf2->ptr[0].mid;
		lastTime2 = mf2->ptr[0].time;
		index2 = 1;
	}

	// while there are still data structures to read
	while (index1 < mf1->size || index2 < mf2->size) {

		// get next time if possible
		int time1 = index1 < mf1->size ? mf1->ptr[index1].time : INT_MAX;
		int time2 = index2 < mf2->size ? mf2->ptr[index2].time : INT_MAX;

		// update file1 parameters to next time, keep file2 parameters the same
		if (time1 <= time2) {
			lastPrice1 = mf1->ptr[index1].mid;
			lastTime1 = time1;
			index1++;
		}

		// update file2 parameters to next time, keep file1 parameters the same
		if (time2 <= time1) {
			lastPrice2 = mf2->ptr[index2].mid;
			lastTime2 = time2;
			index2++;
		}

		// ------------------------------------------
		// STOP LIMIT ORDER LOGIC BELOW
		// ------------------------------------------

		// start after entry time
		if (lastTime1 > entry_time && lastTime2 > entry_time) {
			// calculate current position
			float current_position;
			if (is_call == TRUE) {
				// call, lowest - upper
				current_position = (lastPrice1 - lastPrice2);
			}
			else {
				// put, upper - lowest
				current_position = (lastPrice2 - lastPrice1);
			}

			// check if stop loss has been hit
			if (current_position > stop_price) {
				stop_limit_reached = 1;
			}

			if (stop_limit_reached) {
				if (current_position < stop_price) {
					if (current_position > limit_price) {
						// get lowest time
						int current_time = lastTime1 < lastTime2 ? lastTime1 : lastTime2;

						// print output
						printf("%d %.3f\n", current_time, current_position);
						return;
					}
				}
			}
		}
	}

	// free resources
	UnmapViewOfFile(mf1->lpBasePtr);
	CloseHandle(mf1->hMap);
	free(mf1);

	UnmapViewOfFile(mf2->lpBasePtr);
	CloseHandle(mf2->hMap);
	free(mf2);
}

// calculates every skip_time
void mmap_stoploss_v2(const char* date, const char* lower_file_name, const char* upper_file_name, int entry_time, float entry_credit, float stop_multiplier, int skip_time) {
	struct MappedFile* mf1 = open_mapped_file(date, lower_file_name);
	struct MappedFile* mf2 = open_mapped_file(date, upper_file_name);

	float starting_position = entry_credit * stop_multiplier;

	// are strikes call or put?
	int is_call = 0;
	if (lower_file_name[0] == 'C') {
		is_call = 1;
	}

	int current_time = entry_time;
	int index1 = 0, index2 = 0;
	float lastPrice1 = 0.0f, lastPrice2 = 0.0f;
	int lastTime1 = 0, lastTime2 = 0;

	// initalize with first data structure
	if (mf1->size > 0) {
		lastPrice1 = mf1->ptr[0].mid;
		lastTime1 = mf1->ptr[0].time;
		index1 = 1;
	}

	// initalzie with first data structure
	if (mf2->size > 0) {
		lastPrice2 = mf2->ptr[0].mid;
		lastTime2 = mf2->ptr[0].time;
		index2 = 1;
	}

	// skip to current time
	while (index1 < mf1->size) {
		lastTime1 = mf1->ptr[index1].time;

		// reached current time
		if (lastTime1 >= current_time) {
			lastPrice1 = mf1->ptr[index1].mid;
			break;
		}
		index1++;
	}

	// skip to current time
	while (index2 < mf2->size) {
		lastTime2 = mf2->ptr[index2].time;

		// reached current time
		if (lastTime2 >= current_time) {
			lastPrice2 = mf2->ptr[index2].mid;
			break;
		}
		index2++;
	}

	// repeat til end of day
	while (current_time < 155900000) {
		// get next time if possible
		while (mf1->ptr[index1].time <= current_time && index1 < mf1->size) {
			index1++;
		}
		while (mf2->ptr[index2].time <= current_time && index2 < mf2->size) {
			index2++;
		}

		// update parameters
		lastTime1 = mf1->ptr[index1].time;
		lastPrice1 = mf1->ptr[index1].mid;
		lastTime2 = mf2->ptr[index2].time;
		lastPrice2 = mf2->ptr[index2].mid;

		//printf("currentTime: %d, lastTime1: %d, lastTime2: %d\n", current_time, lastTime1, lastTime2);

		// start after entry time
		if (lastTime1 > entry_time && lastTime2 > entry_time) {
			// calculate current position
			float current_position;
			if (is_call == TRUE) {
				// call, lowest - upper
				current_position = (lastPrice1 - lastPrice2);
			}
			else {
				// put, upper - lowest
				current_position = (lastPrice2 - lastPrice1);
			}

			// check if stop loss has been hit
			if (current_position > starting_position) {
				// get lowest time
				int current_time = lastTime1 < lastTime2 ? lastTime1 : lastTime2;

				// print output
				printf("%d %.3f\n", current_time, current_position);
				return;
			}
		}
		current_time += skip_time;
	}

	// free resources
	UnmapViewOfFile(mf1->lpBasePtr);
	CloseHandle(mf1->hMap);
	free(mf1);

	UnmapViewOfFile(mf2->lpBasePtr);
	CloseHandle(mf2->hMap);
	free(mf2);
}

// calculates every skip_time
void mmap_stop_limit_order_v2(const char* date, const char* lower_file_name, const char* upper_file_name, int entry_time, float stop_price, float limit_price, int skip_time) {
	struct MappedFile* mf1 = open_mapped_file(date, lower_file_name);
	struct MappedFile* mf2 = open_mapped_file(date, upper_file_name);

	int stop_limit_reached = 0;

	// are strikes call or put?
	int is_call = 0;
	if (lower_file_name[0] == 'C') {
		is_call = 1;
	}

	int current_time = entry_time;
	int index1 = 0, index2 = 0;
	float lastPrice1 = 0.0f, lastPrice2 = 0.0f;
	int lastTime1 = 0, lastTime2 = 0;

	// initalize with first data structure
	if (mf1->size > 0) {
		lastPrice1 = mf1->ptr[0].mid;
		lastTime1 = mf1->ptr[0].time;
		index1 = 1;
	}

	// initalzie with first data structure
	if (mf2->size > 0) {
		lastPrice2 = mf2->ptr[0].mid;
		lastTime2 = mf2->ptr[0].time;
		index2 = 1;
	}

	// skip to current time
	while (index1 < mf1->size) {
		lastTime1 = mf1->ptr[index1].time;

		// reached current time
		if (lastTime1 >= current_time) {
			lastPrice1 = mf1->ptr[index1].mid;
			break;
		}
		index1++;
	}

	// skip to current time
	while (index2 < mf2->size) {
		lastTime2 = mf2->ptr[index2].time;

		// reached current time
		if (lastTime2 >= current_time) {
			lastPrice2 = mf2->ptr[index2].mid;
			break;
		}
		index2++;
	}

	// repeat til end of day
	while (current_time < 155900000) {
		// get next time if possible
		while (mf1->ptr[index1].time <= current_time && index1 < mf1->size) {
			index1++;
		}
		while (mf2->ptr[index2].time <= current_time && index2 < mf2->size) {
			index2++;
		}

		// update parameters
		lastTime1 = mf1->ptr[index1].time;
		lastPrice1 = mf1->ptr[index1].mid;
		lastTime2 = mf2->ptr[index2].time;
		lastPrice2 = mf2->ptr[index2].mid;

		// ------------------------------------------
		// STOP LIMIT ORDER LOGIC BELOW
		// ------------------------------------------

		// start after entry time
		if (lastTime1 > entry_time && lastTime2 > entry_time) {
			// calculate current position
			float current_position;
			if (is_call == TRUE) {
				// call, lowest - upper
				current_position = (lastPrice1 - lastPrice2);
			}
			else {
				// put, upper - lowest
				current_position = (lastPrice2 - lastPrice1);
			}

			// check if stop loss has been hit
			if (current_position > stop_price) {
				stop_limit_reached = 1;
			}

			if (stop_limit_reached) {
				if (current_position < stop_price) {
					if (current_position > limit_price) {
						// get lowest time
						int current_time = lastTime1 < lastTime2 ? lastTime1 : lastTime2;

						// print output
						printf("%d %.3f\n", current_time, current_position);
						return;
					}
				}
			}
		}
		current_time += skip_time;
	}

	// free resources
	UnmapViewOfFile(mf1->lpBasePtr);
	CloseHandle(mf1->hMap);
	free(mf1);

	UnmapViewOfFile(mf2->lpBasePtr);
	CloseHandle(mf2->hMap);
	free(mf2);
}

float get_mid_price(const char* date, const char* filename, int time) {
	struct MappedFile* mf = open_mapped_file(date, filename);
	if (mf == NULL) {
		return -1;
	}

	// get file attributes
	int i = mf->size;
	struct Data* ptr = mf->ptr;
	struct Data* closest = NULL;

	// find closest data point to time
	while (i-- > 0) {
		if (ptr->time <= time) {
			closest = ptr;
		}
		ptr++;
	}

	if (closest != NULL) {
		return closest->mid;
	}

	// free resources
	UnmapViewOfFile(mf->lpBasePtr);
	CloseHandle(mf->hMap);
	free(mf);
	return -1;
}

float get_mid_price_normal(const char* date, const char* filename, int time) {
	char source_filename[256];
	snprintf(source_filename, sizeof(source_filename), "./processed-data/%s/%s", date, filename);

	gzFile file = gzopen(source_filename, "rb");
	if (file == NULL) {
		//fprintf(stderr, "stderr: could not open %s\n", filename);
		return -1;
	}

	// file attributes
	struct Data data;
	struct Data closest;
	int found = 0;

	// find closest data point to time
	while (gzread(file, &data, sizeof(data)) > 0) {
		if (data.time > time) {
			break;
		}
		closest = data;
		found = 1;
	}
	gzclose(file);

	return found ? closest.mid : -1;
}

void find_call_strikes(const char* date, const char* entry_time, const char* spread_width, const char* entry_credit, const char* num_spreads, const char* lower_search, const char* upper_search) {
	int sw = atoi(spread_width);
	float ec = atof(entry_credit);
	int ns = atoi(num_spreads);
	int spreads = 0;
	int us = atoi(upper_search);
	int ls = atoi(lower_search);
	int et = atoi(entry_time);

	char short_file[256];
	char long_file[256];
	struct PriceHash* midprices = NULL;

	for (int short_strike = us; short_strike >= ls && spreads < ns; short_strike -= 5) {
		int long_strike = short_strike + sw;

		// get file names
		snprintf(short_file, sizeof(short_file), "C%d", short_strike);
		snprintf(long_file, sizeof(long_file), "C%d", long_strike);

		// get prices
		//float short_price = get_mid_price_normal(date, short_file, et);
		//float long_price = get_mid_price_normal(date, long_file, et);

		float short_price = find_midprice(&midprices, short_file);
		if (short_price == -1) {
			short_price = get_mid_price_normal(date, short_file, et);
			add_midprice(&midprices, short_file, short_price);
		}

		float long_price = find_midprice(&midprices, long_file);
		if (long_price == -1) {
			long_price = get_mid_price_normal(date, long_file, et);
			add_midprice(&midprices, long_file, long_price);
		}

		// calculate credit received
		if (short_price != -1 && long_price != -1) {
			float credit_received = short_price - long_price;

			// found spread
			if (credit_received >= ec) {
				printf("%d %d %.3f\n", short_strike, long_strike, credit_received);
				spreads++;
			}
		}
		else {
			//fprintf(stderr, "stderr: could not find prices for either %s or %s\n", short_file, long_file);
		}
	}

	delete_price_hash(&midprices);
}

void find_put_strikes(const char* date, const char* entry_time, const char* spread_width, const char* entry_credit, const char* num_spreads, const char* lower_search, const char* upper_search) {
	int sw = atoi(spread_width);
	float ec = atof(entry_credit);
	int ns = atoi(num_spreads);
	int spreads = 0;
	int us = atoi(upper_search);
	int ls = atoi(lower_search);
	int et = atoi(entry_time);

	char short_file[256];
	char long_file[256];
	struct PriceHash* midprices = NULL;

	for (int long_strike = ls; long_strike <= us && spreads < ns; long_strike += 5) {
		int short_strike = long_strike + sw;

		// get file names
		snprintf(short_file, sizeof(short_file), "P%d", short_strike);
		snprintf(long_file, sizeof(long_file), "P%d", long_strike);

		// get prices
		//float short_price = get_mid_price_normal(date, short_file, et);
		//float long_price = get_mid_price_normal(date, long_file, et);

		float short_price = find_midprice(&midprices, short_file);
		if (short_price == -1) {
			short_price = get_mid_price_normal(date, short_file, et);
			add_midprice(&midprices, short_file, short_price);
		}

		float long_price = find_midprice(&midprices, long_file);
		if (long_price == -1) {
			long_price = get_mid_price_normal(date, long_file, et);
			add_midprice(&midprices, long_file, long_price);
		}

		// calculate credit received
		if (short_price != -1 && long_price != -1) {
			float credit_received = short_price - long_price;

			// found spread
			if (credit_received >= ec) {
				printf("%d %d %.3f\n", long_strike, short_strike, credit_received);
				spreads++;
			}
		}
		else {
			//fprintf(stderr, "stderr: could not find prices for either %s or %s\n", short_file, long_file);
		}
	}

	delete_price_hash(&midprices);
}

int main(int argc, char* argv[]) {
	// to execute program via command lines
	if (argc == 5) {
		// get price for given time or details for a specific record
		read_mmap(argv[1], argv[2], argv[3], argv[4]);
		return;
	}
	if (argc == 9) {
		// find call or put spreads
		if (strcmp(argv[8], "both") == 0) {
			find_call_strikes(argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argv[7]);
			printf("break\n");
			find_put_strikes(argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argv[7]);
			return;
		}
		if (strcmp(argv[8], "call") == 0) {
			find_call_strikes(argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argv[7]);
			return;
		}
		if (strcmp(argv[8], "put") == 0) {
			find_put_strikes(argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argv[7]);
			return;
		}
	}
	if (argc == 8) {
		if (strcmp(argv[7], "stoploss") == 0) {
			// get exit time and stop credit for entry time and stop multiplier
			mmap_stoploss(argv[1], argv[2], argv[3], atoi(argv[4]), atof(argv[5]), atof(argv[6]));
			return;
		}
		if (strcmp(argv[7], "stoplimitorder") == 0) {
			// get exit time and stop credit for stop price and limit price
			mmap_stop_limit_order(argv[1], argv[2], argv[3], atoi(argv[4]), atof(argv[5]), atof(argv[6]));
			return;
		}
		return;
	}

	//--------------------TESTING--------------------
	//clock_t start, end;
	//double cpu_time_used;
	//start = clock();
	//end = clock();

	//read_normal("20210312", "C1000");
	//read_mmap("20230103", "C4070", "C", "100000000");
	//read_mmap("20230412", "P4060", "A", "120000000");
	//mmap_stoploss("20230412", "C4135", "C4165", 120000000, 2.025, 1.25);
	//mmap_stop_limit_order("20230412", "P4060", "P4090", 100000000, 1, 0.9);
	//mmap_stoploss_v2("20230412", "P4140", "P4180", 100000000, 15, 2.5, 5000);
	//mmap_stop_limit_order_v2("20230412", "P4060", "P4090", 100000000, 1, 0.9, 5000);
	//find_call_strikes("20230410", "100000000", "30", "1.5", "3", "3000", "4000");
	//find_put_strikes("20230410", "100000000", "30", "1.5", "3", "3000", "4000");
	//find_call_strikes("20230410", "100000000", "30", "1.5", "3", "3000", "4000");
	//printf("Finished\n");

	//cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
	//printf("\n%sProcessing Time:%s %f seconds\n", CYAN, RESET, cpu_time_used);
	//return;
	//-----------------------------------------------

	clock_t start, end;
	double cpu_time_used;

	// process all files in directory
	start = clock();
	process_directory();
	end = clock();

	// print time taken to compress all files
	cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
	printf("\n%sProcessing Time:%s %f seconds\n", CYAN, RESET, cpu_time_used);
	return 0;
}