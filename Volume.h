#ifndef __VOLUME_H__
#define __VOLUME_H__

#include <mutex>
#include <string>
#include <map>
#include <list>


class VolumeFile
{
private:
	std::string fn_;
	int fd_;

public:
	VolumeFile(const char *fn);
	virtual ~VolumeFile();

	uint64_t size();
	uint64_t tell();
	void flush();
	void seek(uint64_t offset);
	void truncate(uint64_t length);
	void pread(void *buff, int32_t len, uint64_t offset);
	void pwrite(const void *buff, int32_t len, uint64_t offset);
	void write(const void *buff, int32_t len);

	static void unlink(const char *fn);
};

class Volume
{
	static int FILE_SIZE_SHIFT;
	static uint64_t FILE_SIZE;
	static uint64_t FILE_OFFSET_MASK;
	static uint64_t FILE_START_MASK;
	static size_t FILE_POOL_SIZE;

private:
	std::string path_;
	std::mutex write_mtx_;			// serialize all write operations, protect size_ and curr_file_
	
	std::mutex fmtx_;
	std::map<uint64_t, std::shared_ptr<VolumeFile> > fmap_;
	std::list<uint64_t> flist_;

private:
	std::string offsetToPathfile(uint64_t offset);
	void evictFileWithLock();
	std::shared_ptr<VolumeFile> getFile(uint64_t offset, uint64_t *start, uint64_t *end);

public:
	Volume(const char *path);
	virtual ~Volume();

public:
	void flush();
	void pwrite(const void *buff, int32_t len, uint64_t offset);
	void pread(void *buff, int32_t len, uint64_t offset);
};
#endif

