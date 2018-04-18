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
	static uint64_t size(const char *fn);
	uint64_t tell();
	void sync();
	void seek(uint64_t offset);
	void truncate(uint64_t length);
	void pread(void *buff, int32_t len, uint64_t offset);
	void pwrite(const void *buff, int32_t len, uint64_t offset);
	void write(const void *buff, int32_t len);

	static void unlink(const char *fn);
};

class Volume
{
private:
	// Paramters
	int FILE_SIZE_SHIFT;
	uint64_t FILE_SIZE;
	uint64_t FILE_OFFSET_MASK;
	uint64_t FILE_START_MASK;
	size_t FILE_POOL_SIZE;

private:
	std::string path_;
	std::mutex write_mtx_;			// serialize all write operations
	
	std::mutex fmtx_;
	std::map<uint64_t, std::shared_ptr<VolumeFile> > fmap_;
	std::list<uint64_t> flist_;

private:
	std::string offsetToPathfile(uint64_t offset);
	void evictFileWithLock();
	std::shared_ptr<VolumeFile> getFile(uint64_t offset);
	uint64_t provisioned_length_;

public:
	Volume(const char *path, int file_size_shift = 30, size_t pool_size = 256);		// 1GB
	virtual ~Volume();

	void pwrite(const void *buff, int32_t len, uint64_t offset);
	void pread(void *buff, int32_t len, uint64_t offset);
	void sync();

	uint64_t getProvisionedLength() { return provisioned_length_; }
};
#endif

