#ifndef __VOLUME_H__
#define __VOLUME_H__

#include <mutex>
#include <string>
#include <map>
#include <list>
#include <atomic>


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
	void pwrite(void *buff, int32_t len, uint64_t offset);
	void write(void *buff, int32_t len);
};

class Volume
{
	static const int FILE_SIZE_SHIFT = 34;						// for 16GB file size
	static const uint64_t FILE_SIZE = (1UL << FILE_SIZE_SHIFT);
	static const size_t POOL_SIZE = 256;

private:
	std::string path_;
	uint64_t offset_;

	std::mutex pool_mtx_;
	std::map<uint64_t, std::shared_ptr<VolumeFile> > fmap_;
	std::list<uint64_t> lru_list_;

private:
	std::shared_ptr<VolumeFile> getFile(uint64_t offset);

public:
	Volume(const char *path);
	virtual ~Volume();

public:
	void flush();
	uint64_t size();
	uint64_t tell();
	uint64_t seek(uint64_t offset);
	void truncate(uint64_t length);
	void append(const void *buff, int32_t len);
	void pread(void *buff, int32_t len, uint64_t offset);
	void pwrite(const void *buff, int32_t len, uint64_t offset);

};
#endif

