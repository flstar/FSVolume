#ifndef __VOLUME_H__
#define __VOLUME_H__

#include <string>

class Volume
{
public:
	Volume(const char *path);
	~Volume();
private:
	std::string path_;

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
