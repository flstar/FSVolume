#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <assert.h>

#include "Exception.h"
#include "Volume.h"

#define STATIC

VolumeFile::VolumeFile(const char *fn)
{
	fn_ = fn;
	fd_ = open(fn, O_CREAT|O_RDWR, 0644);
	if (fd_ < 0) {
		THROW_EXCEPTION(errno, "Failed to open/create file %s", fn);
	}
}

VolumeFile::~VolumeFile()
{
	close(fd_);
	fd_ = -1;
}

uint64_t VolumeFile::size()
{
	struct stat statbuf;
	memset(&statbuf, 0x00, sizeof(statbuf));

	int ret = fstat(fd_, &statbuf);
	if (ret < 0) {
		THROW_EXCEPTION(errno, "Failed to fstat file %s", fn_.c_str());
	}

	return statbuf.st_size;
}

void VolumeFile::seek(uint64_t offset)
{
	off_t new_offset = lseek(fd_, offset, SEEK_SET);
	if (new_offset < 0) {
		THROW_EXCEPTION(errno, "Failed to seek to %lu in file %s", offset, fn_.c_str());
	}
}

void VolumeFile::flush()
{
	int ret = fsync(fd_);
	if (ret < 0) {
		THROW_EXCEPTION(errno, "Failed to flush volume file %s", fn_.c_str());
	}
}

uint64_t VolumeFile::tell()
{
	off_t curr_offset = lseek(fd_, off_t(0), SEEK_CUR);
	return curr_offset;
}

void VolumeFile::truncate(uint64_t length)
{
	int ret = ftruncate(fd_, length);
	if (ret < 0) {
		THROW_EXCEPTION(errno, "Failed to truncate volume file %s", fn_.c_str());
	}
	return;
}

void VolumeFile::pread(void *buff, int len, uint64_t offset)
{
	char *curr = (char *)buff;
	while (len > 0) {
		int ret = ::pread(fd_, curr, len, offset);
		if (ret < 0) {
			if (errno == EINTR) {
				ret = 0;
			}
			else {
				THROW_EXCEPTION(errno, "Failed to pread volume file %s", fn_.c_str());
			}
		}
		else if (ret == 0) {
			// fill 0s for non-existing data
			memset(buff, 0x00, len);
			ret = len;
		}

		offset += ret;
		len -= ret;
		curr += ret;
	}
}

void VolumeFile::pwrite(const void *buff, int len, uint64_t offset)
{
	char *curr = (char *)buff;
	while (len > 0) {
		int ret = ::pwrite(fd_, curr, len, offset);
		if (ret < 0) {
			if (errno == EINTR) {
				ret = 0;
			}
			else {
				THROW_EXCEPTION(errno, "Failed to pwrite volume file %s", fn_.c_str());
			}
		}

		offset += ret;
		len -= ret;
		curr += ret;
	}
}

void VolumeFile::write(const void *buff, int len)
{
	char *curr = (char *)buff;
	while (len > 0) {
		int ret = ::write(fd_, curr, len);
		if (ret < 0) {
			if (errno == EINTR) {
				ret = 0;
			}
			else {
				THROW_EXCEPTION(errno, "Failed to write volume file %s", fn_.c_str());
			}
		}

		len -= ret;
		curr += ret;
	}
}


STATIC
void VolumeFile::unlink(const char *fn)
{
	int ret = ::unlink(fn);
	if (ret != 0) {
		THROW_EXCEPTION(errno, "Failed to unlink volume file %s", fn);
	}
	return;
}

int Volume::FILE_SIZE_SHIFT = 10;							// 1KB for test only
uint64_t Volume::FILE_SIZE = (1UL << FILE_SIZE_SHIFT);
uint64_t Volume::FILE_OFFSET_MASK = (FILE_SIZE - 1);
uint64_t Volume::FILE_START_MASK = ~(FILE_SIZE - 1);
size_t Volume::FILE_POOL_SIZE = 256;

std::string Volume::offsetToPathfile(uint64_t offset)
{
	char buf[32];
	sprintf(buf, "%06lu.vf", offset >> FILE_SIZE_SHIFT);
	return path_ + "/" + buf;
}

Volume::Volume(const char *path)
{
	path_ = path;

	struct stat statbuf;
	memset(&statbuf, 0x00, sizeof(statbuf));

	int ret =  stat(path, &statbuf);
	if (ret != 0) {
		if (errno == ENOENT) {
			// If volume dir does not exist, create it
			ret = mkdir(path, 0755);
			if (ret != 0) {
				THROW_EXCEPTION(errno, "Failed to create volume directory %s", path);
			}
		}
		else {
			THROW_EXCEPTION(errno, "Failed to stat volume directory %s", path);
		}
		getFile(0UL);
	}

	else if (!S_ISDIR(statbuf.st_mode)) {
		THROW_EXCEPTION(ENOTDIR, "Volume path %s is NOT a directory", path);
	}
}

Volume::~Volume()
{
}

void Volume::evictFileWithLock()
{
	if (flist_.size() < FILE_POOL_SIZE) {
		return;
	}

	int counter = 0;
	while (counter == 0) {
		for (auto iter = flist_.begin(); iter != flist_.end() && counter < 8; iter++) {
			uint64_t offset = *iter;
			std::shared_ptr<VolumeFile> f = fmap_[offset];
			if (f.use_count() <= 2) {
				// Only referred by fmap_ and f
				fmap_[offset].reset();
				flist_.remove(offset);
				f.reset();
				++ counter;
			}
		}
		if (counter == 0) {
			// No file was evicted, sleep for a while and try again
			usleep(1);
		}
	}
	return;
}

std::shared_ptr<VolumeFile> Volume::getFile(uint64_t offset)
{
	std::unique_lock<std::mutex> _flck(fmtx_);

	uint64_t start = offset & FILE_START_MASK;
	auto iter = fmap_.find(start);
	if (iter == fmap_.end()) {
		// If choosen file it not opened yet, open it
		evictFileWithLock();
		std::string fn = offsetToPathfile(offset);
		std::shared_ptr<VolumeFile> f(new VolumeFile(fn.c_str()));
		flist_.push_back(start);
		fmap_[start] = f;
	}
	else {
		// If choosen file has been opened, move it to the end of lru list
		flist_.remove(start);
		flist_.push_back(start);
	}
	
	return fmap_[start];
}

void Volume::pwrite(const void *buff, int32_t len, uint64_t offset)
{
	std::unique_lock<std::mutex> _wlck(write_mtx_);

	while (len > 0) {
		std::shared_ptr<VolumeFile> f = getFile(offset);
		uint64_t start = offset & FILE_START_MASK;
		uint64_t in_offset = offset - start;
		uint64_t minlen = std::min(FILE_SIZE - in_offset, uint64_t(len));

		f->pwrite(buff, int32_t(minlen), in_offset);
		offset += minlen;
		len -= minlen;
		buff = (char *)buff + minlen;
	}
	
	return;
}

void Volume::pread(void *buff, int32_t len, uint64_t offset)
{
	while (len > 0) {
		std::shared_ptr<VolumeFile> f = getFile(offset);
		uint64_t start = offset & FILE_START_MASK;
		uint64_t in_offset = offset - start;
		uint64_t minlen = std::min(FILE_SIZE - in_offset, uint64_t(len));

		f->pread(buff, int32_t(minlen), in_offset);
		offset += minlen;
		len -= minlen;
		buff = (char *)buff + minlen;
	}

	return;
}

void Volume::flush()
{
	for (auto it = flist_.begin(); it != flist_.end(); it++) {
		auto mapiter = fmap_.find(*it);
		std::shared_ptr<VolumeFile> f = mapiter->second;
		f->flush();
	}
	return;
}

#undef STATIC

