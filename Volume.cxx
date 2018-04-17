
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>

#include "Exception.h"
#include "Volume.h"


VolumeFile::VolumeFile(const char *fn)
{
	fn_ = fn;
	fd_ = open(fn, O_CREAT|O_RDWR);
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
			THROW_EXCEPTION(-1, "No more data in volume file %s", fn_.c_str());
		}

		offset += ret;
		len -= ret;
		curr += ret;
	}
}

void VolumeFile::pwrite(void *buff, int len, uint64_t offset)
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

void VolumeFile::write(void *buff, int len)
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

Volume::Volume(const char *path)
{
	path_ = path;

	struct stat statbuf;
	memset(&statbuf, 0x00, sizeof(statbuf));

	int ret =  stat(path, &statbuf);
	if (ret != 0) {
		if (errno == ENOENT) {
			ret = mkdir(path, 0755);
			if (ret != 0) {
				THROW_EXCEPTION(errno, "Failed to create volume directory %s", path);
			}
		}
		else {
			THROW_EXCEPTION(errno, "Failed to stat volume directory %s", path);
		}
	}

	else if (!S_ISDIR(statbuf.st_mode)) {
		THROW_EXCEPTION(ENOTDIR, "Volume path %s is NOT a directory", path);
	}

	// Scan all files
	DIR *dirp = opendir(path);
	if (dirp == nullptr) {
		THROW_EXCEPTION(errno, "Failed to open volume dir %s", path);
	}

	struct dirent *dire = nullptr;
	while ((dire = readdir(dirp)) != nullptr) {
		uint64_t offset;
		int n = sscanf(dire->d_name, "%lu.vf", &offset);
		if (n == 1) {
			// parse matching files and ignore others
			offset = offset << FILE_SIZE_SHIFT;
			fmap_[offset] = nullptr;
		}
	}
	if (errno != 0) {
		THROW_EXCEPTION(errno, "Failed to scan volume directory %s", path);
	}
}

Volume::~Volume()
{
}

std::shared_ptr<VolumeFile> Volume::getFile(uint64_t offset)
{
	std::unique_lock<std::mutex> _lck(pool_mtx_);
	return nullptr;
}

