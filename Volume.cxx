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

std::string Volume::offsetToFilename(uint64_t offset)
{
	char buf[32];
	sprintf(buf, "%06lu.vf", offset >> FILE_SIZE_SHIFT);
	return buf;
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
			size_ = 0UL;
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
		int n = sscanf(dire->d_name, "%06lu.vf", &offset);
		if (n == 1) {
			// parse matching files and ignore others
			offset = offset << FILE_SIZE_SHIFT;
			fmap_[offset] = nullptr;
		}
	}
	if (errno != 0) {
		THROW_EXCEPTION(errno, "Failed to scan volume directory %s", path);
	}
	closedir(dirp);

	// Open last file as curr_file_
	auto rit = fmap_.rbegin();
	std::string fn = path_ + "/" + offsetToFilename(rit->first);
	std::shared_ptr<VolumeFile> f(new VolumeFile(fn.c_str()));

	flist_.push_back(rit->first);
	fmap_[rit->first] = f;
	curr_file_ = f;
}

Volume::~Volume()
{
}

void Volume::evictFileLocked()
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
				// Only used by fmap_ and f
				fmap_.erase(offset);
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
	std::unique_lock<std::mutex> _lck(pool_mtx_);

	auto riter = fmap_.rbegin();
	if (offset - (riter->first) >= FILE_SIZE) {
		// If offset is beyond current head file, create a new file for it
		evictFileLocked();
		std::string fn = offsetToFilename(offset);
		std::shared_ptr<VolumeFile> f(new VolumeFile(fn.c_str()));
		flist_.push_back(offset & FILE_START_MASK);
		fmap_[offset & FILE_START_MASK] = f;
		return f;
	}

	// search for matching file
	auto iter = fmap_.lower_bound(offset);
	if (iter->second != nullptr) {
		// if file has been opened, return it
		flist_.remove(iter->first);
		flist_.push_back(iter->first);
		return iter->second;
	}

	// Open the file
	evictFileLocked();
	std::string fn = path_ + "/" + offsetToFilename(iter->first);
	std::shared_ptr<VolumeFile> f(new VolumeFile(fn.c_str()));
	flist_.push_back(iter->first);
	fmap_[iter->first] = f;
	return f;
}

uint64_t Volume::size()
{
	std::unique_lock<std::mutex> _lck(write_mtx_);
	return size_;
}

void Volume::rotateFile()
{
	assert((size_ & FILE_OFFSET_MASK) == 0);
	
	std::unique_lock<std::mutex> _lck(pool_mtx_);

	evictFileLocked();

	std::string fn = path_ + "/" + offsetToFilename(size_);
	std::shared_ptr<VolumeFile> f(new VolumeFile(fn.c_str()));

	flist_.push_back(size_);
	fmap_[size_] = f;
	curr_file_ = f;
}

void Volume::append(const void *buff, int32_t len)
{
	std::unique_lock<std::mutex> _lck(write_mtx_);

	while (len > 0) {
		uint64_t curr_offset = size_ & FILE_OFFSET_MASK;
		uint64_t minlen = std::min(FILE_SIZE - curr_offset, uint64_t(len));

		curr_file_->write(buff, minlen);
		size_ += minlen;
		len -= minlen;
		buff = (char *)buff + minlen;
		
		// If data segment cross the border of files, rotate to next file and continue writing
		if (len > 0) {
			rotateFile();
		}
	}
	return;
}

void Volume::pwrite(const void *buff, int32_t len, uint64_t offset)
{
	// We are not going to deal with writing beyond the end of volume
	assert(offset + len <= size_);
	
	std::unique_lock<std::mutex> _lck(write_mtx_);

	while (len > 0) {
		std::shared_ptr<VolumeFile> f = getFile(offset);
		uint64_t in_offset = offset & FILE_OFFSET_MASK;
		uint64_t minlen = std::min(FILE_SIZE - in_offset, uint64_t(len));

		f->pwrite(buff, int32_t(minlen), offset);
		offset += minlen;
		len -= minlen;
		buff = (char *)buff + minlen;
	}
	
	return;
}

void Volume::pread(void *buff, int32_t len, uint64_t offset)
{
	// We are not going to deal with reading beyond the end of volume
	assert(offset + len <= size_);

	while (len > 0) {
		std::shared_ptr<VolumeFile> f = getFile(offset);
		uint64_t in_offset = offset & FILE_OFFSET_MASK;
		uint64_t minlen = std::min(FILE_SIZE - in_offset, uint64_t(len));

		f->pread(buff, int32_t(minlen), offset);
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

void Volume::truncate(uint64_t length)
{
	// We are not going to deal with truncating to a longer length
	assert(length <= size_);
	
	std::unique_lock<std::mutex> _lck(write_mtx_);
	
	while (size_ > length) {
		if ((size_ & FILE_START_MASK) != (length & FILE_START_MASK)) {
			// Different files, delete current file
			std::string fn = path_ + "/" + offsetToFilename(size_);
			VolumeFile::unlink(fn.c_str());
			// Delete possible cached file handler
			std::unique_lock<std::mutex> _lck2(pool_mtx_);
			fmap_.erase(size_ & FILE_START_MASK);
			flist_.remove(size_ & FILE_START_MASK);
			size_ = (size_ & FILE_START_MASK) - 1;
		}
		else {
			// Reach the new last file
			curr_file_ = getFile(length);
			curr_file_->truncate(length & FILE_OFFSET_MASK);
			curr_file_->seek(length & FILE_OFFSET_MASK);
			size_ = length;
		}
	}
}

#undef STATIC

