#ifndef __EXCEPTION_H__
#define __EXCEPTION_H__

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include <exception>

class Exception : public std::exception
{
	static const size_t MAX_MSG_LEN = 256;
private:
	int32_t code_;
	char msg_[MAX_MSG_LEN];

public:
	Exception() {
		code_ = 0;
		memset(msg_, 0x00, MAX_MSG_LEN);
	}

	Exception(int code) {
		code_ = code;
	}

	Exception(int code, const char *fmt, ...)
	{
		code_ = code;
		sprintf(msg_, "[%d]", code);
		size_t pos = strlen(msg_);
		
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(msg_ + pos, MAX_MSG_LEN - pos, fmt, ap);
		va_end(ap);
	}
	
	virtual ~Exception() noexcept override { }
	virtual const char *what() const noexcept override { return msg_; }
};

#define THROW_EXCEPTION(code, fmt, ...) throw Exception(code, "[%s:%d]" fmt, __FILE__, __LINE__, ##__VA_ARGS__)

#endif
