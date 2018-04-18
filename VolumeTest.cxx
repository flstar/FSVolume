#include <gtest/gtest.h>

#include "Volume.h"


TEST(Volume, Create)
{
	char path[] = "test.create";
	char clear_cmd[64];
	sprintf(clear_cmd, "rm -rf %s", path);
	
	system(clear_cmd);
	Volume *vol = new Volume(path);
	delete vol;

	vol = new Volume(path);
	delete vol;
	system(clear_cmd);
}

TEST(Volume, Append)
{
	char path[] = "test.append";
	char clear_cmd[64];
	sprintf(clear_cmd, "rm -rf %s", path);

	system(clear_cmd);
	Volume vol(path);

	char data[] = "Test";
	vol.append(data, strlen(data));

	char buff[64];
	memset(buff, 0x00, sizeof(buff));
	vol.pread(buff, strlen(data), 0UL);

	EXPECT_EQ(0, strcmp(data, buff));

	char data2[] = "Data";
	vol.append(data2, strlen(data2));

	memset(buff, 0x00, sizeof(buff));
	vol.pread(buff, strlen(data) + strlen(data2), 0UL);

	EXPECT_EQ(0, strcmp("TestData", buff));

	memset(buff, 0x00, sizeof(buff));
	vol.pread(buff, 4, 2UL);
	EXPECT_EQ(0, strcmp("stDa", buff));
	
	system(clear_cmd);
	
}

TEST(Volume, PWrite)
{
	char path[] = "test.pwrite";
	char clear_cmd[64];
	sprintf(clear_cmd, "rm -rf %s", path);
	system(clear_cmd);
	
	const int dsize = 1024;
	std::unique_ptr<char[]> _buff(new char[dsize]);
	char *buff = _buff.get();
	memset(buff, '1', dsize);

	Volume vol(path);
	vol.append(buff, dsize);

	vol.pwrite("0000", 4, 0UL);
	vol.pread(buff, 3, 0UL);
	EXPECT_EQ(0, memcmp("000", buff, 3));

	vol.pwrite("22", 2, 100UL);
	vol.pread(buff, 4, 99UL);
	EXPECT_EQ(0, memcmp("1221", buff, 4));

	vol.pwrite("33333", 5, dsize - 5);
	vol.pread(buff, 10, dsize - 10);
	EXPECT_EQ(0, memcmp("1111133333", buff, 10));
	
	system(clear_cmd);
}

TEST(Volume, Rotate)
{
	char path[] = "test.rotate";
	char clear_cmd[64];
	sprintf(clear_cmd, "rm -rf %s", path);
	system(clear_cmd);

	const int dsize = 3000;
	std::unique_ptr<char[]> _buff(new char[dsize]);
	char *buff = _buff.get();
	memset(buff, '1', 1024);
	memset(buff+1024, '2', 1024);
	memset(buff+2*1024, '3', dsize - 2*1024);

	Volume vol(path);
	vol.append(buff, dsize);

	std::unique_ptr<char[]> _buff2(new char[dsize]);
	char *buff2 = _buff2.get();
	memset(buff2, 0x00, dsize);
	vol.pread(buff2, dsize, 0UL);

	EXPECT_EQ(0, memcmp(buff, buff2, dsize));

	system(clear_cmd);
}

TEST(Volume, Truncate)
{
	char path[] = "test.truncate";
	char clear_cmd[64];
	sprintf(clear_cmd, "rm -rf %s", path);
	system(clear_cmd);

	const int dsize = 3000;
	std::unique_ptr<char[]> _buff(new char[dsize]);
	char *buff = _buff.get();
	memset(buff, '1', 1024);
	memset(buff+1024, '2', 1024);
	memset(buff+2*1024, '3', dsize - 2*1024);

	Volume vol(path);
	vol.append(buff, dsize);

	vol.truncate(1024);
	EXPECT_EQ(1024, vol.size());

	vol.append("5555", 4);

	std::unique_ptr<char[]> _buff2(new char[dsize]);
	char *buff2 = _buff2.get();

	vol.pread(buff2, 1028, 0UL);
	memset(buff+1024, '5', 4);

	EXPECT_EQ(0, memcmp(buff, buff2, 1028));

	system(clear_cmd);
}

int main(int argc, char *argv[])
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

