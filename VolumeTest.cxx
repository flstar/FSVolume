#include <gtest/gtest.h>

#include "Volume.h"

TEST(Volume, Hello)
{
	printf("Hello world!\n");
}

int main(int argc, char *argv[])
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

