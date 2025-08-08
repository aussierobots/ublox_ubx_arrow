#include <gtest/gtest.h>
#include "ublox_ubx_arrow_sink_node/arrow_writer.hpp"

using ublox::ubx::ArrowWriter;
using ublox_ubx_msgs::msg::UBXNavHPPosLLH;

TEST(ArrowWriterTest, AppendRecordBatchClearsInactiveQueue)
{
  ArrowWriter<UBXNavHPPosLLH> writer{"", ""};

  for (int i = 0; i < 3; ++i) {
    auto msg = std::make_shared<UBXNavHPPosLLH>();
    msg->header.stamp.sec = i;
    msg->header.stamp.nanosec = i;
    msg->header.frame_id = "map";
    writer.add_msg(msg);
  }

  EXPECT_EQ(writer.switched_msgs_size(), 0u);

  writer.switch_queues();
  EXPECT_EQ(writer.switched_msgs_size(), 3u);

  auto status = writer.append_record_batch();
  ASSERT_TRUE(status.ok());

  EXPECT_EQ(writer.switched_msgs_size(), 0u);
  EXPECT_EQ(writer.record_batches_size(), 1u);
  auto rows = writer.record_batches_rows();
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(rows[0], 3u);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
