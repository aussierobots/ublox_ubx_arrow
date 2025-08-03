// Copyright 2021 Australian Robotics Supplies & Technology
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rclcpp/rclcpp.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "rcl_interfaces/msg/set_parameters_result.hpp"
#include <string>
#include <unistd.h>
#include "ublox_ubx_arrow_sink_node/visibility_control.h"
#include "ublox_ubx_arrow_sink_node/arrow_writer.hpp"
#include <chrono>
#include <ctime>
#include "ublox_ubx_msgs/msg/ubx_nav_hp_pos_llh.hpp"

using namespace std::chrono_literals;
using std::placeholders::_1;

namespace ublox::ubx {

class ArrowSinkNode: public rclcpp::Node
{
public:
  UBLOX_UBX_ARROW_SINK_NODE_PUBLIC
  explicit ArrowSinkNode(const rclcpp::NodeOptions &options):
  Node("arrow_sink", rclcpp::NodeOptions(options).use_intra_process_comms(true))
  {
    RCLCPP_INFO(get_logger(), "starting %s", get_name());

    auto arrow_build_info = &arrow::GetBuildInfo();
    RCLCPP_INFO(get_logger(), "arrow version: %s", arrow_build_info->version_string.c_str());

    data_path_=declare_parameter<std::string>("data_path","/tmp");

    ubx_nav_hpposllh_writer_ = std::make_unique<ArrowWriter<ublox_ubx_msgs::msg::UBXNavHPPosLLH>>(data_path_, "ubx_nav_hp_pos_llh");
    auto ubx_nav_hpposllh_data_schema_string = ubx_nav_hpposllh_writer_->schema()->ToString();
    RCLCPP_INFO(get_logger(), "ubx_nav_hpposllh data_schema: %s", ubx_nav_hpposllh_data_schema_string.c_str());

    // create subscriptions
    auto qos = rclcpp::SensorDataQoS();
    ubx_nav_hpposllh_sub_ = create_subscription<ublox_ubx_msgs::msg::UBXNavHPPosLLH>("/ubx_nav_hp_pos_llh", qos, std::bind(&ArrowSinkNode::ubx_nav_hpposllh_callback, this, _1));
  }

  UBLOX_UBX_ARROW_SINK_NODE_LOCAL
  ~ArrowSinkNode() {
    RCLCPP_INFO(get_logger(), "finishing ..");
    switch_queues();
    append_ubx_nav_hpposllh_record_batch();

    auto curr_ts = cur_timestamp();
    RCLCPP_INFO(get_logger(), "final write record batch timestamp: %s", curr_ts.c_str());

    write_ubx_nav_hpposllh_record_batch(curr_ts);
  }

private:
  std::string data_path_;
  rclcpp::Subscription<ublox_ubx_msgs::msg::UBXNavHPPosLLH>::SharedPtr ubx_nav_hpposllh_sub_;
  std::unique_ptr<ArrowWriter<ublox_ubx_msgs::msg::UBXNavHPPosLLH>> ubx_nav_hpposllh_writer_;

  UBLOX_UBX_ARROW_SINK_NODE_LOCAL
  void ubx_nav_hpposllh_callback(const ublox_ubx_msgs::msg::UBXNavHPPosLLH::SharedPtr msg) {
    ubx_nav_hpposllh_writer_->add_msg(msg);
  }

  UBLOX_UBX_ARROW_SINK_NODE_LOCAL
  inline void switch_queues() {
    ubx_nav_hpposllh_writer_->switch_queues();
  }

  UBLOX_UBX_ARROW_SINK_NODE_LOCAL
  void append_ubx_nav_hpposllh_record_batch() {
    if (ubx_nav_hpposllh_writer_->switched_msgs_size() == 0) {
      RCLCPP_WARN(get_logger(), "no new ubx_nav_hp_pos_llh messages.");
    } else {
      RCLCPP_INFO(get_logger(), "Append new ubx_nav_hp_pos_llh record batch ....");
      auto status = ubx_nav_hpposllh_writer_->append_record_batch();
      if (!status.ok()){
        RCLCPP_ERROR(get_logger(), "ubx_nav_hpposllh_writer_->append_record_batch() -> status: %s", status.ToString().c_str());
      } else {
        std::string rb_row_str = "[ ";
        for (size_t n: ubx_nav_hpposllh_writer_->record_batches_rows()){
          rb_row_str += std::to_string(n) + " ";
        }
        rb_row_str += "]";

        RCLCPP_INFO(get_logger(), "ubx_nav_hpposllh_writer_->record_batches_size(): %lu nums: %s", ubx_nav_hpposllh_writer_->record_batches_size(), rb_row_str.c_str());
      }

    }
  }

  UBLOX_UBX_ARROW_SINK_NODE_LOCAL
  void write_ubx_nav_hpposllh_record_batch(std::string cur_ts) {
    if (ubx_nav_hpposllh_writer_->record_batches_size() == 0) {
      RCLCPP_WARN(get_logger(), "nothing to write for ubx_nav_hp_pos_llh.");
      return;
    }

    auto status = ubx_nav_hpposllh_writer_->build_table();
    if (!status.ok()) {
      RCLCPP_ERROR(get_logger(), "ubx_nav_hpposllh_writer_->build_table() status: %s", status.ToString().c_str());
    } else {
      auto rows = ubx_nav_hpposllh_writer_->table_rows();
      RCLCPP_INFO(get_logger(),"ubx_nav_hpposllh_writer_->table_rows(): %lu", rows);
      auto fp = ubx_nav_hpposllh_writer_->filepath_parquet(cur_ts);

      RCLCPP_INFO(get_logger(), "ubx_nav_hpposllh_writer_->filepath_parquet(%s): %s", cur_ts.c_str(), fp.c_str() );

      auto status = ubx_nav_hpposllh_writer_->write_table_to_parquet(cur_ts);
      if (!status.ok()) {
        RCLCPP_ERROR(get_logger(), "ubx_nav_hpposllh_writer_->write_table_to_parquet() status: %s", status.ToString().c_str());
      } else {
        RCLCPP_INFO(get_logger(), "wrote %lu UBXNavHPPosLLH msgs to %s.", rows, fp.c_str());
      }
    }
  }
};
} // end namespace ublox_ubx_arrow

RCLCPP_COMPONENTS_REGISTER_NODE(ublox::ubx::ArrowSinkNode)