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
#include <chrono>
#include <ctime>

namespace ublox_ubx_arrow {

class ArrowSinkNode: public rclcpp::Node
{
public:
  UBLOX_UBX_ARROW_SINK_NODE_PUBLIC
  explicit ArrowSinkNode(const rclcpp::NodeOptions &options):
  Node("arrow_sink", rclcpp::NodeOptions(options).use_intra_process_comms(true))
  {
    RCLCPP_INFO(get_logger(), "starting %s", get_name());
  }
};
} // end namespace ublox_ubx_arrow

RCLCPP_COMPONENTS_REGISTER_NODE(ublox_ubx_arrow::ArrowSinkNode)