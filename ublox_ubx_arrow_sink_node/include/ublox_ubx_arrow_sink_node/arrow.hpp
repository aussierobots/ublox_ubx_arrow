#ifndef UBLOX_UBX_ARROW_SINK_NODE_ARROW_HPP
#define UBLOX_UBX_ARROW_SINK_NODE_ARROW_HPP

#include <iostream>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>


namespace ublox::ubx {
  using namespace ::arrow;

  const std::string TIMEZONE ("UTC");
  // const arrow::TimeUnit::type TIMEUNIT = arrow::TimeUnit::MICRO;
  const arrow::TimeUnit::type TIMEUNIT = arrow::TimeUnit::NANO;
  
  // https://github.com/ros2/rcl_interfaces/blob/master/builtin_interfaces/msg/Time.msg
  namespace time {
    auto sec = arrow::field("sec", arrow::int32(), false);
    auto nanosec = arrow::field("nanosec", arrow::uint32(), false);
  }
  auto time_struct = arrow::struct_({time::sec, time::nanosec});

  // https://github.com/ros2/common_interfaces/blob/master/std_msgs/msg/Header.msg
  namespace header {
    auto stamp = arrow::field("stamp", time_struct, false);
    auto frame_id = arrow::field("frame_id", arrow::utf8(), false);
  }

  auto header_struct = arrow::struct_({header::stamp, header::frame_id});

  // https://github.com/aussierobots/ublox_dgnss/blob/main/ublox_ubx_msgs/msg/UBXNavHPPosLLH.msg
  namespace nav::hp_pos_llh {
    auto timestamp = arrow::field("timestamp", arrow::timestamp(TIMEUNIT, TIMEZONE), false);
    auto header = arrow::field("header", header_struct, false);
    auto version = arrow::field("version", arrow::uint8(), false);
    auto invalid_lon = arrow::field("invalid_lon", arrow::boolean(), false);
    auto invalid_lat = arrow::field("invalid_lat", arrow::boolean(), false);
    auto invalid_height = arrow::field("invalid_height", arrow::boolean(), false);
    auto invalid_hmsl = arrow::field("invalid_hmsl", arrow::boolean(), false);
    auto invalid_lon_hp = arrow::field("invalid_lon_lp", arrow::boolean(), false);
    auto invalid_lat_hp = arrow::field("invalid_lat_hp", arrow::boolean(), false);
    auto invalid_height_hp = arrow::field("invalid_height_hp", arrow::boolean(), false);
    auto invalid_hmsl_hp = arrow::field("invalid_hmsl_hp", arrow::boolean(), false);
    auto itow = arrow::field("itow", arrow::uint32(), false);
    auto lon = arrow::field("lon", arrow::int32(), false);
    auto lat = arrow::field("lat", arrow::int32(), false);
    auto height = arrow::field("height", arrow::int32(), false);
    auto hmsl = arrow::field("hmsl", arrow::int32(), false);
    auto lon_hp = arrow::field("lon_hp", arrow::int8(), false);
    auto lat_hp = arrow::field("lat_hp", arrow::int8(), false);
    auto height_hp = arrow::field("height_hp", arrow::int8(), false);
    auto hmsl_hp = arrow::field("hmsl_hp", arrow::int8(), false);
    auto h_acc = arrow::field("h_acc", arrow::uint32(), false);
    auto v_acc = arrow::field("v_acc", arrow::uint32(), false);
  }

  namespace nav {
    auto hp_pos_llh_struct = arrow::struct_({
      hp_pos_llh::timestamp,
      hp_pos_llh::header,
      hp_pos_llh::version,
      hp_pos_llh::invalid_lon,
      hp_pos_llh::invalid_lat,
      hp_pos_llh::invalid_height,
      hp_pos_llh::invalid_hmsl,
      hp_pos_llh::invalid_lon_hp,
      hp_pos_llh::invalid_lat_hp,
      hp_pos_llh::invalid_height_hp,
      hp_pos_llh::invalid_hmsl_hp,
      hp_pos_llh::itow,
      hp_pos_llh::lon,
      hp_pos_llh::lat,
      hp_pos_llh::height,
      hp_pos_llh::hmsl,
      hp_pos_llh::lon_hp,
      hp_pos_llh::lat_hp,
      hp_pos_llh::height_hp,
      hp_pos_llh::hmsl_hp,
      hp_pos_llh::h_acc,
      hp_pos_llh::v_acc
    });
  }
}

#endif // UBLOX_UBX_ARROW_SINK_NODE_ARROW_HPP