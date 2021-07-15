cmake_minimum_required(VERSION 3.8)
project(ublox_ubx_arrow)

set (ARROW_ZSTD_USE_SHARED ON)

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(ublox_ubx_msgs REQUIRED)
find_package(ublox_ubx_arrow_sink_node REQUIRED)


ament_package()
