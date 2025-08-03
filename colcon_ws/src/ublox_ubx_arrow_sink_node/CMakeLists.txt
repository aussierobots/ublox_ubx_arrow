cmake_minimum_required(VERSION 3.8)
project(ublox_ubx_arrow_sink_node)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()
LIST(INSERT CMAKE_MODULE_PATH 0 ${CMAKE_CURRENT_SOURCE_DIR}/CMake)

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(ublox_ubx_msgs REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(std_msgs REQUIRED)
find_package(Zstd REQUIRED)
if (ZSTD_FOUND)
  set(ZSTD_LIB ${ZSTD_LIBRARY})
endif (ZSTD_FOUND)
find_package(Arrow REQUIRED)

include_directories(include SYSTEM ${ARROW_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR})

add_library(ublox_ubx_arrow_components SHARED
  src/arrow_sink_node.cpp
)

target_compile_definitions(ublox_ubx_arrow_components
  PRIVATE "UBLOX_UBX_ARROW_SINK_NODE_BUILDING_DLL"
)
target_compile_features(ublox_ubx_arrow_components PUBLIC c_std_99 cxx_std_20)  # Require C99 and C++20

ament_target_dependencies(ublox_ubx_arrow_components
  rclcpp
  rclcpp_components
  std_msgs
  ublox_ubx_msgs
)

target_link_libraries(ublox_ubx_arrow_components
  arrow
  parquet
)

rclcpp_components_register_node(ublox_ubx_arrow_components PLUGIN "ublox::ubx::ArrowSinkNode" EXECUTABLE ublox_ubx_arrow_sink_node)

install(TARGETS
  ublox_ubx_arrow_components
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  # the following line skips the linter which checks for copyrights
  # uncomment the line when a copyright and license is not present in all source files
  set(ament_cmake_copyright_FOUND TRUE)
  # the following line skips cpplint (only works in a git repo)
  # uncomment the line when this package is not in a git repo
  #set(ament_cmake_cpplint_FOUND TRUE)
  ament_lint_auto_find_test_dependencies()
endif()

ament_package()
