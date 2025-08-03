#ifndef UBLOX_UBX_ARROW_SINK_NODE_ARROW_WRITER_HPP
#define UBLOX_UBX_ARROW_SINK_NODE_ARROW_WRITER_HPP
#include <iostream>
#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/properties.h>
#include <parquet/parquet_version.h>
#include <chrono>
#include <vector>
#include <unistd.h>
#include <ctime>
#include "ublox_ubx_msgs/msg/ubx_nav_hp_pos_llh.hpp"
#include "arrow.hpp"

using namespace arrow;

namespace ublox::ubx {

  template <typename MessageT>
  class ArrowWriter {
  private:
    std::shared_ptr<arrow::Schema> schema_;

    // use two queues - only append to one at a time
    uint8_t active_msq_queue_;
    std::vector<typename MessageT::SharedPtr> msgs_0_;
    std::vector<typename MessageT::SharedPtr> msgs_1_;

    std::shared_ptr<arrow::Table> table_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches_;

    std::string data_path_;
    std::string filename_;

    size_t parquet_chunk_size_ = 1000;

  public:
    ArrowWriter(const std::string &data_path, const std::string &filename) {
      data_path_ = data_path;
      filename_ = filename;
      schema_ = schema();
      msgs_0_.clear();
      msgs_1_.clear();
      active_msq_queue_=0;
    }
    std::shared_ptr<arrow::Schema> schema ();
    size_t msgs_size();
    size_t switched_msgs_size();
    void add_msg(typename MessageT::SharedPtr msg);
    void switch_queues();
    arrow::Status append_record_batch();
    size_t record_batches_size();
    std::vector<size_t> record_batches_rows();
    arrow::Status build_table();
    std::string table_tostring();
    size_t table_rows();
    std::shared_ptr<arrow::Schema> table_schema();
    arrow::Status write_table_to_parquet(std::string cur_ts);

    std::string filepath_parquet(std::string cur_ts) {
      auto filepath = data_path_+"/"+filename_+"_"+cur_ts+".parquet";
      return filepath;
    }

  private:

    std::vector<typename MessageT::SharedPtr> move_msgs();

    arrow::Status build_record_batch(std::shared_ptr<arrow::RecordBatch> * record_batch);

    arrow::MemoryPool* get_memory_pool() {
      return arrow::default_memory_pool();
    }
  };

  template<>
  std::shared_ptr<arrow::Schema> ArrowWriter<ublox_ubx_msgs::msg::UBXNavHPPosLLH>::schema() {
    if (schema_ == nullptr)
      schema_ = arrow::schema({
                              nav::hp_pos_llh::timestamp,
                              nav::hp_pos_llh::header,
                              nav::hp_pos_llh::version,
                              nav::hp_pos_llh::invalid_lon,
                              nav::hp_pos_llh::invalid_lat,
                              nav::hp_pos_llh::invalid_height,
                              nav::hp_pos_llh::invalid_hmsl,
                              nav::hp_pos_llh::invalid_lon_hp,
                              nav::hp_pos_llh::invalid_lat_hp,
                              nav::hp_pos_llh::invalid_height_hp,
                              nav::hp_pos_llh::invalid_hmsl_hp,
                              nav::hp_pos_llh::itow,
                              nav::hp_pos_llh::lon,
                              nav::hp_pos_llh::lat,
                              nav::hp_pos_llh::height,
                              nav::hp_pos_llh::hmsl,
                              nav::hp_pos_llh::lon_hp,
                              nav::hp_pos_llh::lat_hp,
                              nav::hp_pos_llh::height_hp,
                              nav::hp_pos_llh::hmsl_hp,
                              nav::hp_pos_llh::h_acc,
                              nav::hp_pos_llh::v_acc
                              });

    return schema_;
  }

  template <typename MessageT>
  size_t ArrowWriter<MessageT>::msgs_size() {
    size_t n;
    if (active_msq_queue_ ==0)
      n = msgs_0_.size();
    else
      n = msgs_1_.size();
    return n;
  }

  template <typename MessageT>
  size_t ArrowWriter<MessageT>::switched_msgs_size() {
    size_t n;
    if (active_msq_queue_ ==0)
      n = msgs_1_.size();
    else
      n = msgs_0_.size();
    return n;
  }

  template <typename MessageT>
  size_t ArrowWriter<MessageT>::record_batches_size() {
    return record_batches_.size();
  }

  template <typename MessageT>
  std::vector<size_t> ArrowWriter<MessageT>::record_batches_rows() {
    std::vector<size_t> batches_rows;
    for (auto rb: record_batches_) {
      batches_rows.push_back(rb->num_rows());
    }
    return batches_rows;
  }

  template <typename MessageT>
  size_t ArrowWriter<MessageT>::table_rows() {
    return table_->num_rows();
  }

  template <typename MessageT>
  std::shared_ptr<arrow::Schema> ArrowWriter<MessageT>::table_schema() {
    return table_->schema();
  }

  template <typename MessageT>
  std::string ArrowWriter<MessageT>::table_tostring() {
    return table_->ToString();
  }


  template<typename MessageT>
  void ArrowWriter<MessageT>::switch_queues() {
    if (active_msq_queue_ == 0){
      active_msq_queue_ = 1;
    } else {
      active_msq_queue_ = 0;
    }
  }

  template<typename MessageT>
  std::vector<typename MessageT::SharedPtr> ArrowWriter<MessageT>::move_msgs(){

    // whatever is not the active queue, use as the msg source
    // the queues should have been switched before append
    if (active_msq_queue_ == 0) {
      std::vector<typename MessageT::SharedPtr> msgs(msgs_1_);
      msgs_1_.clear();
      return msgs;
    } else {
      std::vector<typename MessageT::SharedPtr> msgs(msgs_0_);
      msgs_0_.clear();
      return msgs;
    }
  }

  template <typename MessageT>
  void ArrowWriter<MessageT>::add_msg(typename MessageT::SharedPtr msg) {
    if (active_msq_queue_ == 0)
      msgs_0_.push_back(msg);
    else
      msgs_1_.push_back(msg);
  }

  template <typename MessageT>
  arrow::Status ArrowWriter<MessageT>::append_record_batch() {
    if (switched_msgs_size() == 0) {
      return arrow::Status::OK();
    }

    std::shared_ptr<arrow::RecordBatch> record_batch;
    ARROW_RETURN_NOT_OK(build_record_batch(&record_batch));
    record_batches_.push_back(record_batch);

    return arrow::Status::OK();
  }

  template <>
  arrow::Status ArrowWriter<ublox_ubx_msgs::msg::UBXNavHPPosLLH>::build_record_batch(std::shared_ptr<arrow::RecordBatch>* record_batch) {
    // make a copy and start using the other queue
    auto hpposllh_msgs = move_msgs();
    size_t msgs_size = hpposllh_msgs.size();

    auto pool = get_memory_pool();

    arrow::TimestampBuilder timestamp_builder(arrow::timestamp(TIMEUNIT, TIMEZONE), pool);
    arrow::Int32Builder sec_builder(pool);
    arrow::UInt32Builder nanosec_builder(pool);
    arrow::StringBuilder frame_id_builder(pool);

    arrow::UInt8Builder version_builder(pool);
    arrow::BooleanBuilder invalid_lon_builder(pool);
    arrow::BooleanBuilder invalid_lat_builder(pool);
    arrow::BooleanBuilder invalid_height_builder(pool);
    arrow::BooleanBuilder invalid_hmsl_builder(pool);
    arrow::BooleanBuilder invalid_lon_hp_builder(pool);
    arrow::BooleanBuilder invalid_lat_hp_builder(pool);
    arrow::BooleanBuilder invalid_height_hp_builder(pool);
    arrow::BooleanBuilder invalid_hmsl_hp_builder(pool);
    arrow::UInt32Builder itow_builder(pool);
    arrow::Int32Builder lon_builder(pool);
    arrow::Int32Builder lat_builder(pool);
    arrow::Int32Builder height_builder(pool);
    arrow::Int32Builder hmsl_builder(pool);
    arrow::Int8Builder lon_hp_builder(pool);
    arrow::Int8Builder lat_hp_builder(pool);
    arrow::Int8Builder height_hp_builder(pool);
    arrow::Int8Builder hmsl_hp_builder(pool);
    arrow::UInt32Builder h_acc_builder(pool);
    arrow::UInt32Builder v_acc_builder(pool);

    for (auto msg: hpposllh_msgs) {
      int64_t ts_nano = msg->header.stamp.sec * 1e+9 + msg->header.stamp.nanosec;
      // int64_t ts_micro = ts_nano / 1e+3;
      // ARROW_RETURN_NOT_OK(timestamp_builder.Append(ts_micro));
      ARROW_RETURN_NOT_OK(timestamp_builder.Append(ts_nano));
      ARROW_RETURN_NOT_OK(sec_builder.Append(msg->header.stamp.sec));
      ARROW_RETURN_NOT_OK(nanosec_builder.Append(msg->header.stamp.nanosec));
      ARROW_RETURN_NOT_OK(frame_id_builder.Append(msg->header.frame_id));
      ARROW_RETURN_NOT_OK(version_builder.Append(msg->version));
      ARROW_RETURN_NOT_OK(invalid_lon_builder.Append(msg->invalid_lon));
      ARROW_RETURN_NOT_OK(invalid_lat_builder.Append(msg->invalid_lat));
      ARROW_RETURN_NOT_OK(invalid_height_builder.Append(msg->invalid_height));
      ARROW_RETURN_NOT_OK(invalid_hmsl_builder.Append(msg->invalid_hmsl));
      ARROW_RETURN_NOT_OK(invalid_lon_hp_builder.Append(msg->invalid_lon_hp));
      ARROW_RETURN_NOT_OK(invalid_lat_hp_builder.Append(msg->invalid_lat_hp));
      ARROW_RETURN_NOT_OK(invalid_height_hp_builder.Append(msg->invalid_height_hp));
      ARROW_RETURN_NOT_OK(invalid_hmsl_hp_builder.Append(msg->invalid_hmsl_hp));
      ARROW_RETURN_NOT_OK(itow_builder.Append(msg->itow));
      ARROW_RETURN_NOT_OK(lon_builder.Append(msg->lon));
      ARROW_RETURN_NOT_OK(lat_builder.Append(msg->lat));
      ARROW_RETURN_NOT_OK(height_builder.Append(msg->height));
      ARROW_RETURN_NOT_OK(hmsl_builder.Append(msg->hmsl));
      ARROW_RETURN_NOT_OK(lon_hp_builder.Append(msg->lon_hp));
      ARROW_RETURN_NOT_OK(lat_hp_builder.Append(msg->lat_hp));
      ARROW_RETURN_NOT_OK(height_hp_builder.Append(msg->height_hp));
      ARROW_RETURN_NOT_OK(hmsl_hp_builder.Append(msg->hmsl_hp));
      ARROW_RETURN_NOT_OK(h_acc_builder.Append(msg->h_acc));
      ARROW_RETURN_NOT_OK(v_acc_builder.Append(msg->v_acc));
    }

    std::shared_ptr<arrow::Array> timestamp_array;
    ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&timestamp_array));
    // build header struct array
    std::shared_ptr<arrow::Array> sec_array;
    ARROW_RETURN_NOT_OK(sec_builder.Finish(&sec_array));
    std::shared_ptr<arrow::Array> nanosec_array;
    ARROW_RETURN_NOT_OK(nanosec_builder.Finish(&nanosec_array));
    std::vector<std::shared_ptr<arrow::Array>> time_children = {sec_array, nanosec_array};
    auto time_struct_array = std::make_shared<arrow::StructArray>(time_struct, msgs_size, time_children);

    std::shared_ptr<arrow::Array> frame_id_array;
    ARROW_RETURN_NOT_OK(frame_id_builder.Finish(&frame_id_array));

    std::vector<std::shared_ptr<arrow::Array>> header_children = {time_struct_array, frame_id_array};
    auto header_struct_array = std::make_shared<arrow::StructArray>(header_struct, msgs_size, header_children);

    std::shared_ptr<arrow::Array> version_array;
    ARROW_RETURN_NOT_OK(version_builder.Finish(&version_array));

    std::shared_ptr<arrow::Array> invalid_lon_array, invalid_lat_array, invalid_height_array, invalid_hmsl_array;
    ARROW_RETURN_NOT_OK(invalid_lon_builder.Finish(&invalid_lon_array));
    ARROW_RETURN_NOT_OK(invalid_lat_builder.Finish(&invalid_lat_array));
    ARROW_RETURN_NOT_OK(invalid_height_builder.Finish(&invalid_height_array));
    ARROW_RETURN_NOT_OK(invalid_hmsl_builder.Finish(&invalid_hmsl_array));

    std::shared_ptr<arrow::Array> invalid_lon_hp_array, invalid_lat_hp_array, invalid_height_hp_array, invalid_hmsl_hp_array;
    ARROW_RETURN_NOT_OK(invalid_lon_hp_builder.Finish(&invalid_lon_hp_array));
    ARROW_RETURN_NOT_OK(invalid_lat_hp_builder.Finish(&invalid_lat_hp_array));
    ARROW_RETURN_NOT_OK(invalid_height_hp_builder.Finish(&invalid_height_hp_array));
    ARROW_RETURN_NOT_OK(invalid_hmsl_hp_builder.Finish(&invalid_hmsl_hp_array));

    std::shared_ptr<arrow::Array> itow_array;
    ARROW_RETURN_NOT_OK(itow_builder.Finish(&itow_array));

    std::shared_ptr<arrow::Array> lon_array, lat_array, height_array, hmsl_array;
    ARROW_RETURN_NOT_OK(lon_builder.Finish(&lon_array));
    ARROW_RETURN_NOT_OK(lat_builder.Finish(&lat_array));
    ARROW_RETURN_NOT_OK(height_builder.Finish(&height_array));
    ARROW_RETURN_NOT_OK(hmsl_builder.Finish(&hmsl_array));

    std::shared_ptr<arrow::Array> lon_hp_array, lat_hp_array, height_hp_array, hmsl_hp_array;
    ARROW_RETURN_NOT_OK(lon_hp_builder.Finish(&lon_hp_array));
    ARROW_RETURN_NOT_OK(lat_hp_builder.Finish(&lat_hp_array));
    ARROW_RETURN_NOT_OK(height_hp_builder.Finish(&height_hp_array));
    ARROW_RETURN_NOT_OK(hmsl_hp_builder.Finish(&hmsl_hp_array));

    std::shared_ptr<arrow::Array> h_acc_array, v_acc_array;
    ARROW_RETURN_NOT_OK(h_acc_builder.Finish(&h_acc_array));
    ARROW_RETURN_NOT_OK(v_acc_builder.Finish(&v_acc_array));

    std::vector<std::shared_ptr<arrow::Array>> nav_hpposllh_children = {
      timestamp_array, header_struct_array, version_array,
      invalid_lon_array, invalid_lat_array, invalid_height_array, invalid_hmsl_array,
      invalid_lon_hp_array, invalid_lat_hp_array, invalid_height_hp_array, invalid_hmsl_hp_array,
      itow_array,
      lon_array, lat_array, height_array, hmsl_array,
      lon_hp_array, lat_hp_array, height_hp_array, hmsl_hp_array,
      h_acc_array, v_acc_array

    };

    *record_batch = arrow::RecordBatch::Make(schema_, msgs_size, nav_hpposllh_children);

    hpposllh_msgs.clear();

    return arrow::Status::OK();
  }

  template <typename MessageT>
  arrow::Status ArrowWriter<MessageT>::write_table_to_parquet(std::string cur_ts) {
    auto filepath = filepath_parquet(cur_ts);

    // auto parquet_writer = parquet::default_writer_properties();
    auto parquet_writer_builder = parquet::WriterProperties::Builder();
    parquet_writer_builder.version(parquet::ParquetVersion::PARQUET_2_LATEST);
    auto parquet_writer = parquet_writer_builder.build();

    // auto arrow_properties = parquet::default_arrow_writer_properties();
    auto arrow_writer_properties_builder = parquet::ArrowWriterProperties::Builder();
    arrow_writer_properties_builder.enable_compliant_nested_types();
    auto arrow_properties = arrow_writer_properties_builder.build();

    try {
      std::shared_ptr<arrow::io::FileOutputStream> outfile;
      PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(filepath));
      PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table_, get_memory_pool(), outfile, parquet_chunk_size_, parquet_writer, arrow_properties));
    } catch (const ::parquet::ParquetException& e) {
      return arrow::Status::IOError(e.what());
    }
    return arrow::Status::OK();
  }

  template <typename MessageT>
  arrow::Status ArrowWriter<MessageT>::build_table() {
    auto result = arrow::Table::FromRecordBatches(record_batches_);
    if (!result.ok()) {
      return result.status();
    }
    table_ = *std::move(result);

    record_batches_.clear();

    return arrow::Status::OK();
  }

  inline std::string cur_timestamp() {
    time_t rawtime;
    struct tm * timeinfo;
    char buffer[80];

    std::time (&rawtime);
    timeinfo = localtime(&rawtime);

    strftime(buffer, sizeof(buffer), "%Y%m%d%H%M%S",timeinfo);
    std::string str(buffer);

    return str;
  }

}

#endif // UBLOX_UBX_ARROW_SINK_NODE_ARROW_WRITER_HPP