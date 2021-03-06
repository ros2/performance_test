// Copyright 2017 Apex.AI, Inc.
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

/**
 * \brief Contains all includes and type abstraction for all means of communication.
 */

#ifndef EXPERIMENT_CONFIGURATION__TOPICS_HPP_
#define EXPERIMENT_CONFIGURATION__TOPICS_HPP_

#include <performance_test/for_each.hpp>

// ROS2 2 types:
#include <performance_test/msg/array1k.hpp>
#include <performance_test/msg/array4k.hpp>
#include <performance_test/msg/array16k.hpp>
#include <performance_test/msg/array32k.hpp>
#include <performance_test/msg/array60k.hpp>
#include <performance_test/msg/array64k.hpp>
#include <performance_test/msg/array256k.hpp>
#include <performance_test/msg/array1m.hpp>
#include <performance_test/msg/array2m.hpp>
#include <performance_test/msg/array4m.hpp>
#include <performance_test/msg/array8m.hpp>

#include <performance_test/msg/struct16.hpp>
#include <performance_test/msg/struct256.hpp>
#include <performance_test/msg/struct4k.hpp>
#include <performance_test/msg/struct32k.hpp>

#include <performance_test/msg/point_cloud512k.hpp>
#include <performance_test/msg/point_cloud1m.hpp>
#include <performance_test/msg/point_cloud2m.hpp>
#include <performance_test/msg/point_cloud4m.hpp>
#include <performance_test/msg/point_cloud8m.hpp>

#include <performance_test/msg/range.hpp>
#include <performance_test/msg/nav_sat_fix.hpp>

#include <performance_test/msg/radar_detection.hpp>
#include <performance_test/msg/radar_track.hpp>

#ifdef PERFORMANCE_TEST_ZERO_COPY_ENABLED
  #include <performance_test/msg/array1k_zero_copy.hpp>
  #include <performance_test/msg/array4k_zero_copy.hpp>
  #include <performance_test/msg/array16k_zero_copy.hpp>
  #include <performance_test/msg/array32k_zero_copy.hpp>
  #include <performance_test/msg/array60k_zero_copy.hpp>
  #include <performance_test/msg/array64k_zero_copy.hpp>
  #include <performance_test/msg/array256k_zero_copy.hpp>
  #include <performance_test/msg/array1m_zero_copy.hpp>
  #include <performance_test/msg/array2m_zero_copy.hpp>
  #include <performance_test/msg/array4m_zero_copy.hpp>
  #include <performance_test/msg/array8m_zero_copy.hpp>

  #include <performance_test/msg/struct16_zero_copy.hpp>
  #include <performance_test/msg/struct256_zero_copy.hpp>
  #include <performance_test/msg/struct4k_zero_copy.hpp>
  #include <performance_test/msg/struct32k_zero_copy.hpp>

  #include <performance_test/msg/point_cloud512k_zero_copy.hpp>
  #include <performance_test/msg/point_cloud1m_zero_copy.hpp>
  #include <performance_test/msg/point_cloud2m_zero_copy.hpp>
  #include <performance_test/msg/point_cloud4m_zero_copy.hpp>
  #include <performance_test/msg/point_cloud8m_zero_copy.hpp>

  #include <performance_test/msg/range_zero_copy.hpp>
  #include <performance_test/msg/nav_sat_fix_zero_copy.hpp>

  #include <performance_test/msg/radar_detection_zero_copy.hpp>
  #include <performance_test/msg/radar_track_zero_copy.hpp>
#endif

// FastRTPS Types:
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  #include <fast_rtps/Array1k_PubSubTypes.h>
  #include <fast_rtps/Array4k_PubSubTypes.h>
  #include <fast_rtps/Array16k_PubSubTypes.h>
  #include <fast_rtps/Array32k_PubSubTypes.h>
  #include <fast_rtps/Array60k_PubSubTypes.h>
  #include <fast_rtps/Array64k_PubSubTypes.h>
  #include <fast_rtps/Array256k_PubSubTypes.h>
  #include <fast_rtps/Array1m_PubSubTypes.h>
  #include <fast_rtps/Array2m_PubSubTypes.h>
  #include <fast_rtps/Array4m_PubSubTypes.h>
  #include <fast_rtps/Array8m_PubSubTypes.h>

  #include <fast_rtps/Struct16_PubSubTypes.h>
  #include <fast_rtps/Struct256_PubSubTypes.h>
  #include <fast_rtps/Struct4k_PubSubTypes.h>
  #include <fast_rtps/Struct32k_PubSubTypes.h>

  #include <fast_rtps/PointCloud512k_PubSubTypes.h>
  #include <fast_rtps/PointCloud1m_PubSubTypes.h>
  #include <fast_rtps/PointCloud2m_PubSubTypes.h>
  #include <fast_rtps/PointCloud4m_PubSubTypes.h>
  #include <fast_rtps/PointCloud8m_PubSubTypes.h>

  #include <fast_rtps/Range_PubSubTypes.h>
  #include <fast_rtps/NavSatFix_PubSubTypes.h>

  #include <fast_rtps/RadarDetection_PubSubTypes.h>
  #include <fast_rtps/RadarTrack_PubSubTypes.h>
#endif

// Connext DDS Micro Types:
#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  #include <performance_test/msg/dds_connext_micro_typefiles/Array1k_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array4k_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array16k_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array32k_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array60k_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array64k_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array256k_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array1m_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array2m_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array4m_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Array8m_Support.h>

  #include <performance_test/msg/dds_connext_micro_typefiles/Struct16_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Struct256_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Struct4k_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/Struct32k_Support.h>

  #include <performance_test/msg/dds_connext_micro_typefiles/PointCloud512k_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/PointCloud1m_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/PointCloud2m_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/PointCloud4m_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/PointCloud8m_Support.h>

  #include <performance_test/msg/dds_connext_micro_typefiles/Range_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/NavSatFix_Support.h>

  #include <performance_test/msg/dds_connext_micro_typefiles/RadarDetection_Support.h>
  #include <performance_test/msg/dds_connext_micro_typefiles/RadarTrack_Support.h>
#endif


#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
#include <connextdds/Array1k_Support.h>
#include <connextdds/Array4k_Support.h>
#include <connextdds/Array16k_Support.h>
#include <connextdds/Array32k_Support.h>
#include <connextdds/Array60k_Support.h>
#include <connextdds/Array64k_Support.h>
#include <connextdds/Array256k_Support.h>
#include <connextdds/Array1m_Support.h>
#include <connextdds/Array2m_Support.h>
#include <connextdds/Array4m_Support.h>
#include <connextdds/Array8m_Support.h>

#include <connextdds/Struct16_Support.h>
#include <connextdds/Struct256_Support.h>
#include <connextdds/Struct4k_Support.h>
#include <connextdds/Struct32k_Support.h>

#include <connextdds/PointCloud512k_Support.h>
#include <connextdds/PointCloud1m_Support.h>
#include <connextdds/PointCloud2m_Support.h>
#include <connextdds/PointCloud4m_Support.h>
#include <connextdds/PointCloud8m_Support.h>

#include <connextdds/Range_Support.h>
#include <connextdds/NavSatFix_Support.h>

#include <connextdds/RadarDetection_Support.h>
#include <connextdds/RadarTrack_Support.h>
#endif


// Cyclone DDS Types:
#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  #include <cyclonedds/Array1k_.h>
  #include <cyclonedds/Array4k_.h>
  #include <cyclonedds/Array16k_.h>
  #include <cyclonedds/Array32k_.h>
  #include <cyclonedds/Array60k_.h>
  #include <cyclonedds/Array64k_.h>
  #include <cyclonedds/Array256k_.h>
  #include <cyclonedds/Array1m_.h>
  #include <cyclonedds/Array2m_.h>
  #include <cyclonedds/Array4m_.h>
  #include <cyclonedds/Array8m_.h>

  #include <cyclonedds/Struct16_.h>
  #include <cyclonedds/Struct256_.h>
  #include <cyclonedds/Struct4k_.h>
  #include <cyclonedds/Struct32k_.h>

  #include <cyclonedds/PointCloud512k_.h>
  #include <cyclonedds/PointCloud1m_.h>
  #include <cyclonedds/PointCloud2m_.h>
  #include <cyclonedds/PointCloud4m_.h>
  #include <cyclonedds/PointCloud8m_.h>

  #include <cyclonedds/Range_.h>
  #include <cyclonedds/NavSatFix_.h>

  #include <cyclonedds/RadarDetection_.h>
  #include <cyclonedds/RadarTrack_.h>
#endif

// OpenDDS Types
#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  #include <dds/DdsDcpsTopicC.h>
  #include <opendds/Array1k_TypeSupportImpl.h>
  #include <opendds/Array4k_TypeSupportImpl.h>
  #include <opendds/Array16k_TypeSupportImpl.h>
  #include <opendds/Array32k_TypeSupportImpl.h>
  #include <opendds/Array60k_TypeSupportImpl.h>
  #include <opendds/Array64k_TypeSupportImpl.h>
  #include <opendds/Array256k_TypeSupportImpl.h>
  #include <opendds/Array1m_TypeSupportImpl.h>
  #include <opendds/Array2m_TypeSupportImpl.h>
  #include <opendds/Array4m_TypeSupportImpl.h>
  #include <opendds/Array8m_TypeSupportImpl.h>
  #include <opendds/Struct16_TypeSupportImpl.h>
  #include <opendds/Struct256_TypeSupportImpl.h>
  #include <opendds/Struct4k_TypeSupportImpl.h>
  #include <opendds/Struct32k_TypeSupportImpl.h>
  #include <opendds/PointCloud512k_TypeSupportImpl.h>
  #include <opendds/PointCloud1m_TypeSupportImpl.h>
  #include <opendds/PointCloud2m_TypeSupportImpl.h>
  #include <opendds/PointCloud4m_TypeSupportImpl.h>
  #include <opendds/PointCloud8m_TypeSupportImpl.h>
  #include <opendds/Range_TypeSupportImpl.h>
  #include <opendds/NavSatFix_TypeSupportImpl.h>
  #include <opendds/RadarDetection_TypeSupportImpl.h>
  #include <opendds/RadarTrack_TypeSupportImpl.h>
#endif

#include <algorithm>
#include <string>
#include <tuple>
#include <vector>

namespace performance_test
{

namespace topics
{

///  \cond
class Array1k
{
public:
  using RosType = performance_test::msg::Array1k;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array1k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array1k_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array1k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array1k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array1k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array1k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array1k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array1k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array1k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array1k_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Array1k");
  }
};

class Array4k
{
public:
  using RosType = performance_test::msg::Array4k;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array4k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array4k_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array4k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array4k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array4k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array4k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array4k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array4k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array4k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array4k_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("Array4k");
  }
};

class Array16k
{
public:
  using RosType = performance_test::msg::Array16k;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array16k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array16k_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array16k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array16k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array16k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array16k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array16k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array16k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array16k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array16k_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("Array16k");
  }
};

class Array32k
{
public:
  using RosType = performance_test::msg::Array32k;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array32k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array32k_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array32k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array32k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array32k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array32k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array32k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array32k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array32k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array32k_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Array32k");
  }
};

class Array60k
{
public:
  using RosType = performance_test::msg::Array60k;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array60k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array60k_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array60k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array60k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array60k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array60k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array60k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array60k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array60k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array60k_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Array60k");
  }
};

class Array64k
{
public:
  using RosType = performance_test::msg::Array64k;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array64k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array64k_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array64k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array64k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array64k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array64k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array64k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array64k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array64k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array64k_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Array64k");
  }
};

class Array256k
{
public:
  using RosType = performance_test::msg::Array256k;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array256k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array256k_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array256k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array256k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array256k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array256k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array256k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array256k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array256k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array256k_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Array256k");
  }
};

class Array1m
{
public:
  using RosType = performance_test::msg::Array1m;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array1m_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array1m_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array1m_;
#endif
#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array1m_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array1m__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array1m_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array1m_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array1m_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array1m_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array1m_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Array1m");
  }
};

class Array2m
{
public:
  using RosType = performance_test::msg::Array2m;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array2m_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array2m_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array2m_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array2m_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array2m__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array2m_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array2m_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array2m_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array2m_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array2m_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Array2m");
  }
};

class Array4m
{
public:
  using RosType = performance_test::msg::Array4m;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array4m_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array4m_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array4m_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array4m_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array4m__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array4m_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array4m_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array4m_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array4m_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array4m_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Array4m");
  }
};

class Array8m
{
public:
  using RosType = performance_test::msg::Array8m;

#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Array8m_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array8m_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Array8m_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Array8m_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Array8m__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Array8m_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Array8m_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Array8m_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Array8m_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Array8m_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Array8m");
  }
};

class Struct16
{
public:
  using RosType = performance_test::msg::Struct16;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Struct16_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Struct16_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Struct16_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Struct16_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Struct16__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Struct16_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Struct16_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Struct16_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Struct16_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Struct16_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("Struct16");
  }
};

class Struct256
{
public:
  using RosType = performance_test::msg::Struct256;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Struct256_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Struct256_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Struct256_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Struct256_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Struct256__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Struct256_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Struct256_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Struct256_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Struct256_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Struct256_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("Struct256");
  }
};

class Struct4k
{
public:
  using RosType = performance_test::msg::Struct4k;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Struct4k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Struct4k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Struct4k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Struct4k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Struct4k_;
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Struct4k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Struct4k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Struct4k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Struct4k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Struct4k_TypeSupportImpl();
  }
#endif
  static std::string msg_name()
  {
    return std::string("Struct4k");
  }
};

class Struct32k
{
public:
  using RosType = performance_test::msg::Struct32k;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Struct32k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Struct32k_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Struct32k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Struct32k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Struct32k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Struct32k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Struct32k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Struct32k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Struct32k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Struct32k_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("Struct32k");
  }
};

class PointCloud512k
{
public:
  using RosType = performance_test::msg::PointCloud512k;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::PointCloud512k_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud512k_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__PointCloud512k_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__PointCloud512k_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__PointCloud512k__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::PointCloud512k_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::PointCloud512k_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::PointCloud512k_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::PointCloud512k_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::PointCloud512k_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud512k");
  }
};

class PointCloud1m
{
public:
  using RosType = performance_test::msg::PointCloud1m;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::PointCloud1m_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud1m_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__PointCloud1m_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__PointCloud1m_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__PointCloud1m__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::PointCloud1m_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::PointCloud1m_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::PointCloud1m_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::PointCloud1m_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::PointCloud1m_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud1m");
  }
};

class PointCloud2m
{
public:
  using RosType = performance_test::msg::PointCloud2m;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::PointCloud2m_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud2m_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__PointCloud2m_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__PointCloud2m_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__PointCloud2m__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::PointCloud2m_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::PointCloud2m_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::PointCloud2m_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::PointCloud2m_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::PointCloud2m_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud2m");
  }
};

class PointCloud4m
{
public:
  using RosType = performance_test::msg::PointCloud4m;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::PointCloud4m_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud4m_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__PointCloud4m_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__PointCloud4m_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__PointCloud4m__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::PointCloud4m_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::PointCloud4m_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::PointCloud4m_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::PointCloud4m_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::PointCloud4m_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud4m");
  }
};

class PointCloud8m
{
public:
  using RosType = performance_test::msg::PointCloud8m;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::PointCloud8m_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud8m_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__PointCloud8m_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__PointCloud8m_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__PointCloud8m__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::PointCloud8m_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::PointCloud8m_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::PointCloud8m_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::PointCloud8m_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::PointCloud8m_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud8m");
  }
};

class Range
{
public:
  using RosType = performance_test::msg::Range;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::Range_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Range_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__Range_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__Range_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__Range__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::Range_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::Range_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::Range_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::Range_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::Range_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("Range");
  }
};

class NavSatFix
{
public:
  using RosType = performance_test::msg::NavSatFix;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::NavSatFix_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__NavSatFix_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__NavSatFix_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__NavSatFix_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__NavSatFix__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::NavSatFix_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::NavSatFix_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::NavSatFix_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::NavSatFix_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::NavSatFix_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("NavSatFix");
  }
};

class RadarDetection
{
public:
  using RosType = performance_test::msg::RadarDetection;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::RadarDetection_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__RadarDetection_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__RadarDetection_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__RadarDetection_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__RadarDetection__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::RadarDetection_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::RadarDetection_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::RadarDetection_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::RadarDetection_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::RadarDetection_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("RadarDetection");
  }
};

class RadarTrack
{
public:
  using RosType = performance_test::msg::RadarTrack;
#ifdef PERFORMANCE_TEST_FASTRTPS_ENABLED
  using EprosimaTopicType = performance_test_msgs::msg::dds_::RadarTrack_PubSubType;
  using EprosimaType = typename EprosimaTopicType::type;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__RadarTrack_;
#endif

#ifdef PERFORMANCE_TEST_CONNEXTDDS_ENABLED
  using ConnextDDSType = performance_test_msgs_msg_dds__RadarTrack_;
#endif

#ifdef PERFORMANCE_TEST_CYCLONEDDS_ENABLED
  using CycloneDDSType = performance_test_msgs_msg_dds__RadarTrack_;
  static const dds_topic_descriptor_t * CycloneDDSDesc()
  {
    return &performance_test_msgs_msg_dds__RadarTrack__desc;
  }
#endif

#ifdef PERFORMANCE_TEST_OPENDDS_ENABLED
  using OpenDDSTopicType = performance_test_msgs::msg::dds_::RadarTrack_;
  using OpenDDSDataWriterType = performance_test_msgs::msg::dds_::RadarTrack_DataWriter;
  using OpenDDSDataReaderType = performance_test_msgs::msg::dds_::RadarTrack_DataReader;
  using OpenDDSDataTypeSeq = performance_test_msgs::msg::dds_::RadarTrack_Seq;

  static DDS::TypeSupport_ptr get_type_support()
  {
    return new performance_test_msgs::msg::dds_::RadarTrack_TypeSupportImpl();
  }
#endif

  static std::string msg_name()
  {
    return std::string("RadarTrack");
  }
};
///  \endcond

#ifdef PERFORMANCE_TEST_ZERO_COPY_ENABLED
class Array1kZeroCopy
{
public:
  using RosType = performance_test::msg::Array1kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array1kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array1kZeroCopy");
  }
};

class Array4kZeroCopy
{
public:
  using RosType = performance_test::msg::Array4kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array4kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array4kZeroCopy");
  }
};

class Array16kZeroCopy
{
public:
  using RosType = performance_test::msg::Array16kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array16kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array16kZeroCopy");
  }
};

class Array32kZeroCopy
{
public:
  using RosType = performance_test::msg::Array32kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array32kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array32kZeroCopy");
  }
};

class Array60kZeroCopy
{
public:
  using RosType = performance_test::msg::Array60kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array60kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array60kZeroCopy");
  }
};

class Array64kZeroCopy
{
public:
  using RosType = performance_test::msg::Array64kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array64kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array64kZeroCopy");
  }
};

class Array256kZeroCopy
{
public:
  using RosType = performance_test::msg::Array256kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array256kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array256kZeroCopy");
  }
};

class Array1mZeroCopy
{
public:
  using RosType = performance_test::msg::Array1mZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array1mZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array1mZeroCopy");
  }
};

class Array2mZeroCopy
{
public:
  using RosType = performance_test::msg::Array2mZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array2mZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array2mZeroCopy");
  }
};

class Array4mZeroCopy
{
public:
  using RosType = performance_test::msg::Array4mZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array4mZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array4mZeroCopy");
  }
};

class Array8mZeroCopy
{
public:
  using RosType = performance_test::msg::Array8mZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Array8mZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Array8mZeroCopy");
  }
};

class Struct16ZeroCopy
{
public:
  using RosType = performance_test::msg::Struct16ZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Struct16ZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Struct16ZeroCopy");
  }
};

class Struct256ZeroCopy
{
public:
  using RosType = performance_test::msg::Struct256ZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Struct256ZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Struct256ZeroCopy");
  }
};

class Struct4kZeroCopy
{
public:
  using RosType = performance_test::msg::Struct4kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Struct4kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Struct4kZeroCopy");
  }
};

class Struct32kZeroCopy
{
public:
  using RosType = performance_test::msg::Struct32kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__Struct32kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("Struct32kZeroCopy");
  }
};

class PointCloud512kZeroCopy
{
public:
  using RosType = performance_test::msg::PointCloud512kZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud512kZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud512kZeroCopy");
  }
};

class PointCloud1mZeroCopy
{
public:
  using RosType = performance_test::msg::PointCloud1mZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud1mZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud1mZeroCopy");
  }
};

class PointCloud2mZeroCopy
{
public:
  using RosType = performance_test::msg::PointCloud2mZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud2mZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud2mZeroCopy");
  }
};

class PointCloud4mZeroCopy
{
public:
  using RosType = performance_test::msg::PointCloud4mZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud4mZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud4mZeroCopy");
  }
};

class PointCloud8mZeroCopy
{
public:
  using RosType = performance_test::msg::PointCloud8mZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__PointCloud8mZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("PointCloud8mZeroCopy");
  }
};

class RangeZeroCopy
{
public:
  using RosType = performance_test::msg::RangeZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__RangeZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("RangeZeroCopy");
  }
};

class NavSatFixZeroCopy
{
public:
  using RosType = performance_test::msg::NavSatFixZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__NavSatFixZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("NavSatFixZeroCopy");
  }
};

class RadarDetectionZeroCopy
{
public:
  using RosType = performance_test::msg::RadarDetectionZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__RadarDetectionZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("RadarDetectionZeroCopy");
  }
};

class RadarTrackZeroCopy
{
public:
  using RosType = performance_test::msg::RadarTrackZeroCopy;

#ifdef PERFORMANCE_TEST_CONNEXTDDSMICRO_ENABLED
  using ConnextDDSMicroType = performance_test_msg_dds__RadarTrackZeroCopy_;
#endif

  static std::string msg_name()
  {
    return std::string("RadarTrackZeroCopy");
  }
};
#endif

using TopicTypeList = std::tuple<Array1k, Array4k, Array16k, Array32k, Array60k,
    Array64k, Array256k, Array1m,
    Array2m, Array4m, Array8m,
    Struct16, Struct256, Struct4k, Struct32k,
    PointCloud512k, PointCloud1m, PointCloud2m, PointCloud4m, PointCloud8m,
    Range, NavSatFix, RadarDetection, RadarTrack
#ifdef PERFORMANCE_TEST_ZERO_COPY_ENABLED
    , Array1kZeroCopy, Array4kZeroCopy, Array16kZeroCopy, Array32kZeroCopy, Array60kZeroCopy,
    Array64k, Array256k,
    Array1mZeroCopy, Array2mZeroCopy, Array4mZeroCopy, Array8mZeroCopy,
    Struct16ZeroCopy, Struct256ZeroCopy, Struct4kZeroCopy, Struct32kZeroCopy,
    PointCloud512kZeroCopy, PointCloud1mZeroCopy, PointCloud2mZeroCopy, PointCloud4mZeroCopy,
    PointCloud8mZeroCopy, RangeZeroCopy, NavSatFixZeroCopy,
    RadarDetectionZeroCopy, RadarTrackZeroCopy
#endif
>;

/// Returns a vector of supported topic names.
inline std::vector<std::string> supported_msg_names()
{
  std::vector<std::string> result;
  performance_test::for_each(
    TopicTypeList(), [&result](const auto & topic) {
      using T = std::remove_cv_t<std::remove_reference_t<decltype(topic)>>;
      result.push_back(T::msg_name());
    });
  return result;
}

}  // namespace topics
}  // namespace performance_test

#endif  // EXPERIMENT_CONFIGURATION__TOPICS_HPP_
