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

#ifndef COMMUNICATION_ABSTRACTIONS__FAST_RTPS_COMMUNICATOR_HPP_
#define COMMUNICATION_ABSTRACTIONS__FAST_RTPS_COMMUNICATOR_HPP_

#include <atomic>
#include <memory>

#include <fastdds/dds/core/policy/QosPolicies.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include "communicator.hpp"
#include "resource_manager.hpp"
#include "../experiment_configuration/topics.hpp"
#include "../experiment_configuration/qos_abstraction.hpp"

namespace performance_test
{

/// Translates abstract QOS settings to specific QOS settings for FastRTPS.
class FastRTPSQOSAdapter
{
public:
  /**
   * \brief Constructs the QOS adapter.
   * \param qos The abstract QOS settings the adapter should use to derive the implementation specific QOS settings.
   */
  explicit FastRTPSQOSAdapter(const QOSAbstraction qos)
  : m_qos(qos)
  {}

  /// Returns derived FastRTPS reliability setting from the stored abstract QOS setting.
  inline eprosima::fastdds::dds::ReliabilityQosPolicyKind reliability() const
  {
    if (m_qos.reliability == QOSAbstraction::Reliability::BEST_EFFORT) {
      return eprosima::fastdds::dds::ReliabilityQosPolicyKind::BEST_EFFORT_RELIABILITY_QOS;
    } else if (m_qos.reliability == QOSAbstraction::Reliability::RELIABLE) {
      return eprosima::fastdds::dds::ReliabilityQosPolicyKind::RELIABLE_RELIABILITY_QOS;
    } else {
      throw std::runtime_error("Unsupported QOS!");
    }
  }
  /// Returns derived FastRTPS durability setting from the stored abstract QOS setting.
  inline eprosima::fastdds::dds::DurabilityQosPolicyKind durability() const
  {
    if (m_qos.durability == QOSAbstraction::Durability::VOLATILE) {
      return eprosima::fastdds::dds::DurabilityQosPolicyKind::VOLATILE_DURABILITY_QOS;
    } else if (m_qos.durability == QOSAbstraction::Durability::TRANSIENT_LOCAL) {
      return eprosima::fastdds::dds::DurabilityQosPolicyKind::TRANSIENT_LOCAL_DURABILITY_QOS;
    } else {
      throw std::runtime_error("Unsupported QOS!");
    }
  }
  /// Returns derived FastRTPS history policy setting from the stored abstract QOS setting.
  inline eprosima::fastdds::dds::HistoryQosPolicyKind history_kind() const
  {
    if (m_qos.history_kind == QOSAbstraction::HistoryKind::KEEP_ALL) {
      return eprosima::fastdds::dds::HistoryQosPolicyKind::KEEP_ALL_HISTORY_QOS;
    } else if (m_qos.history_kind == QOSAbstraction::HistoryKind::KEEP_LAST) {
      return eprosima::fastdds::dds::HistoryQosPolicyKind::KEEP_LAST_HISTORY_QOS;
    } else {
      throw std::runtime_error("Unsupported QOS!");
    }
  }
  /// Returns derived FastRTPS history depth setting from the stored abstract QOS setting.
  int32_t history_depth() const
  {
    if (m_qos.history_kind == QOSAbstraction::HistoryKind::KEEP_LAST) {
      return static_cast<int32_t>(m_qos.history_depth);
    } else if (m_qos.history_kind == QOSAbstraction::HistoryKind::KEEP_ALL) {
      // Keep all, keeps all. No depth required, but setting to dummy value.
      return 1;
    } else {
      throw std::runtime_error("Unsupported QOS!");
    }
  }
  /// Returns the number of samples to be allocated on the history
  int32_t resource_limits_samples() const
  {
    return static_cast<int32_t>(m_qos.history_depth);
  }
  /// Returns the publish mode policy from the stored abstract QOS setting.
  inline eprosima::fastdds::dds::PublishModeQosPolicyKind publish_mode() const
  {
    if (m_qos.sync_pubsub) {
      return eprosima::fastdds::dds::PublishModeQosPolicyKind::SYNCHRONOUS_PUBLISH_MODE;
    } else {
      return eprosima::fastdds::dds::PublishModeQosPolicyKind::ASYNCHRONOUS_PUBLISH_MODE;
    }
  }

private:
  const QOSAbstraction m_qos;
};

/**
 * \brief Communication plugin for FastRTPS.
 * \tparam Topic The topic type to use.
 *
 * The code in there is derived from
 * https://github.com/eProsima/Fast-RTPS/tree/master/examples/C%2B%2B/HelloWorldExample.
 */
template<class Topic>
class FastRTPSCommunicator : public Communicator
{
public:
  /// The topic type to use.
  using TopicType = typename Topic::EprosimaTopicType;
  /// The data type to publish and subscribe to.
  using DataType = typename Topic::EprosimaType;

  /// Constructor which takes a reference \param lock to the lock to use.
  explicit FastRTPSCommunicator(SpinLock & lock)
  : Communicator(lock),
    m_topic(nullptr),
    m_writer(nullptr),
    m_reader(nullptr),
    m_type(new TopicType())
  {
    m_participant = ResourceManager::get().fastrtps_participant();
    if (m_ec.use_single_participant()) {
      if (!s_type_registered) {
        m_type.register_type(m_participant->participant);
        s_type_registered = true;
      }
    } else {
      m_type.register_type(m_participant->participant);
    }
  }

  void ensure_topic_created()
  {
    if (!m_topic) {
      auto topic_name = Topic::topic_name() + m_ec.pub_topic_postfix();
      m_topic = m_participant->participant->create_topic(
        topic_name,
        m_type.get_type_name(),
        eprosima::fastdds::dds::TOPIC_QOS_DEFAULT);
      if (!m_topic) {
        auto topic_desc = m_participant->participant->lookup_topicdescription(topic_name);
        m_topic = dynamic_cast<eprosima::fastdds::dds::Topic *>(topic_desc);
        if (!m_topic) {
          throw std::runtime_error("Failed to create topic or lookup existing topic");
        }
      }
    }
  }

  /**
   * \brief Publishes the provided data.
   *
   *  The first time this function is called it also creates the data writer.
   *  Further it updates all internal counters while running.
   * \param data The data to publish.
   * \param time The time to fill into the data field.
   */
  void publish(DataType & data, const std::chrono::nanoseconds time)
  {
    namespace ef_dds = eprosima::fastdds::dds;

    if (!m_writer) {
      const FastRTPSQOSAdapter qos(m_ec.qos());

      ensure_topic_created();

      ef_dds::DataWriterQos wqos;
      wqos.history().kind = qos.history_kind();
      wqos.history().depth = qos.history_depth();
      wqos.resource_limits().max_samples = qos.resource_limits_samples();
      wqos.resource_limits().allocated_samples = qos.resource_limits_samples();
      wqos.reliability().kind = qos.reliability();
      wqos.durability().kind = qos.durability();
      wqos.publish_mode().kind = qos.publish_mode();
      m_writer = m_participant->publisher->create_datawriter(m_topic, wqos);
    }
    lock();
    data.time_(time.count());
    data.id_(next_sample_id());
    increment_sent();  // We increment before publishing so we don't have to lock twice.
    unlock();
    m_writer->write(static_cast<void *>(&data));
  }
  /**
   * \brief Reads received data from DDS.
   *
   * In detail this function:
   * * Reads samples from DDS.
   * * Verifies that the data arrived in the right order, chronologically and also consistent with the publishing order.
   * * Counts received and lost samples.
   * * Calculates the latency of the samples received and updates the statistics accordingly.
   */
  void update_subscription()
  {
    namespace ef_dds = eprosima::fastdds::dds;

    if (!m_reader) {
      const FastRTPSQOSAdapter qos(m_ec.qos());

      ensure_topic_created();

      ef_dds::DataReaderQos rqos;
      rqos.history().kind = qos.history_kind();
      rqos.history().depth = qos.history_depth();
      rqos.resource_limits().max_samples = qos.resource_limits_samples();
      rqos.resource_limits().allocated_samples = qos.resource_limits_samples();
      rqos.reliability().kind = qos.reliability();
      rqos.durability().kind = qos.durability();
      m_reader = m_participant->subscriber->create_datareader(m_topic, rqos);
    }

    m_reader->wait_for_unread_message({3, 0});
    lock();
    while (ef_dds::RETCODE_OK == m_reader->take_next_sample(&m_data, &m_info)) {
      if ((m_info.instance_state == ef_dds::ALIVE_INSTANCE_STATE) && m_info.valid_data) {
        if (m_prev_timestamp >= m_data.time_()) {
          throw std::runtime_error(
                  "Data consistency violated. Received sample with not strictly "
                  "older timestamp. Time diff: " + std::to_string(
                    m_data.time_() - m_prev_timestamp) + " Data Time: " +
                  std::to_string(m_data.time_())
          );
        }


        if (m_ec.roundtrip_mode() == ExperimentConfiguration::RoundTripMode::RELAY) {
          unlock();
          publish(m_data, std::chrono::nanoseconds(m_data.time_()));
          lock();
        } else {
          m_prev_timestamp = m_data.time_();
          update_lost_samples_counter(m_data.id_());
          add_latency_to_statistics(m_data.time_());
          increment_received();
        }
      }
    }
    unlock();
  }

  /// Returns the data received in bytes.
  std::size_t data_received()
  {
    return num_received_samples() * sizeof(DataType);
  }

private:
  std::shared_ptr<ResourceManager::FastDDSParticipant> m_participant;
  eprosima::fastdds::dds::Topic * m_topic;
  eprosima::fastdds::dds::DataWriter * m_writer;
  eprosima::fastdds::dds::DataReader * m_reader;

  static bool s_type_registered;
  eprosima::fastdds::dds::SampleInfo m_info;

  eprosima::fastdds::dds::TypeSupport m_type;
  DataType m_data;
};

template<class Topic>
bool FastRTPSCommunicator<Topic>::s_type_registered = false;

}  // namespace performance_test

#endif  // COMMUNICATION_ABSTRACTIONS__FAST_RTPS_COMMUNICATOR_HPP_
