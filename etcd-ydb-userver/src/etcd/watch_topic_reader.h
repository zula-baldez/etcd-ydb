#pragma once

#include <fmt/format.h>

#include <userver/components/component.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/logging/log.hpp>
#include <userver/testsuite/testpoint.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/overloaded.hpp>
#include <userver/utils/underlying_value.hpp>
#include <userver/yaml_config/merge_schemas.hpp>
#include <userver/ydb/component.hpp>
#include <userver/ydb/exceptions.hpp>
#include <userver/ydb/impl/cast.hpp>
#include <userver/ydb/topic.hpp>
#include <utility>
#include "watch_synchronization.h"
namespace etcd_ydb_userver {
class TopicReader {
 public:
  TopicReader(userver::ydb::TopicClient& topic_client,
              const std::string& consumer_name,
              const std::vector<std::string>& topics,
              std::chrono::milliseconds restart_session_delay,
              etcd_ydb_userver::WatchSynchronizationProvider&
                  watch_synchronization_provider)
      : topic_client_{topic_client},
        read_session_settings_{
            ConstructReadSessionSettings(consumer_name, topics)},
        restart_session_delay_{restart_session_delay},
        watch_synchronization_provider{watch_synchronization_provider} {}

  void Run();

 private:
  static NYdb::NTopic::TReadSessionSettings ConstructReadSessionSettings(
      const std::string& consumer_name, const std::vector<std::string>& topics);

  userver::ydb::TopicClient& topic_client_;
  const NYdb::NTopic::TReadSessionSettings read_session_settings_;
  const std::chrono::milliseconds restart_session_delay_;
  etcd_ydb_userver::WatchSynchronizationProvider&
      watch_synchronization_provider;
};
}  // namespace etcd_ydb_userver
