
#pragma once

#include <userver/components/component.hpp>
#include <userver/components/component_list.hpp>
#include <userver/components/loggable_component_base.hpp>
#include <userver/ugrpc/client/client_factory_component.hpp>
#include <userver/ydb/component.hpp>

#include <userver/ydb/table.hpp>
#include <userver/ydb/topic.hpp>
#include <userver/ydb/transaction.hpp>

#include <userver/ydb/builder.hpp>

namespace etcd_ydb_userver {


enum struct WatcherEventType { kTimerInterrupt, kDataReceived, kError };


// sync between topic consumer and watch stream handlers
class WatchSynchronizationProvider final
    : public userver::components::LoggableComponentBase {
 public:
  static constexpr std::string_view kName = "watch-synchronization";

  WatchSynchronizationProvider(
      const userver::components::ComponentConfig& config,
      const userver::components::ComponentContext& component_context)
      : userver::components::LoggableComponentBase(config, component_context) {}

  int AddNewWatcher(int64_t globalId);
  void DeleteWatcher(int64_t globalId);
  void EndProcessing(int64_t globalId);

  void NotifyAllWatchers(
      std::vector<
          NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>&&
          messages);
  void NotifyAllWatchersByTimer();

  int64_t WatcherWaitForEvent(int64_t batchId);
  int64_t GetGlobalWatcherId();

  std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> message_to_process;
  int64_t max_id_ = 1;
  std::unordered_set<int64_t> processing_watchers;
  int64_t batch_id = 1;
  std::unordered_set<int64_t> watchers;

 private:
  userver::engine::Mutex mutex;
  userver::engine::ConditionVariable cv_writers;
  userver::engine::ConditionVariable cv_readers;
};

void AppendWatchSynchronizationProvider(
    userver::components::ComponentList& component_list);
}  // namespace etcd_ydb_userver
template <>
inline constexpr bool userver::components::kHasValidate<
    etcd_ydb_userver::WatchSynchronizationProvider> = true;
