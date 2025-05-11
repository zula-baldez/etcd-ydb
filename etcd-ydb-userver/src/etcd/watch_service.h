#pragma once

#include <fmt/format.h>
#include <api/etcdserverpb/rpc_client.usrv.pb.hpp>
#include <api/etcdserverpb/rpc_service.usrv.pb.hpp>
#include <nlohmann/json.hpp>
#include <userver/components/component.hpp>
#include <userver/components/component_list.hpp>
#include <userver/components/loggable_component_base.hpp>
#include <userver/ugrpc/client/client_factory_component.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/overloaded.hpp>
#include <userver/utils/underlying_value.hpp>
#include <userver/ydb/builder.hpp>
#include <userver/ydb/component.hpp>
#include <userver/ydb/table.hpp>
#include <userver/ydb/topic.hpp>
#include <userver/ydb/transaction.hpp>
#include "client_provider.hpp"

#include <userver/components/component.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/logging/log.hpp>
#include <userver/testsuite/testpoint.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/overloaded.hpp>
#include <userver/utils/underlying_value.hpp>
#include <userver/yaml_config/merge_schemas.hpp>
#include <userver/ydb/exceptions.hpp>
#include <userver/ydb/impl/cast.hpp>
#include <utility>
#include "check_request.h"
#include "revision_service.hpp"
#include "src/library/string_utils/base64/base64.h"
#include "watch_synchronization.h"
#include "watch_topic_reader.h"
namespace etcd_ydb_userver {

class WatchStreamData {
 public:
  WatchStreamData(
      std::unordered_map<int, ::etcdserverpb::WatchRequest>& watches,
      etcdserverpb::WatchBase::WatchReaderWriter& stream,
      WatchSynchronizationProvider& watch_sync_provider_)
      : watches(watches),
        stream(stream),
        watch_sync_provider_(watch_sync_provider_) {}

  std::unordered_map<int, ::etcdserverpb::WatchRequest> watches;
  std::unordered_set<int> updated;
  std::unordered_map<int, int> expected_revisions;

  // for synchronization between historical queries and processing messages
  userver::engine::SharedMutex processMessageMutex;
  int lastProcessedWatchId = 0;
  int lastProcessedBatchId = -1;

  void Send(etcdserverpb::WatchResponse* watchResponse) {
    auto lock = std::unique_lock<userver::engine::Mutex>(watchesMutex);
    if (watches.contains(watchResponse->watch_id())) {
      stream.Write(std::move(*watchResponse));
      updated.insert(watchResponse->watch_id());
    }
  }

  void CreateWatch(int watch_id, const etcdserverpb::WatchRequest& wa,
                   const GetRevisionResponse& revision) {
    auto lock = std::unique_lock<userver::engine::Mutex>(watchesMutex);
    watches[watch_id] = wa;
    int batch_id = watch_sync_provider_.batch_id;
    expected_revisions[watch_id] = revision.Revision + 1;
    lastProcessedBatchId = batch_id;
  }

  void DeleteWatcher(int watch_id) {
    auto lock = std::unique_lock<userver::engine::Mutex>(watchesMutex);
    if (watches.contains(watch_id)) {
      expected_revisions.erase(watch_id);
      watches.erase(watch_id);
    }
  }
  etcdserverpb::WatchBase::WatchReaderWriter& stream;
  WatchSynchronizationProvider& watch_sync_provider_;

 private:
  userver::engine::Mutex watchesMutex;
};

class WatchService final : public etcdserverpb::WatchBase::Component {
 public:
  static constexpr std::string_view kName = "watch-service";

  WatchService(const userver::components::ComponentConfig& config,
               const userver::components::ComponentContext& component_context);

  using WatchReaderWriter = USERVER_NAMESPACE::ugrpc::server::ReaderWriter<
      ::etcdserverpb::WatchRequest, ::etcdserverpb::WatchResponse>;
  using WatchResult = USERVER_NAMESPACE::ugrpc::server::StreamingResult<
      ::etcdserverpb::WatchResponse>;
  WatchResult Watch(CallContext& context, WatchReaderWriter& stream) override;

 private:
  WatchResult ProcessWatchRequest(CallContext& context,
                                  WatchStreamData& watchStreamData);
  void ProcessMessage(WatchStreamData& watchStreamData);
  void SendPings(WatchStreamData& watchStreamData);
  void SendHistoricalKeysChanges(
      WatchStreamData& watchStreamData,
      const etcdserverpb::WatchRequest& request, int64_t watchId);
  static bool validateMessage(const std::string& key,
                              const mvccpb::Event_EventType& event,
                              const int64_t mod_revision,
                              const int64_t expected_revision,
                              const etcdserverpb::WatchCreateRequest& request);
  EtcdTableClientProvider& table_client_provider_;
  RevisionService& revision_service_;
  WatchSynchronizationProvider& watch_sync_provider_;
  userver::engine::TaskWithResult<void> read_task_;
  userver::engine::TaskWithResult<void> timer_task_;

  userver::engine::TaskWithResult<void> request_processing_task_;
};
void AppendWatchService(userver::components::ComponentList& component_list);

}  // namespace etcd_ydb_userver