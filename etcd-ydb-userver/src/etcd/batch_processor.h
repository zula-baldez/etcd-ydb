#pragma once

#include <api/etcdserverpb/rpc_service.usrv.pb.hpp>
#include <userver/components/component.hpp>
#include <userver/components/component_list.hpp>
#include <userver/components/loggable_component_base.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/ugrpc/client/client_factory_component.hpp>
#include <userver/utils/async.hpp>
#include <userver/ydb/component.hpp>

#include <nlohmann/json.hpp>
#include "client_provider.hpp"
#include "model.hpp"
#include "revision_service.hpp"
#include "src/library/string_utils/base64/base64.h"

namespace etcd_ydb_userver {
using Variant =
    std::variant<::etcdserverpb::TxnRequest, ::etcdserverpb::DeleteRangeRequest,
                 ::etcdserverpb::PutRequest, ::etcdserverpb::RangeRequest>;

using ResponseVariant = std::variant<etcd_ydb_userver::TxnResult,
                                     DeleteRangeResult, PutResult, RangeResult>;

class BatchProcessor final : public userver::components::LoggableComponentBase {
 public:
  static constexpr std::string_view kName = "batch-processor";

  BatchProcessor(const userver::components::ComponentConfig& config,
                 const userver::components::ComponentContext& component_context)
      : userver::components::LoggableComponentBase(config, component_context),
        client_provider_(
            component_context.FindComponent<EtcdTableClientProvider>()),
        revision_service_(component_context.FindComponent<RevisionService>()) {
    auto delay = config["write-delay"].As<int>(50);
    task_ = userver::utils::CriticalAsync("write-query-task",
                                          [&, delay] { Run(delay); });
  }

  std::vector<std::variant<::etcdserverpb::TxnRequest,
                           ::etcdserverpb::DeleteRangeRequest,
                           ::etcdserverpb::PutRequest>>
      waiting_queue_;
  std::vector<std::variant<::etcdserverpb::TxnRequest,
                           ::etcdserverpb::DeleteRangeRequest,
                           ::etcdserverpb::PutRequest>>
      processing_queue_;
  std::vector<
      std::variant<TxnResult, DeleteRangeResult, PutResult, RangeResult>>
      responses;

  bool isProcessing = false;

  userver::engine::Mutex mutex = userver::engine::Mutex();
  userver::engine::ConditionVariable cv_writers;
  userver::engine::ConditionVariable cv_readers;

  void Run(int64_t delay);

  static userver::yaml_config::Schema GetStaticConfigSchema() {
    return userver::yaml_config::MergeSchemas<
        userver::components::LoggableComponentBase>(R"(
type: object
description: config
additionalProperties: false
properties:
    write-delay:
        type: integer
        description: write delay
)");
  }

 private:
  void ProcessPrevKvs(
      std::unordered_map<std::string, KeyState>& prevKvs, const Variant& req,
      std::optional<std::reference_wrapper<userver::engine::Mutex>>
          keyBucketMutex,
      NYdb::NTable::TSession& session);
  void ProcessPrevKvs(
      std::unordered_map<std::string, KeyState>& prevKvs,
      const ::etcdserverpb::PutRequest& req,
      std::optional<std::reference_wrapper<userver::engine::Mutex>>
          keyBucketMutex,
      NYdb::NTable::TSession& session);

  void ProcessPrevKvs(
      std::unordered_map<std::string, KeyState>& prevKvs,
      const ::etcdserverpb::DeleteRangeRequest& req,
      std::optional<std::reference_wrapper<userver::engine::Mutex>>
          keyBucketMutex,
      NYdb::NTable::TSession& session);

  void ProcessPrevKvs(
      std::unordered_map<std::string, KeyState>& prevKvs,
      const ::etcdserverpb::RangeRequest& req,
      std::optional<std::reference_wrapper<userver::engine::Mutex>>
          keyBucketMutex,
      NYdb::NTable::TSession& session);

  void ProcessPrevKvs(
      std::unordered_map<std::string, KeyState>& prevKvs,
      const ::etcdserverpb::TxnRequest& req,
      std::optional<std::reference_wrapper<userver::engine::Mutex>>
          keyBucketMutex,
      NYdb::NTable::TSession& session);

  ResponseVariant ProcessRequest(
      std::unordered_map<std::string, KeyState>& requestPrevKvs,
      const Variant& req,
      std::unordered_map<std::string, KeyState>& batchChangedKeys,
      std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
          history,
      std::vector<KeyState>& deleted, GetRevisionResponse& revision);

  ResponseVariant ProcessRequest(
      std::unordered_map<std::string, KeyState>& requestPrevKvs,
      const ::etcdserverpb::PutRequest& req,
      std::unordered_map<std::string, KeyState>& batchChangedKeys,
      std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
          history,
      std::vector<KeyState>& deleted, GetRevisionResponse& revision);

  ResponseVariant ProcessRequest(
      std::unordered_map<std::string, KeyState>& requestPrevKvs,
      const ::etcdserverpb::DeleteRangeRequest& req,
      std::unordered_map<std::string, KeyState>& batchChangedKeys,
      std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
          history,
      std::vector<KeyState>& deleted, GetRevisionResponse& revision);

  ResponseVariant ProcessRequest(
      std::unordered_map<std::string, KeyState>& requestPrevKvs,
      const ::etcdserverpb::RangeRequest& req,
      std::unordered_map<std::string, KeyState>& batchChangedKeys,
      std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
          history,
      std::vector<KeyState>& deleted, GetRevisionResponse& revision);

  ResponseVariant ProcessRequest(
      std::unordered_map<std::string, KeyState>& requestPrevKvs,
      const ::etcdserverpb::TxnRequest& req,
      std::unordered_map<std::string, KeyState>& batchChangedKeys,
      std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
          history,
      std::vector<KeyState>& deleted, GetRevisionResponse& revision);

  void ProcessPutKv(std::unordered_map<std::string, KeyState>& changedKeys,
                    NYdb::NTable::TTransaction& transaction);
  void ProcessDeleteKv(std::unordered_map<std::string, KeyState>& changedKeys,
                       NYdb::NTable::TTransaction& transaction);
  void ProcessPutPrevKv(std::vector<KeyState>& deleted,
                        NYdb::NTable::TTransaction& transaction);
  void UpdateRevision(GetRevisionResponse& revision,
                      NYdb::NTable::TTransaction& transaction);
  void ProcessSendWatch(
      std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
          history,
      NYdb::NTable::TTransaction& transaction);

  etcd_ydb_userver::EtcdTableClientProvider& client_provider_;
  etcd_ydb_userver::RevisionService& revision_service_;
  userver::engine::TaskWithResult<void> task_;
  std::optional<NYdb::NTopic::TContinuationToken> token;
};

void AppendBatchProcessor(userver::components::ComponentList& component_list);

}  // namespace etcd_ydb_userver