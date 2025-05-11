#pragma once

#include <userver/components/component.hpp>
#include <userver/components/component_list.hpp>
#include <userver/components/loggable_component_base.hpp>
#include <userver/ugrpc/client/client_factory_component.hpp>
#include <userver/ydb/component.hpp>

#include <userver/ydb/builder.hpp>
#include <userver/ydb/table.hpp>
#include <userver/ydb/topic.hpp>
#include <userver/ydb/transaction.hpp>
#include "batch_processor.h"
#include "check_request.h"
namespace etcd_ydb_userver {

class QueryOptimizer final : public userver::components::LoggableComponentBase {
 public:
  static constexpr std::string_view kName = "query-optimizer";

  QueryOptimizer(const userver::components::ComponentConfig& config,
                 const userver::components::ComponentContext& component_context)
      : userver::components::LoggableComponentBase(config, component_context),
        shared_data_(component_context.FindComponent<BatchProcessor>()) {}

  PutResult Put(userver::ugrpc::server::CallContext& context,
                ::etcdserverpb::PutRequest&& request) {
    return GetResult<::etcdserverpb::PutRequest, PutResult>(context,
                                                            std::move(request));
  }

  DeleteRangeResult DeleteRange(userver::ugrpc::server::CallContext& context,
                                ::etcdserverpb::DeleteRangeRequest&& request) {
    return GetResult<::etcdserverpb::DeleteRangeRequest, DeleteRangeResult>(
        context, std::move(request));
  }

  TxnResult Txn(userver::ugrpc::server::CallContext& context,
                ::etcdserverpb::TxnRequest&& request) {
    return GetResult<::etcdserverpb::TxnRequest, TxnResult>(context,
                                                            std::move(request));
  }

  [[nodiscard]] static inline std::pair<std::string, bool> Compare(
      const std::string& key, const std::string& rangeEnd) noexcept {
    if (rangeEnd.empty()) {
      return {"key == $key", false};
    } else if (rangeEnd == kEmptyKey || rangeEnd == kEmptyKey2 ||
               rangeEnd == kEmptyKey3) {
      return {"key >= $key", false};
    } else if (rangeEnd == GetPrefix(key)) {
      return {"StartsWith(key, $key)", false};
    } else {
      return {"key BETWEEN $key AND $range_end", true};
    }
  }

  [[nodiscard]] static inline bool CompareKeys(
      const std::string& startRange, const std::string& rangeEnd,
      const std::string& key) noexcept {
    if (rangeEnd.empty()) {
      return key == startRange;
    } else if (rangeEnd == kEmptyKey || rangeEnd == kEmptyKey2 ||
               rangeEnd == kEmptyKey3) {
      return key >= startRange;
    } else if (rangeEnd == GetPrefix(key)) {
      return key.starts_with(startRange);
    } else {
      return key >= startRange && key <= rangeEnd;
    }
  }
  static constexpr std::string kEmptyKey = {"\0", 1};
  // TODO seems like deserialization bug, sometimes \0 represented as " "
  static constexpr std::string kEmptyKey2 = " ";
  static constexpr std::string kEmptyKey3 = "\x18";

  [[nodiscard]] static inline std::string GetPrefix(std::string key) noexcept {
    while (!key.empty()) {
      if (static_cast<unsigned char>(key.back()) <
          std::numeric_limits<unsigned char>::max()) {
        char key_back = key.back();
        ++key_back;
        key.back() = key_back;
        return key;
      }
      key.pop_back();
    }
    return kEmptyKey;
  }

 private:
  BatchProcessor& shared_data_;
  userver::engine::TaskWithResult<void> task_;

  template <typename RequestT, typename ResultT>
  ResultT GetResult(userver::ugrpc::server::CallContext& context,
                    RequestT&& request) {
    if (const auto status = CheckRequest(request); !status.ok()) {
      return ResultT{status};
    }
    auto lock = std::unique_lock<userver::engine::Mutex>(shared_data_.mutex);
    bool res = shared_data_.cv_readers.Wait(
        lock, [&] { return !shared_data_.isProcessing; });
    if (!res) {
      return ResultT{grpc::Status::CANCELLED};
    }
    size_t index = shared_data_.waiting_queue_.size();
    shared_data_.waiting_queue_.emplace_back(std::forward<RequestT>(request));
    if (shared_data_.waiting_queue_.size() == 10) {
      shared_data_.cv_writers.NotifyOne();
    }
    auto vres = shared_data_.cv_readers.Wait(lock);
    if (vres != userver::engine::CvStatus::kNoTimeout) {
      return ResultT{grpc::Status::CANCELLED};
    }
    return get<ResultT>(shared_data_.responses[index]);
  }
};

void AppendQueryOptimizer(userver::components::ComponentList& component_list);
}  // namespace etcd_ydb_userver
