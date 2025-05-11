#include "batch_processor.h"
namespace etcd_ydb_userver {

void BatchProcessor::ProcessPrevKvs(
    std::unordered_map<std::string, KeyState>& prevKvs,
    const ::etcdserverpb::TxnRequest& request,
    std::optional<std::reference_wrapper<userver::engine::Mutex>>
        keyBucketMutex,
    NYdb::NTable::TSession& session) {
  if (!request.compare().empty()) {
    std::string statement = R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $keys AS List<Struct<key: String>>;

SELECT *
    FROM kv WHERE key IN (
SELECT key
    FROM AS_TABLE($keys)
);
)";

    NYdb::TParamsBuilder params;

    auto& targetParam = params.AddParam("$keys");

    targetParam.BeginList();

    for (auto& TxnCmpRequest : request.compare()) {
      targetParam.AddListItem()
          .BeginStruct()
          .AddMember("key")
          .String(TxnCmpRequest.key())
          .EndStruct();
    }

    targetParam.EndList();
    targetParam.Build();

    auto response =
        session
            .ExecuteDataQuery(statement,
                              NYdb::NTable::TTxControl::BeginTx(
                                  NYdb::NTable::TTxSettings::OnlineRO())
                                  .CommitTx(),
                              params.Build())
            .GetValueSync();

    auto cursor = response.GetResultSetParser(0);
    while (cursor.TryNextRow()) {
      auto key = cursor.ColumnParser("key").GetString();

      auto keyState = KeyState(
          key, cursor.ColumnParser("value").GetString(),
          cursor.ColumnParser("mod_revision").GetInt64(),
          cursor.ColumnParser("create_revision").GetInt64(),
          std::optional<int64_t>(), cursor.ColumnParser("version").GetInt64());

      if (keyBucketMutex.has_value()) {
        std::lock_guard<userver::engine::Mutex> lock(keyBucketMutex->get());
        prevKvs[key] = keyState;
      } else {
        prevKvs[key] = keyState;
      }
    }
  }
  std::vector<userver::engine::TaskWithResult<void>> tasks;
  userver::engine::Mutex bucketMutex;
  if (!keyBucketMutex.has_value()) {
    keyBucketMutex = {bucketMutex};
  }
  for (auto& op : request.success()) {
    if (op.has_request_txn()) {
      tasks.emplace_back(userver::utils::CriticalAsync(
          "txn-prev-process", [op, &prevKvs, this, keyBucketMutex, &session] {
            ProcessPrevKvs(prevKvs, Variant{op.request_txn()}, keyBucketMutex,
                           session);
          }));
    }
    if (op.has_request_range()) {
      tasks.emplace_back(userver::utils::CriticalAsync(
          "txn-prev-process", [op, &prevKvs, this, keyBucketMutex, &session] {
            ProcessPrevKvs(prevKvs, Variant{op.request_range()}, keyBucketMutex,
                           session);
          }));
    }
    if (op.has_request_put()) {
      tasks.emplace_back(userver::utils::CriticalAsync(
          "txn-prev-process", [op, &prevKvs, this, keyBucketMutex, &session] {
            ProcessPrevKvs(prevKvs, Variant{op.request_put()}, keyBucketMutex,
                           session);
          }));
    }
    if (op.has_request_delete_range()) {
      tasks.emplace_back(userver::utils::CriticalAsync(
          "txn-prev-process", [op, &prevKvs, this, keyBucketMutex, &session] {
            ProcessPrevKvs(prevKvs, Variant{op.request_delete_range()},
                           keyBucketMutex, session);
          }));
    }
  }
  for (auto& op : request.failure()) {
    if (op.has_request_txn()) {
      tasks.emplace_back(userver::utils::CriticalAsync(
          "txn-prev-process", [op, &prevKvs, this, keyBucketMutex, &session] {
            ProcessPrevKvs(prevKvs, Variant{op.request_txn()}, keyBucketMutex,
                           session);
          }));
    }
    if (op.has_request_range()) {
      tasks.emplace_back(userver::utils::CriticalAsync(
          "txn-prev-process", [op, &prevKvs, this, keyBucketMutex, &session] {
            ProcessPrevKvs(prevKvs, Variant{op.request_range()}, keyBucketMutex,
                           session);
          }));
    }
    if (op.has_request_put()) {
      tasks.emplace_back(userver::utils::CriticalAsync(
          "txn-prev-process", [op, &prevKvs, this, keyBucketMutex, &session] {
            ProcessPrevKvs(prevKvs, Variant{op.request_put()}, keyBucketMutex,
                           session);
          }));
    }
    if (op.has_request_delete_range()) {
      tasks.emplace_back(userver::utils::CriticalAsync(
          "txn-prev-process", [op, &prevKvs, this, keyBucketMutex, &session] {
            ProcessPrevKvs(prevKvs, Variant{op.request_delete_range()},
                           keyBucketMutex, session);
          }));
    }
  }
  for (auto& task : tasks) {
    task.Wait();
  }
}

void BatchProcessor::ProcessPrevKvs(
    std::unordered_map<std::string, KeyState>& prevKvs,
    const ::etcdserverpb::RangeRequest& request,
    std::optional<std::reference_wrapper<userver::engine::Mutex>>
        keyBucketMutex,
    NYdb::NTable::TSession& session) {
  const auto [compareCond, useRangeEnd] =
      KvUtil::Compare(request.key(), request.range_end());

  std::ostringstream query;
  query << R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $revision AS Int64;
DECLARE $key AS String;
  )";
  if (useRangeEnd) {
    query << R"(
            DECLARE $range_end AS String;
    )";
  }
  query << R"(

    SELECT *
        FROM kv
        WHERE )"
        << compareCond << ";";

  NYdb::TParamsBuilder builder = client_provider_.getTableClient()
                                     .GetNativeTableClient()
                                     .GetParamsBuilder();
  builder.AddParam("$key").String(request.key()).Build();
  if (useRangeEnd) {
    builder.AddParam("$range_end").String(request.range_end());
  }

  auto response =
      session
          .ExecuteDataQuery(query.str(),
                            NYdb::NTable::TTxControl::BeginTx(
                                NYdb::NTable::TTxSettings::OnlineRO())
                                .CommitTx(),
                            builder.Build())
          .GetValueSync();

  auto cursor = response.GetResultSetParser(0);

  while (cursor.TryNextRow()) {
    auto key = cursor.ColumnParser("key").GetString();

    auto keyState = KeyState(key, cursor.ColumnParser("value").GetString(),
                             cursor.ColumnParser("mod_revision").GetInt64(),
                             cursor.ColumnParser("create_revision").GetInt64(),
                             std::optional<int64_t>(),
                             cursor.ColumnParser("version").GetInt64());
    if (keyBucketMutex.has_value()) {
      std::lock_guard<userver::engine::Mutex> lock(keyBucketMutex->get());
      prevKvs[key] = keyState;
    } else {
      prevKvs[key] = keyState;
    }
  }
}

ResponseVariant BatchProcessor::ProcessRequest(
    std::unordered_map<std::string, KeyState>& prevKvs,
    const ::etcdserverpb::TxnRequest& request,
    std::unordered_map<std::string, KeyState>& changedKeys,
    std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
        history,
    std::vector<KeyState>& deleted, GetRevisionResponse& revision) {
  bool success = true;

  for (auto& compare : request.compare()) {
    KeyState prevValue;
    if (changedKeys.contains(compare.key())) {
      prevValue = changedKeys[compare.key()];
    } else if (prevKvs.contains(compare.key())) {
      prevValue = prevKvs[compare.key()];
    }
    if (!prevValue.isInitialized()) {
      if (compare.target_union_case() ==
          etcdserverpb::Compare::TARGET_UNION_NOT_SET) {
        continue;
      } else {
        success = true;
        break;
      }
    }

    auto cmp = [&compare](auto&& a, auto&& b) {
      switch (compare.result()) {
        case etcdserverpb::Compare_CompareResult_EQUAL:
          return a == b;
        case etcdserverpb::Compare_CompareResult_GREATER:
          LOG_DEBUG() << a;
          LOG_DEBUG() << b;
          return a > b;
        case etcdserverpb::Compare_CompareResult_LESS:
          return a < b;
        case etcdserverpb::Compare_CompareResult_NOT_EQUAL:
          return a != b;
        default:
          throw std::runtime_error("Unexpected compare type");
      }
    };

    switch (compare.target_union_case()) {
      case etcdserverpb::Compare::kVersion: {
        success = cmp(prevValue.version, compare.version());
        break;
      }
      case etcdserverpb::Compare::kCreateRevision: {
        success = cmp(prevValue.create_revision, compare.create_revision());
        break;
      }
      case etcdserverpb::Compare::kModRevision: {
        success = cmp(prevValue.mod_revision, compare.mod_revision());
        break;
      }
      case etcdserverpb::Compare::kValue: {
        success = cmp(prevValue.value, compare.value());
        break;
      }
      default:
        break;
    }
  }

  google::protobuf::Arena arena;
  auto response =
      google::protobuf::Arena::CreateMessage<etcdserverpb::TxnResponse>(&arena);
  auto requests = success ? request.success() : request.failure();
  response->set_succeeded(success);
  for (auto& req : requests) {
    switch (req.request_case()) {
      case etcdserverpb::RequestOp::kRequestRange: {
        auto rangeResponse =
            ProcessRequest(prevKvs, req.request_range(), changedKeys, history,
                           deleted, revision);
        auto* rangeReponse =
            response->mutable_responses()->Add()->mutable_response_range();
        *rangeReponse =
            get<userver::ugrpc::server::Result<etcdserverpb::RangeResponse>>(
                rangeResponse)
                .GetResponse();
        break;
      }
      case etcdserverpb::RequestOp::kRequestPut: {
        auto putResponse =
            ProcessRequest(prevKvs, req.request_put(), changedKeys, history,
                           deleted, revision);
        auto* op = response->mutable_responses()->Add()->mutable_response_put();
        *op = get<userver::ugrpc::server::Result<etcdserverpb::PutResponse>>(
                  putResponse)
                  .GetResponse();
        break;
      }
      case etcdserverpb::RequestOp::kRequestDeleteRange: {
        auto deleteResponse =
            ProcessRequest(prevKvs, req.request_delete_range(), changedKeys,
                           history, deleted, revision);
        auto* op = response->mutable_responses()
                       ->Add()
                       ->mutable_response_delete_range();
        *op = get<userver::ugrpc::server::Result<
            etcdserverpb::DeleteRangeResponse>>(deleteResponse)
                  .GetResponse();
        break;
      }
      case etcdserverpb::RequestOp::kRequestTxn: {
        auto txnResponse =
            ProcessRequest(prevKvs, req.request_txn(), changedKeys, history,
                           deleted, revision);
        auto* op = response->mutable_responses()->Add()->mutable_response_txn();
        auto& txnEmbedded =
            get<userver::ugrpc::server::Result<etcdserverpb::TxnResponse>>(
                txnResponse);
        auto embeddedResponse = std::move(txnEmbedded).ExtractResponse();
        embeddedResponse.mutable_header()->set_revision(0);
        txnEmbedded = TxnResult(std::move(embeddedResponse));
        *op = txnEmbedded.GetResponse();
        break;
      }
      case etcdserverpb::RequestOp::REQUEST_NOT_SET:
        break;
    }
  }

  response->mutable_header()->set_revision(revision.Revision);
  return ResponseVariant{std::move(*response)};
}

ResponseVariant BatchProcessor::ProcessRequest(
    std::unordered_map<std::string, KeyState>& prevKvs,
    const ::etcdserverpb::RangeRequest& request,
    std::unordered_map<std::string, KeyState>& changedKeys,
    std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
        history,
    std::vector<KeyState>& deleted, GetRevisionResponse& revision) {
  std::unordered_map<std::string, KeyState> range;

  for (auto& put : changedKeys) {
    if ((request.limit() != 0 && request.limit() <= range.size())) {
      break;
    }
    if (KvUtil::CompareKeys(request.key(), request.range_end(), put.first) &&
        !put.second.isDeleted()) {
      range[put.first] = put.second;
    }
  }

  for (auto& prevKv : prevKvs) {
    if ((request.limit() != 0 && request.limit() <= range.size())) {
      break;
    }

    if (!KvUtil::CompareKeys(request.key(), request.range_end(),
                             prevKv.first)) {
      continue;
    }

    if (!changedKeys.contains(prevKv.first)) {
      range[prevKv.first] = prevKv.second;
    }
  }
  google::protobuf::Arena arena;
  auto rangeResponse =
      google::protobuf::Arena::CreateMessage<etcdserverpb::RangeResponse>(
          &arena);
  rangeResponse->mutable_header()->set_revision(revision.Revision);
  rangeResponse->set_count(range.size());
  if (!request.count_only()) {
    for (auto& rangeKey : range) {
      mvccpb::KeyValue keyValue;
      keyValue.set_key(rangeKey.second.key);
      keyValue.set_version(rangeKey.second.version);
      keyValue.set_mod_revision(rangeKey.second.mod_revision);
      keyValue.set_create_revision(rangeKey.second.create_revision);
      keyValue.set_value(rangeKey.second.value);
      rangeResponse->mutable_kvs()->Add(std::move(keyValue));
    }
  }
  std::sort(rangeResponse->mutable_kvs()->begin(),
            rangeResponse->mutable_kvs()->end(),
            [](const mvccpb::KeyValue& a, const mvccpb::KeyValue& b) {
              return a.key() <= b.key();
            });
  if (request.sort_order() !=
      etcdserverpb::RangeRequest_SortOrder::RangeRequest_SortOrder_NONE) {
    auto apply_sort =
        [](google::protobuf::RepeatedPtrField<mvccpb::KeyValue>& r, auto comp,
           auto proj) { return std::ranges::stable_sort(r, comp, proj); };
    auto apply_proj = [&](google::protobuf::RepeatedPtrField<mvccpb::KeyValue>&
                              r,
                          auto comp) {
      auto impl = [&](auto proj) { return apply_sort(r, comp, proj); };
      switch (request.sort_target()) {
        case etcdserverpb::RangeRequest_SortTarget::RangeRequest_SortTarget_KEY:
          return impl(&mvccpb::KeyValue::key);
        case etcdserverpb::RangeRequest_SortTarget::
            RangeRequest_SortTarget_CREATE:
          return impl(&mvccpb::KeyValue::create_revision);
        case etcdserverpb::RangeRequest_SortTarget::RangeRequest_SortTarget_MOD:
          return impl(&mvccpb::KeyValue::mod_revision);
        case etcdserverpb::RangeRequest_SortTarget::
            RangeRequest_SortTarget_VERSION:
          return impl(&mvccpb::KeyValue::version);
        case etcdserverpb::RangeRequest_SortTarget::
            RangeRequest_SortTarget_VALUE:
          return impl(&mvccpb::KeyValue::value);
        default:
          throw std::runtime_error("Unknown sort target");
      }
    };
    auto apply_comp =
        [&](google::protobuf::RepeatedPtrField<mvccpb::KeyValue>& r) {
          auto impl = [&](auto comp) { return apply_proj(r, comp); };
          switch (request.sort_order()) {
            case etcdserverpb::RangeRequest_SortOrder_ASCEND:
              return impl(std::less{});
            case etcdserverpb::RangeRequest_SortOrder_DESCEND:
              return impl(std::greater{});
            default:
              throw std::runtime_error("Unknown sort order");
          }
        };
    apply_comp(*rangeResponse->mutable_kvs());
  }
  return ResponseVariant{std::move(*rangeResponse)};
}
}  // namespace etcd_ydb_userver