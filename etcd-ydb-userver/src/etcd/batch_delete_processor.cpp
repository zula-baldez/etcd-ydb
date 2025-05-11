#include "batch_processor.h"
namespace etcd_ydb_userver {

void BatchProcessor::ProcessDeleteKv(
    std::unordered_map<std::string, KeyState>& changedKeys,
    NYdb::NTable::TTransaction& transaction) {
  if (std::any_of(changedKeys.begin(), changedKeys.end(),
                  [](auto& k) { return k.second.isDeleted(); })) {
    NYdb::TParamsBuilder deleteKeys;
    auto& keys = deleteKeys.AddParam("$keys");
    keys.BeginList();
    for (auto& changed : changedKeys) {
      if (changed.second.isDeleted()) {
        keys.AddListItem()
            .BeginStruct()
            .AddMember("key")
            .String(changed.first)
            .EndStruct();
      }
    }
    keys.EndList();
    keys.Build();
    const std::string deleteQuery = R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $keys AS List<Struct<key: String>>;

DELETE
    FROM kv WHERE key IN (
SELECT key
    FROM AS_TABLE($keys)
);

)";
    auto response =
        transaction.GetSession()
            .ExecuteDataQuery(deleteQuery,
                              NYdb::NTable::TTxControl::Tx(transaction),
                              deleteKeys.Build())
            .GetValueSync();
  }
}

void BatchProcessor::ProcessPrevKvs(
    std::unordered_map<std::string, KeyState>& prevKvs,
    const ::etcdserverpb::DeleteRangeRequest& request,
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
    const ::etcdserverpb::DeleteRangeRequest& request,
    std::unordered_map<std::string, KeyState>& changedKeys,
    std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
        history,
    std::vector<KeyState>& deleted, GetRevisionResponse& revision) {
  std::unordered_map<std::string, KeyState> deletedInRequest;

  for (auto& put : changedKeys) {
    if (KvUtil::CompareKeys(request.key(), request.range_end(), put.first) &&
        !put.second.isDeleted()) {
      deletedInRequest[put.first] = put.second;
    }
  }
  for (auto& deletedPrev : prevKvs) {
    if (!KvUtil::CompareKeys(request.key(), request.range_end(),
                             deletedPrev.first)) {
      continue;
    }
    if (!changedKeys.contains(deletedPrev.first)) {
      deletedInRequest[deletedPrev.first] = deletedPrev.second;
    }
  }

  google::protobuf::Arena arena;
  auto deleteResponse =
      google::protobuf::Arena::CreateMessage<etcdserverpb::DeleteRangeResponse>(
          &arena);
  auto newRevision =
      deletedInRequest.empty() ? revision.Revision : revision.Revision + 1;
  deleteResponse->mutable_header()->set_revision(newRevision);
  deleteResponse->set_deleted(deletedInRequest.size());

  for (auto& deletedKey : deletedInRequest) {
    deletedKey.second.KeyToDeleted(revision);
    deletedKey.second.changedViaDeletion = true;
    if (request.prev_kv()) {
      mvccpb::KeyValue keyValue;
      keyValue.set_key(deletedKey.second.key);
      keyValue.set_version(deletedKey.second.version);
      keyValue.set_mod_revision(deletedKey.second.mod_revision);
      keyValue.set_create_revision(deletedKey.second.create_revision);
      keyValue.set_value(deletedKey.second.value);
      deleteResponse->mutable_prev_kvs()->Add(std::move(keyValue));
    }
    history.push_back({{}, deletedKey.second});
    deleted.push_back(deletedKey.second);
    changedKeys[deletedKey.first] = deletedKey.second;
  }
  revision.Revision = newRevision;
  return ResponseVariant{std::move(*deleteResponse)};
}
}  // namespace etcd_ydb_userver