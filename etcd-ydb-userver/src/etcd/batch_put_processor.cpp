#include "batch_processor.h"
namespace etcd_ydb_userver {

void BatchProcessor::ProcessPrevKvs(
    std::unordered_map<std::string, KeyState>& prevKvs,
    const ::etcdserverpb::PutRequest& request,
    std::optional<std::reference_wrapper<userver::engine::Mutex>>
        keyBucketMutex,
    NYdb::NTable::TSession& session) {
  std::string statement = R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $key AS String;

SELECT *
    FROM kv
    WHERE key == $key;
)";

  NYdb::TParamsBuilder builder = client_provider_.getTableClient()
                                     .GetNativeTableClient()
                                     .GetParamsBuilder();
  builder.AddParam("$key").String(request.key()).Build();

  auto response =
      session
          .ExecuteDataQuery(statement,
                            NYdb::NTable::TTxControl::BeginTx(
                                NYdb::NTable::TTxSettings::OnlineRO())
                                .CommitTx(),
                            builder.Build())
          .GetValueSync();

  auto cursor = response.GetResultSetParser(0);
  while (cursor.TryNextRow()) {
    auto keyState = KeyState(cursor.ColumnParser("key").GetString(),
                             cursor.ColumnParser("value").GetString(),
                             cursor.ColumnParser("mod_revision").GetInt64(),
                             cursor.ColumnParser("create_revision").GetInt64(),
                             std::optional<int64_t>(),
                             cursor.ColumnParser("version").GetInt64());
    if (keyBucketMutex.has_value()) {
      std::lock_guard<userver::engine::Mutex> lock(keyBucketMutex->get());
      prevKvs[request.key()] = keyState;
    } else {
      prevKvs[request.key()] = keyState;
    }
  }
}

ResponseVariant BatchProcessor::ProcessRequest(
    std::unordered_map<std::string, KeyState>& prevKvs,
    const ::etcdserverpb::PutRequest& request,
    std::unordered_map<std::string, KeyState>& changedKeys,
    std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
        history,
    std::vector<KeyState>& deleted, GetRevisionResponse& revision) {
  KeyState prevVal;
  KeyState newVal;
  if (prevKvs.contains(request.key())) {
    prevVal = prevKvs[request.key()];
  }
  if (changedKeys.contains(request.key())) {
    prevVal = changedKeys[request.key()];
  }

  if (request.ignore_value() && !prevVal.isInitialized()) {
    return ResponseVariant{PutResult{grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: key not found")}};
  }

  if (!prevVal.isInitialized()) {
    newVal = KeyState::NewKey(request.key(), request.value(), revision);
  } else {
    newVal = prevVal.NewVersion(revision, request);
    prevVal.KeyToDeleted(revision);
  }

  google::protobuf::Arena arena;

  auto putResponse =
      google::protobuf::Arena::CreateMessage<etcdserverpb::PutResponse>(&arena);

  putResponse->mutable_header()->set_revision(newVal.mod_revision);
  if (request.prev_kv() && prevVal.isInitialized()) {
    putResponse->mutable_prev_kv()->set_key(prevVal.key);
    putResponse->mutable_prev_kv()->set_mod_revision(prevVal.mod_revision);
    putResponse->mutable_prev_kv()->set_create_revision(
        prevVal.create_revision);
    putResponse->mutable_prev_kv()->set_version(prevVal.version);
    putResponse->mutable_prev_kv()->set_value(prevVal.value);
  }
  if (prevVal.isInitialized()) {
    deleted.push_back(prevVal);
    history.emplace_back(newVal, prevVal);
  } else {
    history.push_back({newVal, {}});
  }
  changedKeys[newVal.key] = newVal;
  revision.Revision = newVal.mod_revision;
  return ResponseVariant{std::move(*putResponse)};
}

void BatchProcessor::ProcessPutKv(
    std::unordered_map<std::string, KeyState>& changedKeys,
    NYdb::NTable::TTransaction& transaction) {
  if (std::any_of(changedKeys.begin(), changedKeys.end(),
                  [](auto& k) { return !k.second.isDeleted(); })) {
    const userver::ydb::Query kvsQuery{
        R"(
      PRAGMA TablePathPrefix("/Root/test/.etcd");

      DECLARE $kvs AS List<Struct<
          key: String,
          value: String,
          create_revision: Int64,
          mod_revision: Int64,
          version: Int64
      >>;

      UPSERT INTO kv (key, mod_revision, create_revision, version, value)
      SELECT key, mod_revision, create_revision, version, value
      FROM AS_TABLE($kvs);
            )"};

    NYdb::TParamsBuilder params;

    auto& targetParam = params.AddParam("$kvs");
    targetParam.BeginList();
    for (const auto& changed : changedKeys) {
      if (!changed.second.isDeleted()) {
        targetParam.AddListItem()
            .BeginStruct()
            .AddMember("key")
            .String(changed.first)
            .AddMember("value")
            .String(changed.second.value)
            .AddMember("mod_revision")
            .Int64(changed.second.mod_revision)
            .AddMember("create_revision")
            .Int64(changed.second.create_revision)
            .AddMember("version")
            .Int64(changed.second.version)
            .EndStruct();
      }
    }
    targetParam.EndList();
    targetParam.Build();
    auto response =
        transaction.GetSession()
            .ExecuteDataQuery(kvsQuery.Statement(),
                              NYdb::NTable::TTxControl::Tx(transaction),
                              params.Build())
            .GetValueSync();
  }
}

void BatchProcessor::ProcessPutPrevKv(std::vector<KeyState>& deleted,
                                      NYdb::NTable::TTransaction& transaction) {
  if (!deleted.empty()) {
    const std::string prevKvsQuery =
        R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $prev_kvs AS List<Struct<
    key: String,
    value: String,
    create_revision: Int64,
    mod_revision: Int64,
    version: Int64,
    delete_revision: Int64,
    deleted: Bool
>>;

UPSERT INTO kv_past (key, mod_revision, create_revision, version, value, delete_revision, deleted)
SELECT key, mod_revision, create_revision, version, value, delete_revision, deleted
FROM AS_TABLE($prev_kvs);
)";

    NYdb::TParamsBuilder prevKvsParamsBuilder;

    auto& prevKvsParams = prevKvsParamsBuilder.AddParam("$prev_kvs");
    prevKvsParams.BeginList();
    for (const auto& changed : deleted) {
      prevKvsParams.AddListItem()
          .BeginStruct()
          .AddMember("key")
          .String(changed.key)
          .AddMember("value")
          .String(changed.value)
          .AddMember("mod_revision")
          .Int64(changed.mod_revision)
          .AddMember("create_revision")
          .Int64(changed.create_revision)
          .AddMember("version")
          .Int64(changed.version)
          .AddMember("delete_revision")
          .Int64(changed.delete_revision.value())
          .AddMember("deleted")
          .Bool(changed.changedViaDeletion)
          .EndStruct();
    }

    prevKvsParams.EndList();
    prevKvsParams.Build();

    auto response =
        transaction.GetSession()
            .ExecuteDataQuery(prevKvsQuery,
                              NYdb::NTable::TTxControl::Tx(transaction),
                              prevKvsParamsBuilder.Build())
            .GetValueSync();
  }
}

}  // namespace etcd_ydb_userver