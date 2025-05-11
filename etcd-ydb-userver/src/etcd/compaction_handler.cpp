
#include "kv_service.hpp"

namespace etcd_ydb_userver {
KvService::CompactResult KvService::Compact(
    userver::ugrpc::server::CallContext& context,
    etcdserverpb::CompactionRequest&& request, GetRevisionResponse& revision,
    userver::ydb::Transaction& transaction) {
  if (request.revision() != 0 && request.revision() > revision.Revision) {
    return KvService::CompactResult{grpc::Status(
        grpc::StatusCode::OUT_OF_RANGE,
        "etcdserver: mvcc: required revision is a future revision")};
  }
  if (request.revision() != 0 &&
      request.revision() < revision.CompactRevision) {
    return KvService::CompactResult{
        grpc::Status(grpc::StatusCode::OUT_OF_RANGE,
                     "etcdserver: mvcc: required revision has been compacted")};
  }
  const userver::ydb::Query compactQuery{
      R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $revision AS Int64;

DELETE
    FROM kv_past
    WHERE delete_revision <= $revision;
      )",
      userver::ydb::Query::Name{"kv-compact-query"},
  };

  auto response =
      transaction.Execute(compactQuery, "$revision", request.revision());
  if (response.GetCursorCount() != 0) {
    throw std::runtime_error("Unexpected database response");
  }
  google::protobuf::Arena arena;

  auto compactionResponse =
      google::protobuf::Arena::CreateMessage<etcdserverpb::CompactionResponse>(
          &arena);

  compactionResponse->mutable_header()->set_revision(revision.Revision);

  return KvService::CompactResult{std::move(*compactionResponse)};
}

KvService::CompactResult KvService::Compact(
    userver::ugrpc::server::CallContext& context,
    etcdserverpb::CompactionRequest&& request) {
  if (const auto status = CheckRequest(request); !status.ok()) {
    return KvService::CompactResult{status};
  }

  try {
    auto transaction = table_client_provider_.getTableClient().Begin(
        "compact-transaction", userver::ydb::TransactionMode::kSerializableRW);
    try {
      auto revision = revision_service_.RevisionGet(transaction);
      auto res = KvService::Compact(context, std::move(request), revision,
                                    transaction);

      if (res.IsSuccess()) {
        revision_service_.RevisionSet(res.GetResponse().header().revision(),
                                      request.revision(), transaction);
      }
      transaction.Commit();
      return res;
    } catch (std::exception& e) {
      transaction.Rollback();
      LOG_INFO() << e.what();
      return KvService::CompactResult{
          grpc::Status(grpc::StatusCode::INTERNAL, e.what())};
    }
  } catch (std::exception& e) {
    return KvService::CompactResult{grpc::Status::CANCELLED};
  }
}
}  // namespace etcd_ydb_userver