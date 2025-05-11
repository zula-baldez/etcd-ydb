#include "kv_service.hpp"

namespace etcd_ydb_userver {

KvService::RangeResult KvService::Range(
    CallContext& context, ::etcdserverpb::RangeRequest&& request) {
  if (const auto status = CheckRequest(request); !status.ok()) {
    return KvService::RangeResult{status};
  }
  try {
    auto session = table_client_provider_.getTableClient()
                       .GetNativeTableClient()
                       .CreateSession()
                       .GetValueSync()
                       .GetSession();

    auto transaction =
        session.BeginTransaction(NYdb::NTable::TTxSettings::SnapshotRO())
            .GetValueSync()
            .GetTransaction();


    auto revision = revision_service_.RevisionGet(transaction);

    if (request.revision() != 0 && request.revision() > revision.Revision) {
      return KvService::RangeResult{grpc::Status(
          grpc::StatusCode::OUT_OF_RANGE,
          "etcdserver: mvcc: required revision is a future revision")};
    }
    if (request.revision() != 0 &&
        request.revision() < revision.CompactRevision) {
      return KvService::RangeResult{grpc::Status(
          grpc::StatusCode::OUT_OF_RANGE,
          "etcdserver: mvcc: required revision has been compacted")};
    }

    auto [compareCond, useRangeEnd] =
        KvUtil::Compare(request.key(), request.range_end());

    std::ostringstream query;
    query << R"(
            PRAGMA TablePathPrefix("/Root/test/.etcd");

            DECLARE $key AS String;)";
    if (useRangeEnd) {
      query << R"(
            DECLARE $range_end AS String;)";
    }
    if (request.revision() > 0) {
      query << R"(
            DECLARE $revision AS Int64;)";
    }
    if (request.limit() > 0) {
      query << R"(
            DECLARE $limit AS Uint64;)";
    }
    query << R"(
            )";
    if (request.revision() > 0) {
      query << R"(
            SELECT )"
            << (request.count_only()  ? "COUNT(*) AS count"
                : request.limit() > 0 ? "COUNT(*) OVER() AS count, kv_past.*"
                                      : "*")
            << R"(
                FROM kv_past
                WHERE )"
            << compareCond.data() << R"(
                    AND mod_revision <= $revision AND (delete_revision IS NULL OR $revision < delete_revision))";
      if (request.limit() > 0) {
        query << R"(
                LIMIT $limit)";
      }
      query << ";";
      query << R"(
            SELECT )"
            << (request.count_only()  ? "COUNT(*) AS count"
                : request.limit() > 0 ? "COUNT(*) OVER() AS count, kv.*"
                                      : "*")
            << R"(
                FROM kv
                WHERE )"
            << compareCond.data() << R"(
                    AND mod_revision <= $revision)";
      if (request.limit() > 0) {
        query << R"(
                LIMIT $limit)";
      }
      query << ";";
    } else {
      query << R"(
            SELECT )"
            << (request.count_only()  ? "COUNT(*) AS count"
                : request.limit() > 0 ? "COUNT(*) OVER() AS count, kv.*"
                                      : "*")
            << R"(
            FROM kv
                WHERE )"
            << compareCond.data();

      if (request.limit() > 0) {
        query << R"(
                LIMIT $limit)";
      }
      query << ";";
    }
    NYdb::TParamsBuilder builder = table_client_provider_.getTableClient()
                                       .GetNativeTableClient()
                                       .GetParamsBuilder();
    builder.AddParam("$key").String(request.key()).Build();

    if (useRangeEnd) {
      builder.AddParam("$range_end").String(request.range_end()).Build();
    }
    if (request.revision() > 0) {
      builder.AddParam("$revision").Int64(request.revision()).Build();
    }
    if (request.limit() > 0) {
      builder.AddParam("$limit")
          .Uint64(static_cast<uint64_t>(request.limit()))
          .Build();
    }

    auto response =
        session
            .ExecuteDataQuery(
                query.str(),
                NYdb::NTable::TTxControl::Tx(transaction).CommitTx(),
                builder.Build())
            .GetValueSync();

    google::protobuf::Arena arena;
    auto rangeResponse =
        google::protobuf::Arena::CreateMessage<etcdserverpb::RangeResponse>(
            &arena);

    rangeResponse->mutable_header()->set_revision(revision.Revision);
    if (request.count_only()) {
      for (size_t i = 0; i < response.GetResultSets().size(); ++i) {
        auto cursor = response.GetResultSetParser(i);
        if (cursor.RowsCount() != 1) {
          throw std::runtime_error("Expected 1 row in database response");
        }
        cursor.TryNextRow();
        rangeResponse->set_count(rangeResponse->count() +
                                 cursor.ColumnParser("count").GetUint64());
      }
    } else if (request.limit() > 0) {
      for (size_t i = 0; i < response.GetResultSets().size(); ++i) {
        auto cursor = response.GetResultSetParser(i);
        int64_t responseCount = 0;
        while (cursor.TryNextRow()) {
          responseCount = cursor.ColumnParser("count").GetUint64();
          LOG_DEBUG() << responseCount;

          if (rangeResponse->kvs_size() >= request.limit()) {
            break;
          }
          mvccpb::KeyValue keyValue = mvccpb::KeyValue();

          keyValue.set_key(cursor.ColumnParser("key").GetString());
          keyValue.set_mod_revision(
              cursor.ColumnParser("mod_revision").GetInt64());
          keyValue.set_create_revision(
              cursor.ColumnParser("create_revision").GetInt64());
          keyValue.set_version(cursor.ColumnParser("version").GetInt64());
          keyValue.set_value(request.keys_only()
                                 ? ""
                                 : cursor.ColumnParser("value").GetString());
          rangeResponse->mutable_kvs()->Add(std::move(keyValue));
        }
        rangeResponse->set_count(rangeResponse->count() + responseCount);
      }
      rangeResponse->set_more(rangeResponse->kvs_size() <
                              rangeResponse->count());
    } else {
      for (size_t i = 0; i < response.GetResultSets().size(); ++i) {
        auto cursor = response.GetResultSetParser(i);
        while (cursor.TryNextRow()) {
          mvccpb::KeyValue keyValue = mvccpb::KeyValue();

          keyValue.set_key(cursor.ColumnParser("key").GetString());

          keyValue.set_mod_revision(
              cursor.ColumnParser("mod_revision").GetInt64());

          keyValue.set_create_revision(
              cursor.ColumnParser("create_revision").GetInt64());

          keyValue.set_version(cursor.ColumnParser("version").GetInt64());

          keyValue.set_value(request.keys_only()
                                 ? ""
                                 : cursor.ColumnParser("value").GetString());
          rangeResponse->mutable_kvs()->Add(std::move(keyValue));
        }
        rangeResponse->set_count(rangeResponse->count() + cursor.RowsCount());
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
      auto apply_proj =
          [&](google::protobuf::RepeatedPtrField<mvccpb::KeyValue>& r,
              auto comp) {
            auto impl = [&](auto proj) { return apply_sort(r, comp, proj); };
            switch (request.sort_target()) {
              case etcdserverpb::RangeRequest_SortTarget::
                  RangeRequest_SortTarget_KEY:
                return impl(&mvccpb::KeyValue::key);
              case etcdserverpb::RangeRequest_SortTarget::
                  RangeRequest_SortTarget_CREATE:
                return impl(&mvccpb::KeyValue::create_revision);
              case etcdserverpb::RangeRequest_SortTarget::
                  RangeRequest_SortTarget_MOD:
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
    if (request.revision() != 0 && request.revision() > revision.Revision) {
      throw std::runtime_error(
          "etcdserver: mvcc: required revision is a future revision");
    }
    if (request.revision() != 0 &&
        request.revision() < revision.CompactRevision) {
      throw std::runtime_error(
          "etcdserver: mvcc: required revision has been compacted");
    }
    return KvService::RangeResult{std::move(*rangeResponse)};
  } catch (std::exception& e) {
    LOG_INFO() << e.what();
    return KvService::RangeResult{
        grpc::Status(grpc::StatusCode::INTERNAL, e.what())};
  }
}

}  // namespace etcd_ydb_userver