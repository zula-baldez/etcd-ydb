#include "revision_service.hpp"

namespace etcd_ydb_userver {

void RevisionService::RevisionSet(i64 NewRevision, i64 NewCompactRevision,
                                  userver::ydb::Transaction& transaction) {
  static const userver::ydb::Query revisionSetQuery{
      R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $revision AS Int64;
DECLARE $compact_revision AS Int64;

UPSERT INTO revision (id, revision) VALUES
    (FALSE, $compact_revision),
    (TRUE,  $revision);
      )",
      userver::ydb::Query::Name{"revision-set"},
  };

  auto response =
      transaction.Execute(revisionSetQuery, "$revision", NewRevision,
                          "$compact_revision", NewCompactRevision);
  if (response.GetCursorCount()) {
    throw std::runtime_error("Unexpected response data");
  }
  this->Revision = NewRevision;
  this->CompactRevision = NewCompactRevision;
}

void RevisionService::RevisionSet(i64 NewRevision, i64 NewCompactRevision,
                                  NYdb::NTable::TTransaction& transaction) {
  static const std::string revisionSetQuery =
      R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $revision AS Int64;
DECLARE $compact_revision AS Int64;

UPSERT INTO revision (id, revision) VALUES
    (FALSE, $compact_revision),
    (TRUE,  $revision);
      )";

  NYdb::TParamsBuilder builder;
  builder.AddParam("$revision").Int64(NewCompactRevision).Build();
  builder.AddParam("$compact_revision").Int64(NewCompactRevision).Build();

  auto response =
      transaction.GetSession()
          .ExecuteDataQuery(revisionSetQuery,
                            NYdb::NTable::TTxControl::Tx(transaction),
                            builder.Build())
          .GetValueSync();

  this->Revision = NewRevision;
  this->CompactRevision = NewCompactRevision;
}

GetRevisionResponse RevisionService::RevisionGet(
    userver::ydb::Transaction& transaction) {
  static const userver::ydb::Query revisionGetQuery{
      R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

SELECT * FROM revision;
      )",
      userver::ydb::Query::Name{"revision-get"},
  };

  auto response = transaction.Execute(revisionGetQuery);
  if (response.GetCursorCount() != 1) {
    throw std::runtime_error("Unknown database response");
  }
  if (response.GetSingleCursor().RowsCount() != 2) {
    throw std::runtime_error("Expected 2 rows in response");
  }
  GetRevisionResponse revisionResponse = GetRevisionResponse();
  for (auto row : response.GetSingleCursor()) {
    bool id = row.Get<bool>("id");
    if (id) {
      revisionResponse.Revision = row.Get<i64>("revision");
    } else {
      revisionResponse.CompactRevision = row.Get<i64>("revision");
    }
  }
  this->Revision = revisionResponse.Revision;
  this->CompactRevision = revisionResponse.CompactRevision;
  return revisionResponse;
}

GetRevisionResponse RevisionService::RevisionGet(
    NYdb::NTable::TTransaction& transaction) {
  static const std::string query =
      R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

SELECT * FROM revision;
      )";

  auto response =
      transaction.GetSession()
          .ExecuteDataQuery(query, NYdb::NTable::TTxControl::Tx(transaction))
          .GetValueSync();

  if (response.GetResultSets().size() != 1) {
    throw std::runtime_error("Unknown database response");
  }
  if (response.GetResultSet(0).RowsCount() != 2) {
    throw std::runtime_error("Expected 2 rows in response");
  }
  GetRevisionResponse revisionResponse = GetRevisionResponse();
  auto cursor = response.GetResultSetParser(0);
  while (cursor.TryNextRow()) {
    bool id = cursor.ColumnParser("id").GetBool();
    if (id) {
      revisionResponse.Revision = cursor.ColumnParser("revision").GetInt64();
    } else {
      revisionResponse.CompactRevision =
          cursor.ColumnParser("revision").GetInt64();
    }
  }
  this->Revision = revisionResponse.Revision;
  this->CompactRevision = revisionResponse.CompactRevision;
  return revisionResponse;
}

void RevisionService::InitRevisions() {
  auto txn = table_client_provider_.getTableClient().Begin("init");
  RevisionGet(txn);
  txn.Commit();
}

void AppendRevisionService(userver::components::ComponentList& component_list) {
  component_list.Append<RevisionService>();
}
}  // namespace etcd_ydb_userver