#include "client_provider.hpp"

namespace etcd_ydb_userver {
void EtcdTableClientProvider::createTables() {
  (*ydb_client_)
      .CreateTable(
          ".etcd/revision",
          NYdb::NTable::TTableBuilder()
              .AddNonNullableColumn("id", NYdb::EPrimitiveType::Bool)
              .AddNonNullableColumn("revision", NYdb::EPrimitiveType::Int64)
              .SetPrimaryKeyColumn("id")
              .Build());
  try {
    static const userver::ydb::Query revisionInitQuery{
        R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $revision AS Int64;
DECLARE $compact_revision AS Int64;

INSERT INTO revision (id, revision) VALUES
    (FALSE, 1),
    (TRUE,  1);
      )",
        userver::ydb::Query::Name{"revision-init-query"},
    };
    (*ydb_client_).ExecuteQuery(revisionInitQuery);
  } catch (std::exception& e) {
    LOG_DEBUG() << e.what();
    // already exists
  }
  (*ydb_client_)
      .CreateTable(
          ".etcd/kv",
          NYdb::NTable::TTableBuilder()
              .AddNonNullableColumn("key", NYdb::EPrimitiveType::String)
              .AddNonNullableColumn("mod_revision", NYdb::EPrimitiveType::Int64)
              .AddNonNullableColumn("create_revision",
                                    NYdb::EPrimitiveType::Int64)
              .AddNonNullableColumn("version", NYdb::EPrimitiveType::Int64)
              .AddNonNullableColumn("value", NYdb::EPrimitiveType::String)

              .SetPrimaryKeyColumn("key")
              .Build());
  (*ydb_client_)
      .CreateTable(
          ".etcd/kv_past",
          NYdb::NTable::TTableBuilder()
              .AddNonNullableColumn("key", NYdb::EPrimitiveType::String)
              .AddNonNullableColumn("mod_revision", NYdb::EPrimitiveType::Int64)
              .AddNonNullableColumn("create_revision",
                                    NYdb::EPrimitiveType::Int64)
              .AddNonNullableColumn("version", NYdb::EPrimitiveType::Int64)
              .AddNonNullableColumn("value", NYdb::EPrimitiveType::String)
              .AddNonNullableColumn("delete_revision",
                                    NYdb::EPrimitiveType::Int64)
              .AddNonNullableColumn("deleted", NYdb::EPrimitiveType::Bool)
              .SetPrimaryKeyColumns({"key", "mod_revision"})
              .Build());
}

void EtcdTableClientProvider::createWatchApiTopic() {
  try {
    auto topicSettings =
        NYdb::NTopic::TCreateTopicSettings()
            .PartitioningSettings(1, 1)
            .PartitionWriteSpeedBytesPerSecond(1024 * 1024 * 10)
            .PartitionWriteBurstBytes(1024 * 1024 * 10)
            .SetSupportedCodecs({NYdb::NTopic::ECodec::RAW});

    auto status = (*topic_client_)
        .GetNativeTopicClient()
        .CreateTopic("/Root/test/.etcd/watch-stream", topicSettings)
        .GetValueSync();
    LOG_INFO() << status.IsSuccess();

    TString producerAndGroupID = "group-id";

    auto writeSettings = NYdb::NTopic::TWriteSessionSettings()
                             .Path("/Root/test/.etcd/watch-stream")
                             .BatchFlushInterval(TDuration::MicroSeconds(0))
                             .BatchFlushSizeBytes(0)
                             .ProducerId(producerAndGroupID)
                             .MessageGroupId(producerAndGroupID);
    write_session_ = getTopicClient()
                              .GetNativeTopicClient()
                              .CreateWriteSession(writeSettings);

    auto consumerSettings = NYdb::NTopic::TAlterTopicSettings()
                                .BeginAddConsumer("watch-api-consumer")
                                .Important(true)
                                .EndAddConsumer()
                                .SetRetentionPeriod(TDuration::Seconds(60));

    (*topic_client_)
        .AlterTopic("/Root/test/.etcd/watch-stream", consumerSettings);
  } catch (std::exception& e) {
    LOG_ERROR() << e.what();
  }
}

userver::ydb::TableClient& EtcdTableClientProvider::getTableClient() {
  return *ydb_client_;
}

userver::ydb::TopicClient& EtcdTableClientProvider::getTopicClient() {
  return *topic_client_;
}

std::shared_ptr<NYdb::NTopic::IWriteSession> EtcdTableClientProvider::getWriteSession() {
  return write_session_;
}

void AppendTableClientProvider(
    userver::components::ComponentList& component_list) {
  component_list.Append<EtcdTableClientProvider>();
}
}  // namespace etcd_ydb_userver