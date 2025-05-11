#pragma once

#include <userver/components/component.hpp>
#include <userver/components/component_list.hpp>
#include <userver/components/loggable_component_base.hpp>
#include <userver/ugrpc/client/client_factory_component.hpp>
#include <userver/ydb/component.hpp>

#include <userver/ydb/table.hpp>
#include <userver/ydb/topic.hpp>
#include <userver/ydb/transaction.hpp>

#include <userver/ydb/builder.hpp>

namespace etcd_ydb_userver {
class EtcdTableClientProvider final
    : public userver::components::LoggableComponentBase {
 public:
  static constexpr std::string_view kName = "etcd-service";

  EtcdTableClientProvider(
      const userver::components::ComponentConfig& config,
      const userver::components::ComponentContext& component_context)
      : userver::components::LoggableComponentBase(config, component_context),
        ydb_client_(
            component_context.FindComponent<userver::ydb::YdbComponent>()
                .GetTableClient("test")),
        topic_client_(
            component_context.FindComponent<userver::ydb::YdbComponent>()
                .GetTopicClient("test")) {
    createTables();
    createWatchApiTopic();
  }
  userver::ydb::TableClient& getTableClient();
  userver::ydb::TopicClient& getTopicClient();
  std::shared_ptr<NYdb::NTopic::IWriteSession> getWriteSession();

 private:
  std::shared_ptr<userver::ydb::TableClient> ydb_client_;
  std::shared_ptr<userver::ydb::TopicClient> topic_client_;
  std::shared_ptr<NYdb::NTopic::IWriteSession> write_session_;

  void createTables();
  void createWatchApiTopic();

};
void AppendTableClientProvider(
    userver::components::ComponentList& component_list);

}  // namespace etcd_ydb_userver

template <>
inline constexpr bool userver::components::kHasValidate<
    etcd_ydb_userver::EtcdTableClientProvider> = true;
