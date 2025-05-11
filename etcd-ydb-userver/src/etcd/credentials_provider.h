#pragma once

#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb-cpp-sdk/client/types/fwd.h>
#include <userver/components/component.hpp>
#include <userver/components/component_list.hpp>
#include <userver/ydb/component.hpp>

#include <userver/ydb/credentials.hpp>

namespace etcd_ydb_userver {

class CredentialsProvider : public userver::ydb::CredentialsProviderComponent {
 public:
  static constexpr std::string_view kName = "cred-prov";

  CredentialsProvider(
      const userver::components::ComponentConfig& config,
      const userver::components::ComponentContext& component_context)
      : userver::ydb::CredentialsProviderComponent(config, component_context) {}

  [[nodiscard]] std::shared_ptr<NYdb::ICredentialsProviderFactory>
  CreateCredentialsProviderFactory(
      const userver::yaml_config::YamlConfig& credentials) const override;
};
void AppendCredentialsProvider(
    userver::components::ComponentList& component_list);
}  // namespace etcd_ydb_userver
