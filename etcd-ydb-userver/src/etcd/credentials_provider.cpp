//
// Created by egor on 02.03.25.
//

#include "credentials_provider.h"

namespace etcd_ydb_userver {

std::shared_ptr<NYdb::ICredentialsProviderFactory>
CredentialsProvider::CreateCredentialsProviderFactory(
    const userver::yaml_config::YamlConfig& credentials) const {
  return NYdb::CreateInsecureCredentialsProviderFactory();
}

void AppendCredentialsProvider(
    userver::components::ComponentList& component_list) {
  component_list.Append<CredentialsProvider>();
}

}  // namespace etcd_ydb_userver