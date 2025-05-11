#include "kv_service.hpp"

namespace etcd_ydb_userver {

void AppendKvService(userver::components::ComponentList& component_list) {
  component_list.Append<etcd_ydb_userver::KvService>();
}

}  // namespace etcd_ydb_userver
