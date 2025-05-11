#include "query_optimizer.h"
namespace etcd_ydb_userver {

void AppendQueryOptimizer(userver::components::ComponentList& component_list) {
  component_list.Append<etcd_ydb_userver::QueryOptimizer>();
}
}  // namespace etcd_ydb_userver
