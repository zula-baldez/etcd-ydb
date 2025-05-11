#include <userver/clients/dns/component.hpp>
#include <userver/clients/http/component.hpp>
#include <userver/compiler/impl/tsan.hpp>
#include <userver/components/minimal_server_component_list.hpp>
#include <userver/congestion_control/component.hpp>
#include <userver/server/handlers/ping.hpp>
#include <userver/server/handlers/tests_control.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/testsuite/testsuite_support.hpp>
#include <userver/ugrpc/server/component_list.hpp>
#include <userver/utils/daemon_run.hpp>
#include <userver/ydb/component.hpp>
#include "etcd/batch_processor.h"
#include "etcd/client_provider.hpp"
#include "etcd/credentials_provider.h"
#include "etcd/kv_service.hpp"
#include "etcd/query_optimizer.h"
#include "etcd/revision_service.hpp"
#include "etcd/watch_service.h"
#include "etcd/watch_synchronization.h"
int main(int argc, char* argv[]) {
  auto component_list =
      userver::components::MinimalServerComponentList()
          .AppendComponentList(userver::ugrpc::server::MinimalComponentList())
          .Append<userver::congestion_control::Component>()
          .Append<userver::server::handlers::Ping>()
          .Append<userver::components::TestsuiteSupport>()
          .Append<userver::components::HttpClient>()
          .Append<userver::server::handlers::TestsControl>()
          .Append<userver::components::DefaultSecdistProvider>()
          .Append<userver::components::Secdist>()
          .Append<userver::ydb::YdbComponent>();

  etcd_ydb_userver::AppendTableClientProvider(component_list);
  etcd_ydb_userver::AppendRevisionService(component_list);
  etcd_ydb_userver::AppendKvService(component_list);
  etcd_ydb_userver::AppendCredentialsProvider(component_list);
  etcd_ydb_userver::AppendWatchService(component_list);
  etcd_ydb_userver::AppendWatchSynchronizationProvider(component_list);
  etcd_ydb_userver::AppendQueryOptimizer(component_list);
  etcd_ydb_userver::AppendBatchProcessor(component_list);

  component_list.Append<userver::clients::dns::Component>();
  return userver::utils::DaemonMain(argc, argv, component_list);
}