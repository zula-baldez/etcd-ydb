#pragma once
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <api/etcdserverpb/rpc_service.usrv.pb.hpp>
#include <userver/clients/dns/component.hpp>
#include <userver/components/component.hpp>
#include <userver/components/component_list.hpp>
#include <userver/components/loggable_component_base.hpp>
#include <userver/ugrpc/client/client_factory_component.hpp>
#include <userver/ydb/component.hpp>
#include <userver/ydb/table.hpp>
#include "check_request.h"
#include "model.hpp"
#include "revision_service.hpp"

#include <userver/ydb/builder.hpp>
#include "client_provider.hpp"
#include "query_optimizer.h"

namespace etcd_ydb_userver {

class KvService final : public etcdserverpb::KVBase::Component {
 public:
  static constexpr std::string_view kName = "kv-service";

  KvService(const userver::components::ComponentConfig& config,
            const userver::components::ComponentContext& component_context)
      : etcdserverpb::KVBase::Component(config, component_context),
        table_client_provider_(
            component_context.FindComponent<EtcdTableClientProvider>()),
        revision_service_(component_context.FindComponent<RevisionService>()),
        query_optimizer_(component_context.FindComponent<QueryOptimizer>()) {}

  using RangeResult =
      USERVER_NAMESPACE::ugrpc::server::Result< ::etcdserverpb::RangeResponse>;
  using PutResult =
      USERVER_NAMESPACE::ugrpc::server::Result< ::etcdserverpb::PutResponse>;
  using DeleteRangeResult = USERVER_NAMESPACE::ugrpc::server::Result<
      ::etcdserverpb::DeleteRangeResponse>;
  using TxnResult =
      USERVER_NAMESPACE::ugrpc::server::Result< ::etcdserverpb::TxnResponse>;
  using CompactResult = USERVER_NAMESPACE::ugrpc::server::Result<
      ::etcdserverpb::CompactionResponse>;

  RangeResult Range(CallContext& context,
                    ::etcdserverpb::RangeRequest&& request) override;

  PutResult Put(userver::ugrpc::server::CallContext& context,
                etcdserverpb::PutRequest&& request) override {
    return query_optimizer_.Put(context, std::move(request));
  }

  DeleteRangeResult DeleteRange(
      userver::ugrpc::server::CallContext& context,
      etcdserverpb::DeleteRangeRequest&& request) override {
    return query_optimizer_.DeleteRange(context, std::move(request));
  }

  TxnResult Txn(CallContext& context,
                ::etcdserverpb::TxnRequest&& request) override {
    return query_optimizer_.Txn(context, std::move(request));
  }

  CompactResult Compact(CallContext& context,
                        ::etcdserverpb::CompactionRequest&& request) override;

  CompactResult Compact(CallContext& context,
                        ::etcdserverpb::CompactionRequest&& request,
                        GetRevisionResponse& revision,
                        userver::ydb::Transaction& transaction);

 private:
  EtcdTableClientProvider& table_client_provider_;
  RevisionService& revision_service_;
  QueryOptimizer& query_optimizer_;
};

void AppendKvService(userver::components::ComponentList& component_list);
}  // namespace etcd_ydb_userver