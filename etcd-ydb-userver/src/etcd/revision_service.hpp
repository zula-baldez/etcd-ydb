#pragma once

#include <userver/components/component.hpp>
#include <userver/components/component_list.hpp>
#include <userver/components/loggable_component_base.hpp>
#include <userver/ugrpc/client/client_factory_component.hpp>
#include <userver/ydb/component.hpp>
#include <userver/ydb/table.hpp>
#include "model.hpp"

#include <userver/ydb/builder.hpp>
#include "client_provider.hpp"

namespace etcd_ydb_userver {
class RevisionService final
    : public userver::components::LoggableComponentBase {
 public:
  static constexpr std::string_view kName = "revision-service";

  RevisionService(
      const userver::components::ComponentConfig& config,
      const userver::components::ComponentContext& component_context)
      : userver::components::LoggableComponentBase(config, component_context),
        table_client_provider_(
            component_context.FindComponent<EtcdTableClientProvider>()) {
    InitRevisions();
  }
  void InitRevisions();
  GetRevisionResponse RevisionGet(userver::ydb::Transaction& transaction);

  GetRevisionResponse RevisionGet(NYdb::NTable::TTransaction& transaction);

  void RevisionSet(i64 NewRevision, i64 NewCompactRevision,
                   NYdb::NTable::TTransaction& transaction);

  void RevisionSet(i64 NewRevision, i64 NewCompactRevision,
                   userver::ydb::Transaction& transaction);
  [[nodiscard]] GetRevisionResponse RevisionGetCached() const {
    return GetRevisionResponse(Revision, CompactRevision);
  }
  [[nodiscard]] i64 CompactRevisionGetCached() const { return CompactRevision; }

 private:
  EtcdTableClientProvider& table_client_provider_;
  i64 Revision = 1;
  i64 CompactRevision = 1;
};
void AppendRevisionService(userver::components::ComponentList& component_list);

}  // namespace etcd_ydb_userver

template <>
inline constexpr bool
    userver::components::kHasValidate<etcd_ydb_userver::RevisionService> = true;
