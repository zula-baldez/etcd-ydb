#include "watch_service.h"

namespace etcd_ydb_userver {

void WatchService::SendHistoricalKeysChanges(
    WatchStreamData& watchStreamData,
    const etcdserverpb::WatchRequest& watchRequest, int64_t watchId) {
  auto request = watchRequest.create_request();
  auto compare = KvUtil::Compare(request.key(), request.range_end());

  std::shared_lock<userver::engine::SharedMutex> lock(watchStreamData.processMessageMutex);

  auto session = table_client_provider_.getTableClient()
                     .GetNativeTableClient()
                     .CreateSession()
                     .GetValueSync()
                     .GetSession();

  auto transaction =
      session.BeginTransaction(NYdb::NTable::TTxSettings::SnapshotRO())
          .GetValueSync()
          .GetTransaction();

  auto revision = revision_service_.RevisionGet(transaction);
  watchStreamData.CreateWatch(watchId, watchRequest, revision);

  std::ostringstream query;
  query << R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $revision AS Int64;
DECLARE $key AS String;
)";
  if (compare.second) {
    query << R"(
            DECLARE $range_end AS String;)";
  }

  if (std::all_of(request.filters().begin(), request.filters().end(),
                  [](int i) {
                    return i != ::etcdserverpb::WatchCreateRequest_FilterType::
                                    WatchCreateRequest_FilterType_NOPUT;
                  })) {
    query << R"(
$put = (
    SELECT create_revision, key, mod_revision as mod_rev, value, version FROM kv
WHERE )" << compare.first
          << R"(
AND mod_revision >= $revision
UNION ALL
    SELECT delete_revision as delete_revision, create_revision, key, mod_revision as mod_rev, value, version FROM kv_past
WHERE )" << compare.first
          << R"(
AND delete_revision >= $revision
AND deleted == FALSE
);
)";
    if (request.prev_kv()) {
      query << R"(
SELECT p.delete_revision as delete_revision, p.create_revision as create_revision, p.key as key, p.mod_rev as mod_rev, p.value as value, p.version as version,
kv_past.delete_revision, kv_past.create_revision, kv_past.key, kv_past.mod_revision, kv_past.value, kv_past.version
FROM $put AS p
LEFT JOIN kv_past
ON kv_past.key = p.key AND kv_past.version = p.version - 1 AND p.mod_rev = kv_past.delete_revision
ORDER BY mod_rev
;
)";
    } else {
      query << "SELECT * from $put ORDER BY mod_rev;\n";
    }
  } else {
    query << "SELECT 1 LIMIT 0;\n";
  }
  if (std::all_of(request.filters().begin(), request.filters().end(),
                  [](int i) {
                    return i != ::etcdserverpb::WatchCreateRequest_FilterType::
                                    WatchCreateRequest_FilterType_NODELETE;
                  })) {
    query << R"(
SELECT delete_revision, create_revision, key, mod_revision as mod_rev, value, version FROM kv_past
    WHERE )"
          << compare.first << R"(
    AND delete_revision >= $revision
    AND deleted == TRUE
ORDER BY delete_revision;
)";
  } else {
    query << "SELECT 1 LIMIT 0;\n";
  }
  LOG_DEBUG() << query.str();

  auto builder = table_client_provider_.getTableClient()
                     .GetNativeTableClient()
                     .GetParamsBuilder();
  builder.AddParam("$revision").Int64(request.start_revision()).Build();
  builder.AddParam("$key").String(request.key()).Build();
  builder.AddParam("$range_end").String(request.range_end()).Build();

  auto response =
      session
          .ExecuteDataQuery(query.str(),
                            NYdb::NTable::TTxControl::Tx(transaction).CommitTx(),
                            builder.Build())
          .GetValueSync();

  if (response.GetResultSets().size() != 2) {
    throw std::runtime_error("Expected 2 cursors in db response");
  }
  auto putsCursor = response.GetResultSetParser(0);
  auto deletesCursor = response.GetResultSetParser(1);
  bool readSuccessPutsCursor = putsCursor.TryNextRow();
  bool readSuccessDeletesCursor = deletesCursor.TryNextRow();

  int64_t delete_revision = -1;
  int64_t mod_rev = -1;
  google::protobuf::Arena arena;
  auto watchResponse =
      google::protobuf::Arena::CreateMessage<etcdserverpb::WatchResponse>(
          &arena);
  watchResponse->set_watch_id(watchId);
  watchResponse->mutable_header()->set_revision(revision.Revision);
  while (readSuccessPutsCursor || readSuccessDeletesCursor) {
    bool put = false;
    if (!readSuccessDeletesCursor) {
      put = true;
      if (mod_rev == -1) {
        mod_rev = (putsCursor.ColumnParser("mod_rev")).GetInt64();
      }
    } else if (!readSuccessPutsCursor) {
      put = false;
      if (delete_revision == -1) {
        delete_revision =
            (deletesCursor.ColumnParser("delete_revision")).GetInt64();
      }
    } else {
      if (mod_rev == -1) {
        mod_rev = (putsCursor.ColumnParser("mod_rev")).GetInt64();
      }
      if (delete_revision == -1) {
        delete_revision =
            (deletesCursor.ColumnParser("delete_revision")).GetInt64();
      }
      put = mod_rev < delete_revision;
    }

    if (put) {
      mvccpb::Event event = mvccpb::Event();
      event.set_type(mvccpb::Event_EventType_PUT);
      event.mutable_kv()->set_key(putsCursor.ColumnParser("key").GetString());
      event.mutable_kv()->set_value(
          putsCursor.ColumnParser("value").GetString());
      event.mutable_kv()->set_create_revision(
          putsCursor.ColumnParser("create_revision").GetInt64());
      event.mutable_kv()->set_mod_revision(mod_rev);
      event.mutable_kv()->set_version(
          putsCursor.ColumnParser("version").GetInt64());
      if (request.prev_kv()) {
        auto pastKey =
            putsCursor.ColumnParser("kv_past.key").GetOptionalString();
        if (pastKey.has_value()) {
          event.mutable_prev_kv()->set_key(pastKey.value());
          event.mutable_prev_kv()->set_value(
              putsCursor.ColumnParser("kv_past.value").GetString());
          event.mutable_prev_kv()->set_create_revision(
              putsCursor.ColumnParser("kv_past.create_revision").GetInt64());
          event.mutable_prev_kv()->set_mod_revision(
              putsCursor.ColumnParser("kv_past.mod_revision").GetInt64());
          event.mutable_prev_kv()->set_version(
              putsCursor.ColumnParser("kv_past.version").GetInt64());
        }
      }
      watchResponse->mutable_events()->Add(std::move(event));
      mod_rev = -1;
      readSuccessPutsCursor = putsCursor.TryNextRow();
    } else {
      mvccpb::Event event = mvccpb::Event();
      event.set_type(mvccpb::Event_EventType_DELETE);
      if (request.prev_kv()) {
        event.mutable_kv()->set_key(
            deletesCursor.ColumnParser("key").GetString());
        event.mutable_kv()->set_value(
            deletesCursor.ColumnParser("value").GetString());
        event.mutable_kv()->set_create_revision(
            deletesCursor.ColumnParser("create_revision").GetInt64());
        event.mutable_kv()->set_mod_revision(
            deletesCursor.ColumnParser("mod_revision").GetInt64());
        event.mutable_kv()->set_version(
            deletesCursor.ColumnParser("version").GetInt64());
      }
      watchResponse->mutable_events()->Add(std::move(event));
      delete_revision = -1;
      readSuccessPutsCursor = deletesCursor.TryNextRow();
    }
  }
  watchStreamData.stream.Write(std::move(*watchResponse));
}

void WatchService::ProcessMessage(WatchStreamData& watchStreamData) {
  LOG_DEBUG() << "Handle DataReceivedEvent [MessagesCount="
              << watch_sync_provider_.message_to_process.size() << "]";

  std::unique_lock<userver::engine::SharedMutex> lock(watchStreamData.processMessageMutex);

  for (auto& watch : watchStreamData.watches) {
    for (auto& message : watch_sync_provider_.message_to_process) {

      auto messageList = nlohmann::json::parse(message.GetData());
      LOG_DEBUG() << "TUT";

      for (auto& json : messageList) {
        LOG_DEBUG() << "TUT";
        std::string key = to_string(json["key"][0]);
        LOG_DEBUG() << "TUT";

        key = Base64Decode(key.substr(1, key.size() - 2));
        auto eventType = json.contains("update")
                             ? mvccpb::Event_EventType_PUT
                             : mvccpb::Event_EventType_DELETE;
        int64_t revision;
        LOG_DEBUG() << "TUT";

        if (json.contains("newImage")) {
          LOG_DEBUG() << "TUT";

          revision = json["newImage"]["mod_revision"];
          LOG_DEBUG() << "TUT";

        } else {
          std::ostringstream query;
          query << R"(
PRAGMA TablePathPrefix("/Root/test/.etcd");

DECLARE $revision AS Int64;
DECLARE $key AS String;

SELECT delete_revision FROM kv_past
WHERE key == $key
AND mod_revision = $revision;
)";
          int64_t mod_rev = json["oldImage"]["mod_revision"];

          auto session = table_client_provider_.getTableClient()
                             .GetNativeTableClient()
                             .CreateSession()
                             .GetValueSync();

          auto builder = table_client_provider_.getTableClient()
                             .GetNativeTableClient()
                             .GetParamsBuilder();
          builder.AddParam("$revision").Uint64(mod_rev).Build();
          builder.AddParam("$key").String(key).Build();

          auto response =
              session.GetSession()
                  .ExecuteDataQuery(query.str(),
                                    NYdb::NTable::TTxControl::BeginTx(
                                        NYdb::NTable::TTxSettings::SnapshotRO())
                                        .CommitTx(),
                                    builder.Build())
                  .GetValueSync();

          if (response.GetResultSets().size() != 1) {
            throw std::runtime_error("Expected 1 cursors in db response");
          }
          revision = response.GetResultSetParser(0)
                         .ColumnParser("delete_revision")
                         .GetInt64();
        }

        if (!validateMessage(key, eventType, revision,
                             watchStreamData.expected_revisions[watch.first],
                             watch.second.create_request())) {
          continue;
        }

        google::protobuf::Arena arena;

        auto watchResponse =
            google::protobuf::Arena::CreateMessage<etcdserverpb::WatchResponse>(
                &arena);
        watchResponse->set_watch_id(watch.first);
        mvccpb::Event event = mvccpb::Event();
        watchResponse->mutable_header()->set_revision(revision);
        if (eventType == mvccpb::Event_EventType_PUT) {
          auto image = json["newImage"];

          std::string value = to_string(image["value"]);
          value = Base64Decode(value.substr(1, value.size() - 2));

          event.set_type(eventType);
          event.mutable_kv()->set_key(key);
          event.mutable_kv()->set_value(value);
          event.mutable_kv()->set_create_revision(image["create_revision"]);
          event.mutable_kv()->set_mod_revision(image["mod_revision"]);
          event.mutable_kv()->set_version(image["version"]);
        } else {
          event.set_type(eventType);
          event.mutable_kv()->set_key(key);
          event.mutable_kv()->set_mod_revision(revision);
        }
        if (watch.second.create_request().prev_kv()) {
          auto oldImage = json["oldImage"];
          std::string value = to_string(oldImage["value"]);
          value = Base64Decode(value.substr(1, value.size() - 2));
          event.mutable_prev_kv()->set_key(key);
          event.mutable_prev_kv()->set_value(value);
          event.mutable_prev_kv()->set_create_revision(
              oldImage["create_revision"]);
          event.mutable_prev_kv()->set_mod_revision(oldImage["mod_revision"]);
          event.mutable_prev_kv()->set_version(oldImage["version"]);
        }
        watchResponse->mutable_events()->Add(std::move(event));
        watchStreamData.Send(watchResponse);
      }
    }
  }
}
bool WatchService::validateMessage(
    const std::string& key, const mvccpb::Event_EventType& event,
    const int64_t mod_revision, const int64_t expected_revision,
    const etcdserverpb::WatchCreateRequest& request) {
  return (request.start_revision() == 0 ||
          request.start_revision() <= mod_revision) &&
         expected_revision <= mod_revision &&
         KvUtil::CompareKeys(request.key(), request.range_end(), key) &&
         std::all_of(request.filters().begin(), request.filters().end(),
                     [&event](int i) { return i != event; });
}

WatchService::WatchResult WatchService::ProcessWatchRequest(
    userver::ugrpc::server::CallContext& context,
    WatchStreamData& watchStreamData) {
  while (true) {
    auto watchRequest = ::etcdserverpb::WatchRequest();
    try {
      bool success = watchStreamData.stream.Read(watchRequest);
      if (!success) {
        return WatchService::WatchResult{grpc::Status::CANCELLED};
      }
    } catch (std::exception& e) {
      LOG_INFO() << e.what();
      return WatchService::WatchResult{grpc::Status::CANCELLED};
    }
    if (!CheckRequest(watchRequest).ok()) {
      google::protobuf::Arena arena;

      auto watchResponse =
          google::protobuf::Arena::CreateMessage<etcdserverpb::WatchResponse>(
              &arena);

      watchResponse->set_canceled(true);
      if (watchRequest.has_create_request()) {
        watchResponse->set_created(true);
      }
      watchResponse->set_watch_id(-1);
      watchStreamData.stream.Write(std::move(*watchResponse));
      continue;
    }
    if (watchRequest.has_cancel_request()) {
      google::protobuf::Arena arena;
      auto watchResponse =
          google::protobuf::Arena::CreateMessage<etcdserverpb::WatchResponse>(
              &arena);

      watchResponse->set_watch_id(watchRequest.cancel_request().watch_id());
      watchResponse->set_created(false);
      watchResponse->set_canceled(true);
      watchStreamData.DeleteWatcher(watchRequest.cancel_request().watch_id());
      watchStreamData.stream.Write(std::move(*watchResponse));
    }

    if (watchRequest.has_create_request()) {
      auto revision = revision_service_.RevisionGetCached();
      int watch_id = 0;
      if (watchRequest.create_request().watch_id() != 0) {
        if (watchStreamData.watches.contains(
                watchRequest.create_request().watch_id())) {
          // TODO handle
        }
        watch_id = watchRequest.create_request().watch_id();
      } else {
        while (watchStreamData.watches.contains(
            ++watchStreamData.lastProcessedWatchId))
          ;
        watch_id = watchStreamData.lastProcessedWatchId;
      }
      if (watchRequest.create_request().start_revision() != 0 &&
          revision.CompactRevision >
              watchRequest.create_request().start_revision()) {
        google::protobuf::Arena arena;
        auto watchResponse =
            google::protobuf::Arena::CreateMessage<etcdserverpb::WatchResponse>(
                &arena);
        watchResponse->set_compact_revision(revision.CompactRevision);
        watchStreamData.stream.Write(std::move(*watchResponse));
        continue;
      }
      google::protobuf::Arena arena;

      auto watchResponse =
          google::protobuf::Arena::CreateMessage<etcdserverpb::WatchResponse>(
              &arena);

      watchResponse->set_watch_id(watch_id);
      watchResponse->set_created(true);
      watchResponse->set_canceled(false);
      watchResponse->mutable_header()->set_revision(revision.Revision);
      watchStreamData.stream.Write(std::move(*watchResponse));
      if (watchRequest.create_request().start_revision() != 0 &&
          watchRequest.create_request().start_revision() <= revision.Revision) {
        try {
          SendHistoricalKeysChanges(watchStreamData, watchRequest, watch_id);

        } catch (std::exception& e) {
          LOG_ERROR() << e.what();
          return WatchService::WatchResult{grpc::Status::CANCELLED};
        }
      } else {
        watchStreamData.CreateWatch(watch_id, watchRequest, revision);
      }
    }

    if (watchRequest.has_progress_request()) {
      SendPings(watchStreamData);
    }
  }
}

WatchService::WatchResult WatchService::Watch(CallContext& context,
                                              WatchReaderWriter& stream) {
  std::unordered_map<int, ::etcdserverpb::WatchRequest> watches;
  WatchStreamData watchStreamData(watches, stream, watch_sync_provider_);

  userver::engine::TaskWithResult<WatchService::WatchResult>
      watchRequestsHandleTask =
          userver::utils::CriticalAsync("watch-requests-handle-task", [&] {
            return ProcessWatchRequest(context, watchStreamData);
          });
  int64_t id = watch_sync_provider_.GetGlobalWatcherId();
  watch_sync_provider_.AddNewWatcher(id);
  try {
    while (true) {
      if (watchRequestsHandleTask.IsFinished()) {
        watch_sync_provider_.DeleteWatcher(id);
        return watchRequestsHandleTask.Get();
      }
      auto batchId = watch_sync_provider_.WatcherWaitForEvent(
          watchStreamData.lastProcessedBatchId);
      if (batchId != watchStreamData.lastProcessedBatchId) {
        ProcessMessage(watchStreamData);
        watchStreamData.lastProcessedBatchId = batchId;
        watch_sync_provider_.EndProcessing(id);
      } else {
        SendPings(watchStreamData);
      }
    }
  } catch (std::exception& e) {
    LOG_ERROR() << e.what();

    watch_sync_provider_.DeleteWatcher(id);
    return WatchService::WatchResult{grpc::Status::CANCELLED};
  }
}

WatchService::WatchService(
    const userver::components::ComponentConfig& config,
    const userver::components::ComponentContext& component_context)
    : etcdserverpb::WatchBase::Component(config, component_context),
      table_client_provider_(
          component_context.FindComponent<EtcdTableClientProvider>()),
      revision_service_(component_context.FindComponent<RevisionService>()),
      watch_sync_provider_(
          component_context.FindComponent<WatchSynchronizationProvider>()) {
  std::string consumer = "watch-api-consumer";
  std::vector<std::string> topics = {"/Root/test/.etcd/watch-stream"};
  auto topic_reader = std::make_unique<etcd_ydb_userver::TopicReader>(
      table_client_provider_.getTopicClient(), consumer, topics,
      std::chrono::milliseconds(200), watch_sync_provider_);

  read_task_ = userver::utils::CriticalAsync(
      config.Name() + "-read-task",
      [topic_reader = std::move(topic_reader)] { topic_reader->Run(); });
  timer_task_ =
      userver::utils::CriticalAsync(config.Name() + "-timer-task", [&] {
        while (true) {
          userver::engine::SleepFor(std::chrono::milliseconds(30000));
          watch_sync_provider_.NotifyAllWatchersByTimer();
        }
      });
}
void WatchService::SendPings(WatchStreamData& watchStreamData) {
  auto revision = revision_service_.RevisionGetCached();
  LOG_INFO() << watchStreamData.updated.size();
  for (auto& watch : watchStreamData.watches) {
    if (!watchStreamData.updated.contains(watch.first) &&
        watch.second.create_request().progress_notify()) {
      google::protobuf::Arena arena;

      auto watchResponse =
          google::protobuf::Arena::CreateMessage<etcdserverpb::WatchResponse>(
              &arena);
      watchResponse->set_watch_id(watch.first);
      watchResponse->mutable_header()->set_revision(revision.Revision);
      watchStreamData.Send(watchResponse);
    }
  }

  watchStreamData.updated.clear();
}

void AppendWatchService(userver::components::ComponentList& component_list) {
  component_list.Append<WatchService>();
}

}  // namespace etcd_ydb_userver
