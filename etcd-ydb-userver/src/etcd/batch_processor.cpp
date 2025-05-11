#include "batch_processor.h"
#include <thread>
namespace etcd_ydb_userver {

void BatchProcessor::ProcessPrevKvs(
    std::unordered_map<std::string, KeyState>& prevKvs, const Variant& req,
    std::optional<std::reference_wrapper<userver::engine::Mutex>>
        keyBucketMutex,
    NYdb::NTable::TSession& session) {
  std::visit(
      [&](auto&& request) {
        ProcessPrevKvs(prevKvs, request, keyBucketMutex, session);
      },
      req);
}

ResponseVariant BatchProcessor::ProcessRequest(
    std::unordered_map<std::string, KeyState>& prevKvs, const Variant& req,
    std::unordered_map<std::string, KeyState>& changedKeys,
    std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
        history,
    std::vector<KeyState>& deleted, GetRevisionResponse& revision) {
  return std::visit(
      [&](auto&& request) {
        return ProcessRequest(prevKvs, request, changedKeys, history, deleted,
                              revision);
      },
      req);
}

void BatchProcessor::UpdateRevision(GetRevisionResponse& revision,
                                    NYdb::NTable::TTransaction& transaction) {
  revision_service_.RevisionSet(revision.Revision, revision.CompactRevision,
                                transaction);
}

void BatchProcessor::Run(int64_t delay) {
  while (true) {
    try {
      auto lock = std::unique_lock<userver::engine::Mutex>(mutex);
      cv_writers.WaitFor(lock, std::chrono::milliseconds(delay),
                         [&]() { return waiting_queue_.size() == 10; });
      if (waiting_queue_.empty()) {
        continue;
      }
      processing_queue_ = std::move(waiting_queue_);
      waiting_queue_.clear();
      responses.clear();
      isProcessing = true;

      std::unordered_map<std::string, KeyState> changedKeys;  // global state
      std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>
          history;
      std::vector<KeyState> deleted;

      std::vector<std::unordered_map<std::string, KeyState>> prevKvs(
          processing_queue_.size());  // prev kvs for each request
      std::vector<userver::engine::TaskWithResult<void>> tasks(
          processing_queue_.size());

      auto session = client_provider_.getTableClient()
                         .GetNativeTableClient()
                         .CreateSession()
                         .GetValueSync()
                         .GetSession();

      for (size_t i = 0; i < processing_queue_.size(); i++) {
        const Variant var = std::visit([](auto& val) -> Variant { return val; },
                                       processing_queue_[i]);
        tasks[i] = userver::utils::CriticalAsync(
            "prev-kv-read-task", [&session, var, &prevKvs, i, this] {
              return ProcessPrevKvs(prevKvs[i], var, {}, session);
            });
      }

      for (auto& task : tasks) {
        task.BlockingWait();
      }
      auto transaction =
          session.BeginTransaction(NYdb::NTable::TTxSettings::SerializableRW())
              .GetValueSync()
              .GetTransaction();

      auto revision = revision_service_.RevisionGet(transaction);
      for (size_t i = 0; i < processing_queue_.size(); i++) {
        const Variant var = std::visit([](auto& val) -> Variant { return val; },
                                       processing_queue_[i]);

        auto resp = ProcessRequest(prevKvs[i], var, changedKeys, history,
                                   deleted, revision);
        responses.emplace_back(std::move(resp));
      }
      ProcessDeleteKv(changedKeys, transaction);
      ProcessPutKv(changedKeys, transaction);
      ProcessPutPrevKv(deleted, transaction);
      UpdateRevision(revision, transaction);

      ProcessSendWatch(history, transaction);
      LOG_DEBUG() <<  "YAAAAAAAAAAAAAAAAAAAAAA";
      transaction.Commit().GetValueSync();
      LOG_DEBUG() <<  "TUT";

      processing_queue_.clear();
      isProcessing = false;
      cv_readers.NotifyAll();

    } catch (std::exception& e) {
      LOG_ERROR() << e.what();
      processing_queue_.clear();
      isProcessing = false;
      cv_readers.NotifyAll();
    }
  }
}
void AppendBatchProcessor(userver::components::ComponentList& component_list) {
  component_list.Append<etcd_ydb_userver::BatchProcessor>();
}

}  // namespace etcd_ydb_userver
