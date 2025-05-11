#include "watch_synchronization.h"
int etcd_ydb_userver::WatchSynchronizationProvider::AddNewWatcher(int64_t globalId) {
  auto lock = std::unique_lock<userver::engine::Mutex>(mutex);
  watchers.insert(globalId);
  return batch_id;
}
void etcd_ydb_userver::WatchSynchronizationProvider::DeleteWatcher(int64_t globalId) {
  auto lock = std::unique_lock<userver::engine::Mutex>(mutex);
  watchers.erase(globalId);
  processing_watchers.erase(globalId);
  if (processing_watchers.empty()) {
    cv_writers.NotifyAll();
  }
}
void etcd_ydb_userver::WatchSynchronizationProvider::NotifyAllWatchers(
    std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>&&
        messages) {
  auto lock = std::unique_lock<userver::engine::Mutex>(mutex);
  bool res = cv_writers.Wait(lock, [this] { return processing_watchers.empty(); });
  if (!res) {
    throw std::runtime_error("Consumer task interrupted");
  }
  message_to_process = std::move(messages);
  processing_watchers = watchers;
  batch_id++;
  cv_readers.NotifyAll();
}
void etcd_ydb_userver::WatchSynchronizationProvider::EndProcessing(int64_t globalId) {
  auto lock = std::unique_lock<userver::engine::Mutex>(mutex);
  processing_watchers.erase(globalId);
  if (processing_watchers.empty()) {
    cv_writers.NotifyAll();
  }
}
int64_t etcd_ydb_userver::WatchSynchronizationProvider::WatcherWaitForEvent(int64_t batchId) {
  auto lock = std::unique_lock<userver::engine::Mutex>(mutex);
  if (batch_id != batchId) {
    return batch_id;
  }
  auto res = cv_readers.Wait(lock);
  if (res != userver::engine::CvStatus::kNoTimeout) {
    throw std::runtime_error("Error while waiting for events");
  }
  return batch_id;
}
void etcd_ydb_userver::WatchSynchronizationProvider::
    NotifyAllWatchersByTimer() {
  auto lock = std::unique_lock<userver::engine::Mutex>(mutex);
  cv_readers.NotifyAll();
}
int64_t etcd_ydb_userver::WatchSynchronizationProvider::GetGlobalWatcherId() {
  auto lock = std::unique_lock<userver::engine::Mutex>(mutex);
  return max_id_++;
}
void etcd_ydb_userver::AppendWatchSynchronizationProvider(
    userver::components::ComponentList& component_list) {
  component_list.Append<etcd_ydb_userver::WatchSynchronizationProvider>();
}
