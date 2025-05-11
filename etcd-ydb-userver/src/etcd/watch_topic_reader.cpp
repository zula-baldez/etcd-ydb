#include "watch_topic_reader.h"
namespace etcd_ydb_userver {

class SessionReadTask {
 public:
  using TReadSessionEvent = NYdb::NTopic::TReadSessionEvent;
  using TSessionClosedEvent = NYdb::NTopic::TSessionClosedEvent;

  explicit SessionReadTask(userver::ydb::TopicReadSession&& read_session,
                           etcd_ydb_userver::WatchSynchronizationProvider&
                               watchSynchronizationProvider)
      : read_session_(std::move(read_session)),
        watch_synchronization_provider_(watchSynchronizationProvider) {}

  ~SessionReadTask() {
    if (!session_closed_) {
      read_session_.Close(std::chrono::milliseconds{3000});
    }
  }

  void Run() {
    while (!userver::engine::current_task::ShouldCancel() && !session_closed_) {
      try {
        LOG_INFO() << "Waiting events...";
        auto events = read_session_.GetEvents(10);
        LOG_INFO() << "Handling events...";
        for (auto& event : events) {
          HandleEvent(event);
        }
      } catch (const userver::ydb::OperationCancelledError& /*ex*/) {
        break;
      }
    }
  }

 private:
  void HandleEvent(TReadSessionEvent::TEvent& event) {
    std::visit(
        userver::utils::Overloaded{
            [this](TReadSessionEvent::TDataReceivedEvent& e) {
              NYdb::NTopic::TDeferredCommit deferredCommit;
              deferredCommit.Add(e);

              HandleDataReceivedEvent(e);
              // commit if HandleDataReceivedEvent succeeded
              deferredCommit.Commit();
            },
            [](TReadSessionEvent::TCommitOffsetAcknowledgementEvent&) {
              //
            },
            [](TReadSessionEvent::TStartPartitionSessionEvent& e) {
              LOG_DEBUG() << "Starting partition session [TopicPath="
                          << e.GetPartitionSession()->GetTopicPath()
                          << ", PartitionId="
                          << e.GetPartitionSession()->GetPartitionId() << "]";
              e.Confirm();  // partition assigned
            },
            [](TReadSessionEvent::TStopPartitionSessionEvent& e) {
              LOG_DEBUG() << "Stopping partition session [TopicPath="
                          << e.GetPartitionSession()->GetTopicPath()
                          << ", PartitionId="
                          << e.GetPartitionSession()->GetPartitionId() << "]";
              e.Confirm();  // partition revoked
            },
            [](TReadSessionEvent::TEndPartitionSessionEvent& e) {
              LOG_DEBUG() << "End partition session [TopicPath="
                          << e.GetPartitionSession()->GetTopicPath()
                          << ", PartitionId="
                          << e.GetPartitionSession()->GetPartitionId() << "]";
            },
            [](TReadSessionEvent::TPartitionSessionClosedEvent& e) {
              if (TReadSessionEvent::TPartitionSessionClosedEvent::EReason::
                      StopConfirmedByUser != e.GetReason()) {
                LOG_WARNING()
                    << "Unexpected PartitionSessionClosedEvent [Reason="
                    << userver::utils::UnderlyingValue(e.GetReason()) << "]";
                // partition lost
              }
            },
            [](TReadSessionEvent::TPartitionSessionStatusEvent&) {
              //
            },
            [this](NYdb::NTopic::TSessionClosedEvent& e) {
              LOG_INFO() << "Session closed";
              session_closed_ = true;
              if (!e.IsSuccess()) {
                throw std::runtime_error{"Session closed unsuccessfully: " +
                                         e.GetIssues().ToString()};
              }
            },
        },
        event);
  }

  void HandleDataReceivedEvent(TReadSessionEvent::TDataReceivedEvent& event) {
    LOG_DEBUG() << "Handle DataReceivedEvent [MessagesCount="
                << event.GetMessagesCount() << "]";
    watch_synchronization_provider_.NotifyAllWatchers(
        std::move(event.GetMessages()));
  }

 private:
  userver::ydb::TopicReadSession read_session_;
  etcd_ydb_userver::WatchSynchronizationProvider&
      watch_synchronization_provider_;
  bool session_closed_{false};
};
NYdb::NTopic::TReadSessionSettings TopicReader::ConstructReadSessionSettings(
    const std::string& consumer_name, const std::vector<std::string>& topics) {
  NYdb::NTopic::TReadSessionSettings read_session_settings;
  read_session_settings.ConsumerName(
      userver::ydb::impl::ToString(consumer_name));
  for (const auto& topic_path : topics) {
    read_session_settings.AppendTopics(
        userver::ydb::impl::ToString(topic_path));
  }
  return read_session_settings;
}
void TopicReader::Run() {
  while (!userver::engine::current_task::ShouldCancel()) {
    try {
      LOG_INFO() << "Creating read session...";
      auto read_session =
          topic_client_.CreateReadSession(read_session_settings_);

      LOG_INFO() << "Starting session read...";
      SessionReadTask session_read_task{std::move(read_session),
                                        watch_synchronization_provider};
      session_read_task.Run();

      LOG_INFO() << "Session read finished";
    } catch (const std::exception& ex) {
      LOG_ERROR() << "Session read failed: " << ex;
    }

    userver::engine::InterruptibleSleepFor(restart_session_delay_);
  }
}
}  // namespace etcd_ydb_userver