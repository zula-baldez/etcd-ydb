#include "batch_processor.h"
namespace etcd_ydb_userver {
static nlohmann::json KeyStateToJson(const KeyState& state) {
  nlohmann::json j;
  j["version"] = state.version;
  j["mod_revision"] = state.mod_revision;
  j["create_revision"] = state.create_revision;
  j["value"] = Base64Encode(state.value);
  return j;
}

static nlohmann::json PairToJson(
    const std::pair<std::optional<KeyState>, std::optional<KeyState>>& p) {
  nlohmann::json j;
  if (p.first.has_value()) {
    j["update"] = "{}";
  } else {
    j["delete"] = "{}";
  }
  if (p.first.has_value()) {
    j["newImage"] = KeyStateToJson(p.first.value());
  }

  if (p.second.has_value()) {
    j["oldImage"] = KeyStateToJson(p.second.value());
  }

  if (p.second.has_value()) {
    j["key"] = {Base64Encode(p.second->key)};
  } else if (p.first.has_value()) {
    j["key"] = {Base64Encode(p.first->key)};
  }

  return j;
}

static nlohmann::json HistoryToJson(
    std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
        history) {
  nlohmann::json j;
  for (auto& update : history) {
    j.push_back(PairToJson(update));
  }
  return j;
}

void BatchProcessor::ProcessSendWatch(
    std::vector<std::pair<std::optional<KeyState>, std::optional<KeyState>>>&
        history,
    NYdb::NTable::TTransaction& transaction) {
  LOG_DEBUG() << "PROCESSING WATCH";
  bool wrote = false;
  std::optional<NYdb::NTopic::TWriteSessionEvent::TEvent> event;
  while (true) {
    event = client_provider_.getWriteSession()->GetEvent(true);
    if (event.has_value()) {
      if (auto* readyEvent = std::get_if<
              NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
        NYdb::NTopic::TWriteMessage writeMessage(
            to_string(HistoryToJson(history)));
        writeMessage.Codec = NYdb::NTopic::ECodec::RAW;
        LOG_DEBUG() << "TUT";

        client_provider_.getWriteSession()->Write(
            std::move(readyEvent->ContinuationToken), std::move(writeMessage),
            &transaction);
        LOG_DEBUG() << "TUT";
        break;
      } else if (auto* closeSessionEvent =
                     std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
        LOG_INFO() << "SESSION CLOSED";
      }
    }
  }
  LOG_DEBUG() << "DONE";
}
}  // namespace etcd_ydb_userver