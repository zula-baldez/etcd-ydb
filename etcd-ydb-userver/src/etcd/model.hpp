#pragma once

#include <userver/ydb/table.hpp>
#include <api/etcdserverpb/rpc_service.usrv.pb.hpp>

namespace etcd_ydb_userver {

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

class KvUtil {
 public:
  [[nodiscard]] static inline std::pair<std::string, bool> Compare(
      const std::string& key, const std::string& rangeEnd) noexcept {
    if (rangeEnd.empty()) {
      return {"key == $key", false};
    } else if (rangeEnd == kEmptyKey || rangeEnd == kEmptyKey2 ||
               rangeEnd == kEmptyKey3) {
      return {"key >= $key", false};
      } else if (rangeEnd == GetPrefix(key)) {
      return {"StartsWith(key, $key)", false};
    } else {
      return {"key BETWEEN $key AND $range_end", true};
    }
  }

  [[nodiscard]] static inline bool CompareKeys(
      const std::string& startRange, const std::string& rangeEnd,
      const std::string& key) noexcept {
    if (rangeEnd.empty()) {
      return key == startRange;
    } else if (rangeEnd == kEmptyKey || rangeEnd == kEmptyKey2 ||
               rangeEnd == kEmptyKey3) {
      return key >= startRange;
    } else if (rangeEnd == GetPrefix(key)) {
      return key.starts_with(startRange);
    } else {
      return key >= startRange && key <= rangeEnd;
    }
  }

 protected:
  static constexpr std::string kEmptyKey = {"\0", 1};
  // TODO seems like deserialization bug, sometimes \0 represented as " "
  static constexpr std::string kEmptyKey2 = " ";
  static constexpr std::string kEmptyKey3 = "\x18";

  [[nodiscard]] static inline std::string GetPrefix(std::string key) noexcept {
    while (!key.empty()) {
      if (static_cast<unsigned char>(key.back()) <
          std::numeric_limits<unsigned char>::max()) {
        char key_back = key.back();
        ++key_back;
        key.back() = key_back;
        return key;
      }
      key.pop_back();
    }
    return kEmptyKey;
  }

};
class GetRevisionResponse {
 public:
  i64 Revision;
  i64 CompactRevision;
};

struct KeyState {
  std::string key;
  std::string value;
  int64_t mod_revision;
  int64_t create_revision;
  std::optional<int64_t> delete_revision;
  int64_t version = 0;
  bool changedViaDeletion = false;

  [[nodiscard]] bool isInitialized() const { return version != 0; }

  [[nodiscard]] bool isDeleted() const { return delete_revision.has_value(); }

  void KeyToDeleted(GetRevisionResponse& revisionResponse) {
    delete_revision = std::optional<int64_t>(revisionResponse.Revision + 1);
  }

  KeyState NewVersion(GetRevisionResponse& revisionResponse, const ::etcdserverpb::PutRequest&  request) {
    return KeyState{key,
                    request.ignore_value() ? value : request.value(),
                    revisionResponse.Revision + 1,
                    create_revision,
                    std::optional<int64_t>(),
                    version + 1};
  }

  static KeyState NewKey(const std::string& newKey, const std::string& newValue,
                         const  GetRevisionResponse& revisionResponse) {

    return KeyState{newKey,
                    newValue,
                    revisionResponse.Revision + 1,
                    revisionResponse.Revision + 1,
                    std::optional<int64_t>(),
                    1};
  }
};

}  // namespace etcd_ydb_userver