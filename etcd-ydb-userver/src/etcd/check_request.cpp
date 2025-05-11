#include "check_request.h"

namespace etcd_ydb_userver {

// kMaxTxnOps is the max operations per txn.
// e.g suppose kMmaxTxnOps = 128.
// Txn.Success can have at most 128 operations,
// and Txn.Failure can have at most 128 operations.
static constexpr auto kMaxTxnOps = 128;

static constexpr std::string kEmptyKey = "";

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

grpc::Status CheckRequest(const etcdserverpb::RangeRequest& request) {
  if (request.key().empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "etcdserver: key is not provided"};
  }

  if (const auto sort_order =
          etcdserverpb::RangeRequest::SortOrder_Name(request.sort_order());
      sort_order.empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "etcdserver: invalid sort option"};
  }

  if (const auto sort_target =
          etcdserverpb::RangeRequest::SortTarget_Name(request.sort_target());
      sort_target.empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "etcdserver: invalid sort option"};
  }

  return {};
}

grpc::Status CheckRequest(const etcdserverpb::PutRequest& request) {
  if (request.key().empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "etcdserver: key is not provided"};
  }

  if (request.ignore_value() && !request.value().empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "etcdserver: value is provided"};
  }

  if (request.ignore_lease() && request.lease() != 0) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "etcdserver: lease is provided"};
  }

  return {};
}

grpc::Status CheckRequest(const etcdserverpb::DeleteRangeRequest& request) {
  if (request.key().empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "etcdserver: key is not provided"};
  }

  return {};
}

grpc::Status CheckRequestOp(const etcdserverpb::RequestOp& operation,
                            int maxTxnOps);

grpc::Status CheckTxnRequest(const etcdserverpb::TxnRequest& request,
                             int maxTxnOps) {
  auto opc = request.compare_size();
  if (opc < request.success_size()) {
    opc = request.success_size();
  }
  if (opc < request.failure_size()) {
    opc = request.failure_size();
  }
  if (opc > maxTxnOps) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "etcdserver: too many operations in txn request"};
  }

  if (const auto compare = request.compare();
      std::any_of(compare.begin(), compare.end(),
                  [](const auto& el) { return el.key().empty(); })) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "etcdserver: key is not provided"};
  }

  for (const auto& req : request.success()) {
    const auto status = CheckRequestOp(req, maxTxnOps - opc);
    if (!status.ok()) {
      return status;
    }
  }

  for (const auto& req : request.failure()) {
    const auto status = CheckRequestOp(req, maxTxnOps - opc);
    if (!status.ok()) {
      return status;
    }
  }

  return {};
}

// checkIntervals tests whether puts and deletes overlap for a list of ops. If
// there is an overlap, returns an error. If no overlap, return put and delete
// sets for recursive evaluation.
std::tuple<grpc::Status, std::vector<std::pair<std::string, std::string>>,
           std::vector<std::pair<std::string, std::string>>,
           std::vector<std::pair<std::string, std::string>>,
           std::vector<std::pair<std::string, std::string>>,
           std::unordered_set<std::string>>
CheckIntervals(
    const google::protobuf::RepeatedPtrField<etcdserverpb::RequestOp>&
        requests) {
  std::vector<std::pair<std::string, std::string>> keyDels;
  std::vector<std::pair<std::string, std::string>> fromKeyDels;
  std::vector<std::pair<std::string, std::string>> prefixKeyDels;
  std::vector<std::pair<std::string, std::string>> rangeDels;
  std::unordered_set<std::string> puts;

  auto compareKey = [](const std::string& putKey, const std::string& key,
                       const std::string&) { return putKey == key; };
  auto compareFromKey = [](const std::string& putKey, const std::string& key,
                           const std::string&) { return putKey >= key; };
  auto comparePrefixKey = [](const std::string& putKey, const std::string& key,
                             const std::string&) {
    return putKey.starts_with(key);
  };
  auto compareRange = [](const std::string& putKey, const std::string& key,
                         const std::string& rangeEnd) {
    return key <= putKey && putKey < rangeEnd;
  };

  auto isRepeatedPut = [&](const std::string& putKey) {
    return puts.contains(putKey) ||
           std::ranges::any_of(
               keyDels,
               [&](const std::pair<std::string, std::string>& range) {
                 return compareKey(putKey, range.first, range.second);
               }) ||
           std::ranges::any_of(
               fromKeyDels,
               [&](const std::pair<std::string, std::string>& range) {
                 return compareFromKey(putKey, range.first, range.second);
               }) ||
           std::ranges::any_of(
               prefixKeyDels,
               [&](const std::pair<std::string, std::string>& range) {
                 return comparePrefixKey(putKey, range.first, range.second);
               }) ||
           std::ranges::any_of(
               rangeDels,
               [&](const std::pair<std::string, std::string>& range) {
                 return compareRange(putKey, range.first, range.second);
               });
  };

  for (const auto& req : requests) {
    if (req.has_request_delete_range()) {
      const auto& request = req.request_delete_range();
      if (request.range_end().empty()) {
        if (std::any_of(
                puts.begin(), puts.end(), [&](const std::string& putKey) {
                  return compareKey(putKey, request.key(), request.range_end());
                })) {
          return {{grpc::StatusCode::INVALID_ARGUMENT,
                   "etcdserver: duplicate key given in txn request"},
                  {},
                  {},
                  {},
                  {},
                  {}};
        }
        keyDels.emplace_back(request.key(), request.range_end());
      } else if (request.range_end() == kEmptyKey) {
        if (std::any_of(puts.begin(), puts.end(),
                        [&](const std::string& putKey) {
                          return compareFromKey(putKey, request.key(),
                                                request.range_end());
                        })) {
          return {{grpc::StatusCode::INVALID_ARGUMENT,
                   "etcdserver: duplicate key given in txn request"},
                  {},
                  {},
                  {},
                  {},
                  {}};
        }
        fromKeyDels.emplace_back(request.key(), request.range_end());
      } else if (request.range_end() == GetPrefix(request.key())) {
        if (std::any_of(puts.begin(), puts.end(),
                        [&](const std::string& putKey) {
                          return comparePrefixKey(putKey, request.key(),
                                                  request.range_end());
                        })) {
          return {{grpc::StatusCode::INVALID_ARGUMENT,
                   "etcdserver: duplicate key given in txn request"},
                  {},
                  {},
                  {},
                  {},
                  {}};
        }
        prefixKeyDels.emplace_back(request.key(), request.range_end());
      } else {
        if (std::any_of(puts.begin(), puts.end(),
                        [&](const std::string& putKey) {
                          return compareRange(putKey, request.key(),
                                              request.range_end());
                        })) {
          return {{grpc::StatusCode::INVALID_ARGUMENT,
                   "etcdserver: duplicate key given in txn request"},
                  {},
                  {},
                  {},
                  {},
                  {}};
        }
        rangeDels.emplace_back(request.key(), request.range_end());
      }
    } else if (req.has_request_put()) {
      const auto& request = req.request_put();
      if (isRepeatedPut(request.key())) {
        return {{grpc::StatusCode::INVALID_ARGUMENT,
                 "etcdserver: duplicate key given in txn request"},
                {},
                {},
                {},
                {},
                {}};
      }
      puts.insert(request.key());
    } else if (req.has_request_txn()) {
      const auto& request = req.request_txn();
      auto [statusThen, keyDelsThen, fromKeyDelsThen, prefixKeyDelsThen,
            delsThen, putsThen] = CheckIntervals(request.success());
      if (!statusThen.ok()) {
        return {statusThen,        keyDelsThen, fromKeyDelsThen,
                prefixKeyDelsThen, delsThen,    putsThen};
      }
      for (const auto& putKey : putsThen) {
        if (isRepeatedPut(putKey)) {
          return {{grpc::StatusCode::INVALID_ARGUMENT,
                   "etcdserver: duplicate key given in txn request"},
                  {},
                  {},
                  {},
                  {},
                  {}};
        }
      }

      auto [statusElse, keyDelsElse, fromKeyDelsElse, prefixKeyDelsElse,
            delsElse, putsElse] = CheckIntervals(request.failure());
      if (!statusElse.ok()) {
        return {statusElse,        keyDelsElse, fromKeyDelsElse,
                prefixKeyDelsElse, delsElse,    putsElse};
      }
      for (const auto& putKey : putsElse) {
        if (isRepeatedPut(putKey)) {
          return {{grpc::StatusCode::INVALID_ARGUMENT,
                   "etcdserver: duplicate key given in txn request"},
                  {},
                  {},
                  {},
                  {},
                  {}};
        }
      }

      keyDels.insert(keyDels.end(), keyDelsThen.begin(), keyDelsThen.end());
      keyDels.insert(keyDels.end(), keyDelsElse.begin(), keyDelsElse.end());

      fromKeyDels.insert(fromKeyDels.end(), fromKeyDelsThen.begin(),
                         fromKeyDelsThen.end());
      fromKeyDels.insert(fromKeyDels.end(), fromKeyDelsElse.begin(),
                         fromKeyDelsElse.end());

      prefixKeyDels.insert(prefixKeyDels.end(), prefixKeyDelsThen.begin(),
                           prefixKeyDelsThen.end());
      prefixKeyDels.insert(prefixKeyDels.end(), prefixKeyDelsElse.begin(),
                           prefixKeyDelsElse.end());

      rangeDels.insert(rangeDels.end(), delsThen.begin(), delsThen.end());
      rangeDels.insert(rangeDels.end(), delsElse.begin(), delsElse.end());

      puts.insert(putsThen.begin(), putsThen.end());
      puts.insert(putsElse.begin(), putsElse.end());
    }
  }

  return {{}, keyDels, fromKeyDels, prefixKeyDels, rangeDels, puts};
}

grpc::Status CheckRequest(const etcdserverpb::TxnRequest& request) {
  if (const auto status = CheckTxnRequest(request, kMaxTxnOps); !status.ok()) {
    return status;
  }

  // check for forbidden put/del overlaps after checking request to avoid
  // quadratic blowup
  if (const auto [status, keyDels, fromKeyDels, prefixKeyDels, rangeDels,
                  puts] = CheckIntervals(request.success());
      !status.ok()) {
    return status;
  }

  if (const auto [status, keyDels, fromKeyDels, prefixKeyDels, rangeDels,
                  puts] = CheckIntervals(request.failure());
      !status.ok()) {
    return status;
  }

  return {};
}

grpc::Status CheckRequest(const etcdserverpb::CompactionRequest&) {
  // There are no checks for CompactionRequest in etcd.
  return {};
}
grpc::Status CheckRequest(const etcdserverpb::WatchRequest& request) {
  if (request.has_create_request()) {
    auto& cr_request = request.create_request();
    auto compare =
        KvUtil::Compare(cr_request.key(), cr_request.range_end()).first;
    if (compare.find("BETWEEN") != std::string::npos &&
        cr_request.key() >= cr_request.range_end()) {
      return grpc::Status::CANCELLED;
    }
  }
  return grpc::Status::OK;
}

grpc::Status CheckRequestOp(const etcdserverpb::RequestOp& operation,
                            int maxTxnOps) {
  if (operation.has_request_range()) {
    return CheckRequest(operation.request_range());
  } else if (operation.has_request_put()) {
    return CheckRequest(operation.request_put());
  } else if (operation.has_request_delete_range()) {
    return CheckRequest(operation.request_delete_range());
  } else if (operation.has_request_txn()) {
    return CheckTxnRequest(operation.request_txn(), maxTxnOps);
  } else {
    return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: key not found"};
  }
}

}  // namespace etcd_ydb_userver
