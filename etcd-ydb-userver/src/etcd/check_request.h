#pragma once
#include <api/etcdserverpb/rpc_service.usrv.pb.hpp>
#include "model.hpp"

#include <grpcpp/support/status.h>

namespace etcd_ydb_userver {

grpc::Status CheckRequest(const etcdserverpb::RangeRequest& request);
grpc::Status CheckRequest(const etcdserverpb::PutRequest& request);
grpc::Status CheckRequest(const etcdserverpb::DeleteRangeRequest& request);
grpc::Status CheckRequest(const etcdserverpb::TxnRequest& request);
grpc::Status CheckRequest(const etcdserverpb::CompactionRequest& request);
grpc::Status CheckRequest(const etcdserverpb::WatchRequest& request);

}  // namespace etcd_ydb_userver
