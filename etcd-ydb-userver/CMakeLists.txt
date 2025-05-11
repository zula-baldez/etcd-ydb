cmake_minimum_required(VERSION 3.12...3.31)
project(etcd-ydb-userver LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(DownloadUserver)

find_package(userver COMPONENTS core grpc QUIET)
if(NOT userver_FOUND)
  # Tries TRY_DIR first, falls back to downloading userver from GitHub using CPM.
  download_userver(TRY_DIR third_party/userver)
endif()

userver_setup_environment()

# Common sources
file(GLOB executables
        "src/etcd/*"
)

add_library(${PROJECT_NAME}_objs OBJECT
    ${executables}
)
target_link_libraries(${PROJECT_NAME}_objs PUBLIC userver::grpc userver-ydb)


# Create a proto library with userver extensions
file(GLOB google_common
        "google/api/*.proto"
)
userver_add_grpc_library(${PROJECT_NAME}_proto PROTOS
        api/etcdserverpb/rpc.proto
        handlers/hello.proto
        api/authpb/auth.proto
        api/mvccpb/kv.proto
        server/etcdserver/api/v3election/v3electionpb/v3election.proto
        server/etcdserver/api/v3lock/v3lockpb/v3lock.proto
        server/lease/leasepb/lease.proto
        ${google_common}
        gogoproto/gogo.proto
)
target_link_libraries(${PROJECT_NAME}_objs PUBLIC ${PROJECT_NAME}_proto)


# The Service
add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_NAME}_objs)

#
#target_link_libraries(${PROJECT_NAME}_unittest PRIVATE ${PROJECT_NAME}_objs userver::utest)
#add_google_tests(${PROJECT_NAME}_unittest)
#
#target_link_libraries(${PROJECT_NAME}_benchmark PRIVATE ${PROJECT_NAME}_objs userver::ubench)
#add_google_benchmark_tests(${PROJECT_NAME}_benchmark)


# Functional Tests
userver_testsuite_add_simple()


# Install
include(GNUInstallDirs)

if(DEFINED ENV{PREFIX})
  message(STATUS "Set install prefix: $ENV{PREFIX}")
  file(TO_CMAKE_PATH "$ENV{PREFIX}" PREFIX_PATH)
  set(CMAKE_INSTALL_PREFIX "${PREFIX_PATH}")
endif()

file(GLOB CONFIGS_FILES ${CMAKE_CURRENT_SOURCE_DIR}/configs/*.yaml ${CMAKE_CURRENT_SOURCE_DIR}/configs/*.json)

install(TARGETS ${PROJECT_NAME} DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT ${PROJECT_NAME})
install(FILES ${CONFIGS_FILES} DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/${PROJECT_NAME} COMPONENT ${PROJECT_NAME})




