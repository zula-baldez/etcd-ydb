# grpc_etcd-ydb-userver

Implementation of etcd api over YDB


## Dependencies

<pre>      
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt-get -y update
sudo apt-get -y install git cmake ninja-build libidn11-dev ragel yasm protobuf-compiler \
protobuf-compiler-grpc libprotobuf-dev libgrpc++-dev libgrpc-dev libgrpc++1 libgrpc10 \
rapidjson-dev zlib1g-dev libxxhash-dev libzstd-dev libsnappy-dev libgtest-dev libgmock-dev \
libbz2-dev libdouble-conversion-dev libstdc++-13-dev nlohmann-json3-dev
wget https://apt.llvm.org/llvm.sh
chmod u+x llvm.sh
sudo ./llvm.sh 16
sudo ln -sf /usr/bin/clang-16 /usr/bin/clang
sudo ln -sf /usr/bin/clang++-16 /usr/bin/clang++
wget https://ftp.gnu.org/pub/gnu/libiconv/libiconv-1.15.tar.gz
tar -xvzf libiconv-1.15.tar.gz
cd libiconv-1.15
./configure --prefix=/usr/local
make
sudo make install
cd ../
wget -O base64-0.5.2.tar.gz https://github.com/aklomp/base64/archive/refs/tags/v0.5.2.tar.gz
tar -xvzf base64-0.5.2.tar.gz && cd base64-0.5.2
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
sudo cmake --build . --config Release --target install
cd ../../
wget -O brotli-1.1.0.tar.gz https://github.com/google/brotli/archive/refs/tags/v1.1.0.tar.gz
tar -xvzf brotli-1.1.0.tar.gz && cd brotli-1.1.0
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
sudo cmake --build . --config Release --target install
cd ../../
wget -O jwt-cpp-0.7.0.tar.gz https://github.com/Thalhammer/jwt-cpp/archive/refs/tags/v0.7.0.tar.gz
tar -xvzf jwt-cpp-0.7.0.tar.gz && cd jwt-cpp-0.7.0
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
sudo cmake --build . --config Release --target install
cd ../../
(V=4.8.1; curl -L https://github.com/ccache/ccache/releases/download/v${V}/ccache-${V}-linux-x86_64.tar.xz | \
sudo tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-${V}-linux-x86_64/ccache)
sudo rm -rf llvm.sh libiconv-1.15.tar.gz base64-0.5.2.tar.gz brotli-1.1.0.tar.gz jwt-cpp-0.7.0.tar.gz \
libiconv-1.15 base64-0.5.2 brotli-1.1.0 jwt-cpp-0.7.0
</pre>
## Build
Run in etcd-ydb-userver folder

`cmake -DCMAKE_BUILD_TYPE=Release --preset release`

Run in build folder

`cmake --build . --target etcd-ydb-userver -j 14`
## Run
Run compiled etcd-ydb-userver:

`./etcd-ydb-userver  --config ../configs/static_config.yaml --config_vars ../configs/config_vars.yaml`