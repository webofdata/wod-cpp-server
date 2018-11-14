FROM ubuntu as builder

# get tools and deps
RUN apt-get update \
  && apt-get install -y git \
                        g++ \
                        make \
                        wget \
                        build-essential \
                        libsnappy-dev \
                        zlib1g-dev \
                        libbz2-dev \
                        liblz4-dev \
                        libgflags-dev \
                        libzstd-dev

#get and install cmake
RUN cd /home && wget https://cmake.org/files/v3.9/cmake-3.9.0.tar.gz \
  && tar xfz cmake-3.9.0.tar.gz \
  && rm cmake-3.9.0.tar.gz \
  && cd cmake-3.9.0 \
  && ./bootstrap \
  && make \
  && make install \
  && cd /home \
  && rm -rf cmake-3.9.0

# get and install boost
RUN cd /home && wget http://downloads.sourceforge.net/project/boost/boost/1.64.0/boost_1_64_0.tar.gz \
  && tar xfz boost_1_64_0.tar.gz \
  && rm boost_1_64_0.tar.gz \
  && cd boost_1_64_0 \
  && ./bootstrap.sh --prefix=/usr/local \
  && ./b2 install \
  && cd /home \
  && rm -rf boost_1_64_0

# get and install rapidjson
RUN cd /home \
 && mkdir /usr/local/include/rapidjson \
 && git clone https://github.com/miloyip/rapidjson.git \
 && cp -a ./rapidjson/include/rapidjson/. /usr/local/include/rapidjson

# get and install rocksdb
RUN cd /home \
  && cd /home \
  && git clone https://github.com/facebook/rocksdb.git \
  && cd rocksdb \
  && make static_lib

# copy rocksdb shared lib to /usr/local/lib
RUN cd /home/rocksdb \
  && cp librocksdb.a /usr/local/lib

# copy header files for rocksdb
RUN cd /home \
  && mkdir /usr/local/include/rocksdb \
  && cp -a ./rocksdb/include/rocksdb/. /usr/local/include/rocksdb

# get and install simple web server
RUN cd /home \
  && git clone https://github.com/eidheim/Simple-Web-Server.git \
  && mkdir /usr/local/include/simple-web-server \
  && cd Simple-Web-Server \
  && cp server_http.hpp client_http.hpp server_https.hpp client_https.hpp crypto.hpp utility.hpp status_code.hpp /usr/local/include/simple-web-server

RUN mkdir /home/wodsource
COPY . /home/wodsource
RUN mkdir /home/wodsource/linuxbuild
RUN cd /home/wodsource/linuxbuild \
  && cmake -DCMAKE_BUILD_TYPE=Release .. \
  && make
RUN strip /home/wodsource/linuxbuild/wodserver
RUN strip /home/wodsource/linuxbuild/wodservertests

FROM ubuntu
RUN mkdir /tmp/stores
RUN mkdir /tmp/data
RUN mkdir /wod
RUN mkdir /wod/stores
COPY --from=builder /home/wodsource/linuxbuild/wodserver /wod
ENTRYPOINT [ "/wod/wodserver" ]










