FROM l.gcr.io/google/bazel:1.0.0 as builder

# Use gcc because clang can't build m4
RUN apt-get update && \
    apt-get install build-essential software-properties-common -y && \
    add-apt-repository ppa:ubuntu-toolchain-r/test -y && \
    apt-get update && \
    # Use gcc-9 for using std::filesystem api
    apt-get install --no-install-recommends -y make gcc-9 g++-9 && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 900 \
        --slave /usr/bin/g++ g++ /usr/bin/g++-9
ENV CC /usr/bin/gcc
COPY . /work/zetasql
WORKDIR /work/zetasql

RUN bazel build //zetasql/experimental:format && \
    cp ./bazel-bin/zetasql/experimental/format .


FROM debian:bullseye-slim
COPY --from=builder /work/zetasql/bazel-bin/zetasql/experimental/format /usr/bin/format
WORKDIR /home
ENTRYPOINT [ "format" ]
CMD [ "." ]
