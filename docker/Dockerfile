# syntax=docker/dockerfile:experimental
FROM l.gcr.io/google/bazel:1.0.0
# use gcc because clang can't build m4
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt-get update && \
    apt-get install build-essential software-properties-common -y && \
    add-apt-repository ppa:ubuntu-toolchain-r/test -y && \
    apt-get update && \
    apt-get install --no-install-recommends -y make gcc-9 g++-9 && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 900 \
        --slave /usr/bin/g++ g++ /usr/bin/g++-9
ENV CC /usr/bin/gcc
COPY . /work/zetasql
WORKDIR /work/zetasql

RUN --mount=type=cache,target=/root/.cache/bazel \
    bazel build \
    --cxxopt="-fpermissive" --cxxopt="-std=gnu++17" ... && \
    find /root/.cache/bazel -name *_jar.jar -exec cp {} /root/.cache/ \;

ENTRYPOINT [ "/bin/bash" ]
