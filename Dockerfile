################################################################################
#                                     BUILD                                    #
################################################################################

FROM ubuntu:18.04 as build

# Setup java
RUN apt-get update && apt-get -qq install -y default-jre default-jdk

# Install prerequisites for bazel
RUN apt-get -qq install curl tar build-essential wget python python3 zip unzip

ENV BAZEL_VERSION=7.2.1

RUN apt install apt-transport-https curl gnupg -y
RUN curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg
RUN mv bazel-archive-keyring.gpg /usr/share/keyrings
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list

RUN apt update && apt -qq install -y bazel-${BAZEL_VERSION}
RUN ln -s /usr/bin/bazel-${BAZEL_VERSION} /usr/bin/bazel

RUN apt-get update && DEBIAN_FRONTEND="noninteractive"                         \
    TZ="America/Los_Angeles" apt-get install -y tzdata

# Unfortunately ZetaSQL has issues with clang (default bazel compiler), so
# we install GCC. Also install make for rules_foreign_cc bazel rules.
RUN apt-get -qq install -y software-properties-common make rename  git ca-certificates libgnutls30
RUN apt-get -qq install -y software-properties-common
RUN add-apt-repository ppa:ubuntu-toolchain-r/test                          && \
    apt-get -qq update                                                      && \
    apt-get -qq install -y gcc-11 g++-11 make rename  git                   && \
    apt-get -qq install -y ca-certificates libgnutls30                      && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 90          \
                        --slave   /usr/bin/g++ g++ /usr/bin/g++-11          && \
    update-alternatives --set gcc /usr/bin/gcc-11


# To support fileNames with non-ascii characters
RUN apt-get -qq install locales && locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8

COPY . /zetasql

# Create a new user zetasql to avoid running as root.
RUN useradd -ms /bin/bash zetasql
RUN chown -R zetasql:zetasql /zetasql
USER zetasql

ENV HOME=/home/zetasql
RUN mkdir -p $HOME/bin

# Supported MODE:
# - `build` (default): Builds all ZetaSQL targets.
# - `execute_query`: Installs the `execute_query` tool only. Erases all other
#                    build artifacts.
ARG MODE=build

RUN cd zetasql && ./docker_build.sh $MODE

ENV PATH=$PATH:$HOME/bin

WORKDIR /zetasql
