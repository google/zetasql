//
// Copyright 2019 ZetaSQL Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "zetasql/local_service/local_service_jni.h"

#include <errno.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_posix.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <memory>

#include "zetasql/local_service/local_service_grpc.h"

namespace zetasql {
namespace local_service {
namespace {

static grpc::Server* GetServer() {
  static grpc::Server* server = []() {
    // The service must remain for the lifetime of the server.
    static ZetaSqlLocalServiceGrpcImpl* service =
        new ZetaSqlLocalServiceGrpcImpl();
    grpc::ServerBuilder builder;
    builder.RegisterService(service);
    return builder.BuildAndStart().release();
  }();
  return server;
}

static void ErrnoSocketException(JNIEnv* env) {
  char buf[128];
  char* outstr = strerror_r(errno, buf, sizeof(buf));

  jclass e = env->FindClass("java/net/SocketException");
  if (e == nullptr) {
    return;
  }
  env->ThrowNew(e, outstr);
}

static int GetSocketFd(JNIEnv* env) {
  int sv[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv) < 0) {
    ErrnoSocketException(env);
    return -1;
  }

  // gRPC takes ownership of the first fd
  grpc::AddInsecureChannelFromFd(GetServer(), sv[0]);
  return sv[1];
}

static jobject WrapFileDescriptor(JNIEnv* env, const int fd) {
  jclass ioutil = env->FindClass("sun/nio/ch/IOUtil");
  if (ioutil == nullptr) {
    return nullptr;
  }

  jmethodID newfd =
      env->GetStaticMethodID(ioutil, "newFD", "(I)Ljava/io/FileDescriptor;");
  if (newfd == nullptr) {
    return nullptr;
  }

  jobject javafd = env->CallStaticObjectMethod(ioutil, newfd, fd);
  if (javafd == nullptr) {
    return nullptr;
  }
  return javafd;
}

extern "C" JNIEXPORT jobject JNICALL
Java_com_google_zetasql_JniChannelProvider_getSocketChannel(
    JNIEnv* env) {
  jclass impl = env->FindClass("sun/nio/ch/SocketChannelImpl");
  if (impl == nullptr) {
    return nullptr;
  }

  jmethodID constructor =
      env->GetMethodID(impl, "<init>",
                       "(Ljava/nio/channels/spi/SelectorProvider;"
                       "Ljava/io/FileDescriptor;"
                       "Ljava/net/InetSocketAddress;)V");
  if (constructor == nullptr) {
    return nullptr;
  }

  int fd = GetSocketFd(env);
  if (fd == -1) {
    return nullptr;
  }

  jobject jfd = WrapFileDescriptor(env, fd);
  if (jfd == nullptr) {
    close(fd);
    return nullptr;
  }

  // java SocketChannel takes ownership of fd on success.
  jobject sc = env->NewObject(impl, constructor, nullptr, jfd, nullptr);
  if (sc == nullptr) {
    close(fd);
    return nullptr;
  }
  return sc;
}

}  // namespace
}  // namespace local_service
}  // namespace zetasql
