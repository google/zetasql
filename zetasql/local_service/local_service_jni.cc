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

#if defined(__MACH__)
#include <fcntl.h>
#endif

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

void strerror_r_ext(int err, char *str, size_t str_len)
{
    if (err < sys_nerr) {
        snprintf(str, str_len, "%s", sys_errlist[err]);
    } else {
        snprintf(str, str_len, "Unknown error %d", err);
    }
}

static void ErrnoSocketException(JNIEnv* env) {
  char buf[128];
  strerror_r_ext(errno, buf, sizeof(buf));

  jclass e = env->FindClass("java/net/SocketException");
  if (e == nullptr) {
    return;
  }
  env->ThrowNew(e, buf);
}

static int GetSocketFd(JNIEnv* env) {
  int sv[2];

#if defined(__MACH__)
  int flags = SOCK_STREAM;
#else
  int flags = SOCK_STREAM | SOCK_NONBLOCK;
#endif

  if (socketpair(AF_UNIX, flags, 0, sv) < 0) {
    ErrnoSocketException(env);
    return -1;
  }

#if defined(__MACH__)
  fcntl(sv[0], F_SETFL, O_NONBLOCK);
  fcntl(sv[1], F_SETFL, O_NONBLOCK);
#endif

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

}  // namespace

jobject GetSocketChannel(JNIEnv* env) {
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

extern "C" JNIEXPORT jint JNICALL JNI_OnLoad_zetasql_local_service(JavaVM* vm,
                                                           void* reserved) {
  JNIEnv* env = nullptr;
  if (vm->GetEnv((void**)&env, JNI_VERSION_1_1) != JNI_OK) {
    return -1;
  }

  jclass system = env->FindClass("java/lang/System");
  if (system == nullptr) {
    return -1;
  }

  jmethodID gp = env->GetStaticMethodID(
      system, "getProperty", "(Ljava/lang/String;)Ljava/lang/String;");
  if (gp == nullptr) {
    return -1;
  }

  jstring classname = (jstring)env->CallStaticObjectMethod(
      system, gp, env->NewStringUTF("zetasql.local_service.class"));
  if (classname == nullptr) {
    return -1;
  }

  const char* classnamestr = env->GetStringUTFChars(classname, nullptr);
  jclass clazz = env->FindClass(classnamestr);
  env->ReleaseStringUTFChars(classname, classnamestr);
  classnamestr = nullptr;
  if (clazz == nullptr) {
    return -1;
  }

  static JNINativeMethod methods[] = {
      {(char*)"getSocketChannel", (char*)"()Ljava/nio/channels/SocketChannel;",
       (void*)GetSocketChannel},
  };
  if (env->RegisterNatives(clazz, methods,
                           sizeof(methods) / sizeof(JNINativeMethod)) !=
      JNI_OK) {
    return -1;
  }

  return env->GetVersion();
}

extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
  return JNI_OnLoad_zetasql_local_service(vm, reserved);
}

}  // namespace local_service
}  // namespace zetasql
