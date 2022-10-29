FROM quay.io/pypa/manylinux2014_x86_64

RUN yum install -y yum-plugin-ovl

RUN yum install -y java-1.8.0-openjdk-devel python3-devel

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk

# Install Bazel
RUN curl -sSLO https://github.com/bazelbuild/bazel/releases/download/4.0.0/bazel-4.0.0-installer-linux-x86_64.sh && \
  chmod +x ./bazel-4.0.0-installer-linux-x86_64.sh && \
  ./bazel-4.0.0-installer-linux-x86_64.sh --prefix=/usr && \
  rm ./bazel-4.0.0-installer-linux-x86_64.sh

VOLUME /zetasql

WORKDIR /zetasql

# Patch for building icu
RUN printf "diff --git a/bazel/icu.BUILD b/bazel/icu.BUILD\n\
index e85da66..e83b23e 100644\n\
--- a/bazel/icu.BUILD\n\
+++ b/bazel/icu.BUILD\n\
@@ -44,6 +44,7 @@ configure_make(\n\
         \"//conditions:default\": {\n\
             \"CXXFLAGS\": \"-fPIC\",  # For JNI\n\
             \"CFLAGS\": \"-fPIC\",  # For JNI\n\
+            \"CXX\": \"g++\",\n\
         },\n\
     }),\n\
     configure_options = [" > /icu.patch

ENTRYPOINT patch bazel/icu.BUILD /icu.patch && \
  bazel build --sandbox_debug --verbose_failures //... && \
  for file in bazel-*; do mv "$file" "$file.bak"; cp -r "$file.bak" "$file"; rm "$file.bak"; done
