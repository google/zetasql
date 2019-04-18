/*
 * Copyright 2019 ZetaSQL Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.google.zetasql;

import com.google.auto.service.AutoService;
import io.grpc.Channel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelException;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

/** Controller class of the ZetaSQL JniChannelProvider. */
@AutoService(ClientChannelProvider.class)
public class JniChannelProvider implements ClientChannelProvider {
  private static final InetSocketAddress ADDRESS = new InetSocketAddress(0);
  private static Channel channel = null;

  static {
    try {
      cz.adamh.utils.NativeUtils.loadLibraryFromJar("/zetasql/local_service/liblocal_service_jni.so");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns a SocketChannel connected to the server. */
  private static native SocketChannel getSocketChannel() throws IOException;

  /** Wraps one end of a socketpair for NioSocketChannel. */
  protected static class SocketPairChannel extends NioSocketChannel {

    private final Object stateLock = new Object();
    private boolean calledDoConnect = false;

    private static SocketChannel newSocket() {
      try {
        return getSocketChannel();
      } catch (IOException e) {
        throw new ChannelException("Failed to open a socket.", e);
      }
    }

    public SocketPairChannel() {
      super(newSocket());
    }

    @Override
    public boolean isActive() {
      synchronized (stateLock) {
        return calledDoConnect && super.isActive();
      }
    }

    @Override
    public InetSocketAddress localAddress() {
      return ADDRESS;
    }

    @Override
    public InetSocketAddress remoteAddress() {
      return ADDRESS;
    }

    @Override
    protected boolean doConnect(SocketAddress remoteAddress, SocketAddress localAddress)
        throws Exception {
      if (!ADDRESS.equals(remoteAddress)) {
        throw new IllegalArgumentException("Invalid remoteAddress");
      }
      synchronized (stateLock) {
        calledDoConnect = true;
      }
      return true;
    }
  }

  private synchronized Channel getChannelInternal() {
    if (channel == null) {
      channel =
          NettyChannelBuilder.forAddress(ADDRESS)
              .channelType(SocketPairChannel.class)
              .usePlaintext()
              .build();
    }
    return channel;
  }

  /** Returns the channel that can be used to call RPC of the ZetaSQL server. */
  @Override
  public Channel getChannel() {
    return getChannelInternal();
  }
}
