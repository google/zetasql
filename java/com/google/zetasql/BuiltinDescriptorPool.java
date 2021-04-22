/*
 * Copyright 2019 Google LLC
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

import com.google.protobuf.BoolValue;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.zetasql.functions.ZetaSQLDateTime.DateTimestampPart;
import com.google.zetasql.functions.ZetaSQLNormalizeMode.NormalizeMode;
import com.google.type.Date;
import com.google.type.LatLng;
import com.google.type.TimeOfDay;

/**
 * DescriptorPool used by the zetasql itself when returning ResolvedAST. Contains, for instance
 * the enums used as part of language constructs for instance {@code zetasql.DateTimestampPart}
 * which is used in DATE_ADD. Also includes any protobuf protos that are used for that same purpose.
 */
final class BuiltinDescriptorPool {
  private static final ImmutableDescriptorPool instance =
      ImmutableDescriptorPool.builder()
          .importFileDescriptor(DateTimestampPart.getDescriptor().getFile())
          .importFileDescriptor(NormalizeMode.getDescriptor().getFile())
          .importFileDescriptor(Timestamp.getDescriptor().getFile())
          .importFileDescriptor(Date.getDescriptor().getFile())
          .importFileDescriptor(TimeOfDay.getDescriptor().getFile())
          .importFileDescriptor(LatLng.getDescriptor().getFile())
          .importFileDescriptor(DoubleValue.getDescriptor().getFile())
          .importFileDescriptor(FloatValue.getDescriptor().getFile())
          .importFileDescriptor(Int64Value.getDescriptor().getFile())
          .importFileDescriptor(UInt64Value.getDescriptor().getFile())
          .importFileDescriptor(Int32Value.getDescriptor().getFile())
          .importFileDescriptor(UInt32Value.getDescriptor().getFile())
          .importFileDescriptor(BoolValue.getDescriptor().getFile())
          .importFileDescriptor(StringValue.getDescriptor().getFile())
          .importFileDescriptor(BytesValue.getDescriptor().getFile())
          .importFileDescriptor(EnumValueDescriptorProto.getDescriptor().getFile())
          .build();

  public static DescriptorPool getInstance() {
    return instance;
  }

  private BuiltinDescriptorPool() {}
}
