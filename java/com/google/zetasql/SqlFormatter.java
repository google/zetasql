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

import com.google.zetasql.LocalService.FormatSqlRequest;
import io.grpc.StatusRuntimeException;

/** Formatter for ZetaSQL. */
public class SqlFormatter {

  public String formatSql(String sql) {
    try {
      return Client.getStub().formatSql(request(sql)).getSql();
    } catch (StatusRuntimeException e) {
      throw new SqlException(e);
    }
  }

  public String lenientFormatSql(String sql) {
    try {
      return Client.getStub().lenientFormatSql(request(sql)).getSql();
    } catch (StatusRuntimeException e) {
      throw new SqlException(e);
    }
  }

  protected static FormatSqlRequest request(String sql) {
    return FormatSqlRequest.newBuilder().setSql(sql).build();
  }
}
