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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.zetasql.LocalService.ParseRequest;
import com.google.zetasql.LocalService.ParseResponse;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import io.grpc.StatusRuntimeException;

/** The Parser class provides a static method to parse an SQL query into an ASTStatement. */
public class Parser {

  public static ASTStatement parseStatement(String sql, LanguageOptions languageOptions) {
    checkNotNull(sql);
    checkNotNull(languageOptions);
    ParseResponse response;
    try {
      ParseRequest request = ParseRequest.newBuilder()
          .setSqlStatement(sql)
          .setOptions(languageOptions.serialize())
          .build();
      response = Client.getStub().parse(request);
      return ASTStatement.deserialize(response.getParsedStatement());
    } catch (StatusRuntimeException e) {
      throw new SqlException(e);
    }
  }

  private Parser() {}
}
