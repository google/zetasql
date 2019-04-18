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

import com.google.zetasql.LocalService.AnalyzeRequest;
import com.google.zetasql.LocalService.AnalyzeResponse;
import com.google.zetasql.LocalService.ExtractTableNamesFromStatementRequest;
import com.google.zetasql.LocalService.ExtractTableNamesFromStatementResponse;
import com.google.zetasql.LocalService.RegisteredParseResumeLocationProto;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import io.grpc.StatusRuntimeException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** The Analyzer class provides static methods to analyze ZetaSQL statements or expressions. */
public class Analyzer implements Serializable {

  final SimpleCatalog catalog;
  final AnalyzerOptions options;

  public Analyzer(AnalyzerOptions options, SimpleCatalog catalog) {
    this.options = options;
    this.catalog = catalog;
  }

  public ResolvedStatement analyzeStatement(String sql) {
    return analyzeStatement(sql, options, catalog);
  }

  public static ResolvedStatement analyzeStatement(
      String sql, AnalyzerOptions options, SimpleCatalog catalog) {
    AnalyzeRequest.Builder request = AnalyzeRequest.newBuilder().setSqlStatement(sql);

    FileDescriptorSetsBuilder fileDescriptorSetsBuilder =
        AnalyzerHelper.serializeSimpleCatalog(catalog, options, request);

    AnalyzeResponse response;
    try {
      response = Client.getStub().analyze(request.build());
    } catch (StatusRuntimeException e) {
      throw new SqlException(e);
    }

    return AnalyzerHelper.deserializeResolvedStatement(
        catalog, fileDescriptorSetsBuilder, response);
  }

  /**
   * Analyze the statement syntax and extract the table names. This function is a one-of which does
   * not require an instance of analyzer.
   *
   * @return List of table names. Every table name is a list of string segments, to retain original
   *     naming structure.
   */
  public static List<List<String>> extractTableNamesFromStatement(String sql) {
    ExtractTableNamesFromStatementRequest request =
        ExtractTableNamesFromStatementRequest.newBuilder().setSqlStatement(sql).build();

    ExtractTableNamesFromStatementResponse response;
    try {
      response = Client.getStub().extractTableNamesFromStatement(request);
    } catch (StatusRuntimeException e) {
      throw new SqlException(e);
    }

    ArrayList<List<String>> result = new ArrayList<>(response.getTableNameCount());
    for (ExtractTableNamesFromStatementResponse.TableName name : response.getTableNameList()) {
      ArrayList<String> nameList = new ArrayList<>(name.getTableNameSegmentCount());
      nameList.addAll(name.getTableNameSegmentList());
      result.add(nameList);
    }
    return result;
  }

  public ResolvedStatement analyzeNextStatement(ParseResumeLocation parseResumeLocation) {
    return analyzeNextStatement(parseResumeLocation, options, catalog);
  }

  /**
   * Analyze the next statement in parseResumeLocation, updating its bytePosition to the end of the
   * statement.
   */
  public static ResolvedStatement analyzeNextStatement(
      ParseResumeLocation parseResumeLocation, AnalyzerOptions options, SimpleCatalog catalog) {
    FileDescriptorSetsBuilder fileDescriptorSetsBuilder;
    AnalyzeResponse response;
    synchronized (parseResumeLocation) {
      AnalyzeRequest.Builder request;
      if (parseResumeLocation.isRegistered()) {
        RegisteredParseResumeLocationProto location =
            RegisteredParseResumeLocationProto.newBuilder()
                .setRegisteredId(parseResumeLocation.getRegisteredId())
                .setBytePosition(parseResumeLocation.getBytePosition())
                .build();
        request = AnalyzeRequest.newBuilder().setRegisteredParseResumeLocation(location);
      } else {
        request =
            AnalyzeRequest.newBuilder().setParseResumeLocation(parseResumeLocation.serialize());
      }

      fileDescriptorSetsBuilder = AnalyzerHelper.serializeSimpleCatalog(catalog, options, request);

      try {
        response = Client.getStub().analyze(request.build());
      } catch (StatusRuntimeException e) {
        throw new SqlException(e);
      }

      parseResumeLocation.setBytePosition(response.getResumeBytePosition());
    }
    return AnalyzerHelper.deserializeResolvedStatement(
        catalog, fileDescriptorSetsBuilder, response);
  }
}
