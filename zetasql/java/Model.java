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

import com.google.common.collect.ImmutableList;
import java.io.Serializable;

/** An ML Model in a ZetaSQL query. */
public interface Model extends Serializable {

  /** Get the model name. */
  public String getName();

  /**
   * Get a fully-qualified description of this Model. Suitable for log messages, but not necessarily
   * a valid SQL path expression.
   */
  public String getFullName();

  public int getInputColumnCount();

  public Column getInputColumn(int i);

  public ImmutableList<? extends Column> getInputColumnList();

  public Column findInputColumnByName(String name);

  public int getOutputColumnCount();

  public Column getOutputColumn(int i);

  public ImmutableList<? extends Column> getOutputColumnList();

  public Column findOutputColumnByName(String name);

  public long getId();
}
