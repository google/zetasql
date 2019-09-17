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

/** SimpleModel is a concrete implementation of the Model interface. */
// TODO: Finish the implementation of SimpleModel.
public final class SimpleModel implements Model {

  public SimpleModel() {}

  @Override
  public String getName() {
    return "";
  }

  @Override
  public String getFullName() {
    return "";
  }

  @Override
  public int getInputColumnCount() {
    return 0;
  }

  @Override
  public Column getInputColumn(int i) {
    return null;
  }

  @Override
  public ImmutableList<? extends Column> getInputColumnList() {
    return null;
  }

  @Override
  public Column findInputColumnByName(String name) {
    return null;
  }

  @Override
  public int getOutputColumnCount() {
    return 0;
  }

  @Override
  public Column getOutputColumn(int i) {
    return null;
  }

  @Override
  public ImmutableList<? extends Column> getOutputColumnList() {
    return null;
  }

  @Override
  public Column findOutputColumnByName(String name) {
    return null;
  }

  @Override
  public long getId() {
    return 0;
  }
}
