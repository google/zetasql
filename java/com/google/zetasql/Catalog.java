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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.List;

/**
 * ZetaSQL uses the Catalog interface to look up names that are visible
 * at global scope inside a query.  Names include
 *   - tables (which could also be views or other table-like objects)
 *   - types (e.g. the named types defined in some database's schema)
 *   - functions (e.g. UDFs defined in a database's schema)
 *   - nested catalogs (which allow names like catalog1.catalog2.Table)
 */
public abstract class Catalog implements Serializable {

  /**
   * Get a fully-qualified description of this Catalog.
   * Suitable for log messages, but not necessarily a valid SQL path expression.
   */
  public abstract String getFullName();

  /**
   * Options for a LookupName call.
   */
  public static class FindOptions {
    FindOptions() {}

    // Possibly deadlines, Tasks for cancellation, etc.
  }

  /**
   * Looks up an object of Constant from this Catalog on {@code path}.
   *
   * <p>Searches for the Constant by traversing nested Catalogs until it reaches the last level of
   * the path, and then calls getConstant.
   *
   * @throws NotFoundException if the Constant can not be found
   */
  public final Constant findConstant(List<String> path) throws NotFoundException {
    return findConstant(path, new FindOptions());
  }

  public final Constant findConstant(List<String> path, FindOptions options)
      throws NotFoundException {
    Preconditions.checkNotNull(path, "Invalid null Constant name path");
    Preconditions.checkArgument(!path.isEmpty(), "Invalid empty Constant name path");
    final String name = path.get(0);
    if (path.size() > 1) {
      Catalog catalog = getCatalog(name, options);
      if (catalog == null) {
        throw new NotFoundException(
            String.format(
                "Constant not found: Catalog %s not found in Catalog %s",
                ZetaSQLStrings.toIdentifierLiteral(name), getFullName()));
      }
      final List<String> pathSuffix = path.subList(1, path.size());
      return catalog.findConstant(pathSuffix, options);
    } else {
      Constant constant = getConstant(name, options);
      if (constant == null) {
        throw new NotFoundException(
            String.format(
                "Constant not found: %s not found in Catalog %s",
                ZetaSQLStrings.toIdentifierLiteral(name), getFullName()));
      }
      return constant;
    }
  }

  /**
   * Look up an object of Table from this Catalog on {@code path}.
   *
   * <p>If a Catalog implementation supports looking up Table by path, it
   * should implement the findTable method. Alternatively, a Catalog can
   * also contain nested catalogs, and implement FindTable method on
   * the inner-most Catalog.
   *
   * <p>The default findTable implementation traverses nested Catalogs until it reaches
   * a Catalog that overrides findTable, or until it gets to the last level of the
   * path and then calls getTable.
   *
   * <p>NOTE: The findTable methods take precedence over getTable methods and will always
   * be called first. So getTable method does not need to be implemented if findTable
   * method is implemented. If both getTable and findTable are implemented(though not
   * recommended), it is the implementation's responsibility to keep them
   * consistent.
   *
   * @throws NotFoundException if the Type can not be found
   */
  public Table findTable(List<String> path) throws NotFoundException {
    return findTable(path, new FindOptions());
  }

  public Table findTable(List<String> path, FindOptions options) throws NotFoundException {
    Preconditions.checkNotNull(path, "Invalid null Table name path");
    Preconditions.checkArgument(!path.isEmpty(), "Invalid empty Table name path");
    final String name = path.get(0);
    if (path.size() > 1) {
      Catalog catalog = getCatalog(name, options);
      if (catalog == null) {
        throw new NotFoundException(String.format(
            "Table not found: Catalog %s not found in Catalog %s",
            ZetaSQLStrings.toIdentifierLiteral(name), getFullName()));
      }
      final List<String> pathSuffix = path.subList(1, path.size());
      return catalog.findTable(pathSuffix, options);
    } else {
      Table table = getTable(name, options);
      if (table == null) {
        throw new NotFoundException(String.format(
            "Table not found: %s not found in Catalog %s",
            ZetaSQLStrings.toIdentifierLiteral(name), getFullName()));
      }
      return table;
    }
  }

  /**
   * Look up an object of Type from this Catalog on {@code path}.
   *
   * <p>If a Catalog implementation supports looking up Type by path, it
   * should implement the findType method. Alternatively, a Catalog can
   * also contain nested catalogs, and implement FindType method on
   * the inner-most Catalog.
   *
   * <p>The default findType implementation traverses nested Catalogs until it reaches
   * a Catalog that overrides findType, or until it gets to the last level of the
   * path and then calls getType.
   *
   * <p>NOTE: The findType methods take precedence over getType methods and will always
   * be called first. So getType method does not need to be implemented if findType
   * method is implemented. If both getType and findType are implemented(though not
   * recommended), it is the implementation's responsibility to keep them
   * consistent.
   *
   * @throws NotFoundException if the Type can not be found
   */
  public Type findType(List<String> path) throws NotFoundException {
    return findType(path, new FindOptions());
  }

  public Type findType(List<String> path, FindOptions options) throws NotFoundException {
    Type type = null;
    Preconditions.checkNotNull(path, "Invalid null Type name path");
    Preconditions.checkArgument(!path.isEmpty(), "Invalid empty Table name path");
    final String name = path.get(0);
    if (path.size() > 1) {
      Catalog catalog = getCatalog(name, options);
      if (catalog == null) {
        // FindType-specific behavior: If we have a path, and it looks like it
        // could be a valid unquoted multi-part proto name, try looking up that
        // proto name as a type.
        final String protoName = convertPathToProtoName(path);
        if (!protoName.isEmpty()) {
          type = getType(protoName, options);
          if (type != null) {
            return type;
          } else {
            throw new NotFoundException(String.format(
                "Type not found: %s is not a type and %s is not a nested Catalog in Catalog %s",
                ZetaSQLStrings.toIdentifierLiteral(protoName),
                ZetaSQLStrings.toIdentifierLiteral(name), getFullName()));
          }
        }
        throw new NotFoundException(String.format(
            "Type not found: Catalog %s  not found in Catalog %s",
            ZetaSQLStrings.toIdentifierLiteral(name), getFullName()));
      }
      final List<String> pathSuffix = path.subList(1, path.size());
      return catalog.findType(pathSuffix, options);
    } else {
      type = getType(name, options);
      if (type == null) {
        throw new NotFoundException(String.format(
            "Type not found: %s not found in Catalog %s",
            ZetaSQLStrings.toIdentifierLiteral(name), getFullName()));
      }
    }
    return type;
  }

  /**
   * Look up an object of Procedure from this Catalog on {@code path}.
   *
   * <p>If a Catalog implementation supports looking up Procedure by path, it
   * should implement the findProcedure method. Alternatively, a Catalog can
   * also contain nested catalogs, and implement FindProcedure method on
   * the inner-most Catalog.
   *
   * <p>The default findProcedure implementation traverses nested Catalogs until it reaches
   * a Catalog that overrides findProcedure, or until it gets to the last level of the
   * path and then calls getProcedure.
   *
   * <p>NOTE: The findProcedure methods take precedence over getProcedure methods and will always
   * be called first. So getProcedure method does not need to be implemented if findProcedure
   * method is implemented. If both getProcedure and findProcedure are implemented(though not
   * recommended), it is the implementation's responsibility to keep them
   * consistent.
   *
   * @throws NotFoundException if the Procedure can not be found
   */
  public Procedure findProcedure(List<String> path) throws NotFoundException {
    return findProcedure(path, new FindOptions());
  }

  public Procedure findProcedure(List<String> path, FindOptions options) throws NotFoundException {
    Preconditions.checkNotNull(path, "Invalid null Procedure name path");
    Preconditions.checkArgument(!path.isEmpty(), "Invalid empty Procedure name path");
    final String name = path.get(0);
    if (path.size() > 1) {
      Catalog catalog = getCatalog(name, options);
      if (catalog == null) {
        throw new NotFoundException(String.format(
            "Procedure not found: Catalog %s not found in Catalog %s",
            ZetaSQLStrings.toIdentifierLiteral(name), getFullName()));
      }
      final List<String> pathSuffix = path.subList(1, path.size());
      return catalog.findProcedure(pathSuffix, options);
    } else {
      Procedure procedure = getProcedure(name, options);
      if (procedure == null) {
        throw new NotFoundException(String.format(
            "Procedure not found: %s not found in Catalog %s",
            ZetaSQLStrings.toIdentifierLiteral(name), getFullName()));
      }
      return procedure;
    }
  }

  /**
   * Given an identifier path, return the type name that results when combining
   * that path into a single protocol buffer type name, if applicable.
   * Returns empty string if {@code path} cannot form a valid proto-style type name.
   *
   * @return type name if applicable, empty string if not
   */
  public static String convertPathToProtoName(List<String> path) {
    if (path == null) {
      return "";
    }

    for (String identifier : path) {
      if (!isIdentifier(identifier)) {
        return "";
      }
    }
    // This also returns "" for empty paths.
    return Joiner.on(".").join(path);
  }

  /**
   * Get an object of Constant from this Catalog, without looking at any nested Catalogs.
   *
   * <p>Returns null if the object doesn't exist.
   *
   * <p>The default implementations always return null.
   *
   * @return Constant object if found, null if not found
   */
  protected final Constant getConstant(String name) {
    return getConstant(name, new FindOptions());
  }

  /**
   * <p>Get an object of Constant from this Catalog, without looking at any nested Catalogs.
   *
   * <p>Returns null if the object doesn't exist.
   *
   * @return Constant object if found, null if not found
   */
  protected abstract Constant getConstant(
      @SuppressWarnings("unused") String name, @SuppressWarnings("unused") FindOptions options);

  /**
   * Get an object of Table from this Catalog, without
   * looking at any nested Catalogs.
   *
   * <p>A NULL pointer should be returned if the object doesn't exist.
   *
   * <p>The default implementations always return null.
   *
   * @return Table object if found, NULL if not found
   */
  protected final Table getTable(String name) {
    return getTable(name, new FindOptions());
  }

  /**
   * NOTE: If FindTable is implemented, there is no need to implement GetTable, as FindTable method
   * takes precedence over GetTable and is always called first.
   *
   * <p>Get an object of Table from this Catalog, without looking at any nested Catalogs.
   *
   * <p>A NULL pointer should be returned if the object doesn't exist.
   *
   * <p>This is normally overridden in subclasses. The default implementations always return null.
   *
   * @return Table object if found, NULL if not found
   */
  protected Table getTable(
      @SuppressWarnings("unused") String name, @SuppressWarnings("unused") FindOptions options) {
    return null;
  }

  /**
   * Get an object of Procedure from this Catalog, without
   * looking at any nested Catalogs.
   *
   * <p>A NULL pointer should be returned if the object doesn't exist.
   *
   * <p>The default implementations always return null.
   *
   * @return Procedure object if found, NULL if not found
   */
  protected final Procedure getProcedure(String name) {
    return getProcedure(name, new FindOptions());
  }

  /**
   * NOTE: If findProcedure is implemented, there is no need to implement getProcedure,
   * as findProcedure method takes precedence over getProcedure and is always called first.
   *
   * <p>Get an object of Procedure from this Catalog, without
   * looking at any nested Catalogs.
   *
   * <p>A NULL pointer should be returned if the object doesn't exist.
   *
   * <p>This is normally overriden in subclasses. The default implementations
   * always return null.
   *
   * @return Procedure object if found, NULL if not found
   */
  protected Procedure getProcedure(
      @SuppressWarnings("unused") String name, @SuppressWarnings("unused") FindOptions options) {
    return null;
  }

  /**
   * Get an object of Type from this Catalog, without
   * looking at any nested Catalogs.
   *
   * <p>A NULL pointer should be returned if the object doesn't exist.
   *
   * <p>The default implementations always return null.
   *
   * @return Type object if found, NULL if not found
   */
  protected final Type getType(String name) {
    return getType(name, new FindOptions());
  }

  /**
   * NOTE: If FindType is implemented, there is no need to implement GetType,
   * as FindType method takes precedence over GetType and is always called first.
   *
   * <p>Get an object of Type from this Catalog, without
   * looking at any nested Catalogs.
   *
   * <p>A NULL pointer should be returned if the object doesn't exist.
   *
   * <p>This is normally overriden in subclasses. The default implementations
   * always return null.
   *
   * @return Type object if found, NULL if not found
   */
  protected Type getType(
      @SuppressWarnings("unused") String name, @SuppressWarnings("unused") FindOptions options) {
    return null;
  }

  /**
   * Get an object of Catalog from this Catalog, without
   * looking at any nested Catalogs.
   *
   * <p>A NULL pointer should be returned if the object doesn't exist.
   *
   * <p>The default implementations always return null.
   *
   * @return Catalog object if found, NULL if not found
   */
  protected final Catalog getCatalog(String name) {
    return getCatalog(name, new FindOptions());
  }

  /**
   * NOTE: If FindCatalog is implemented, there is no need to implement GetCatalog,
   * as FindCatalog method takes precedence over GetCatalog and is always called first.
   *
   * <p>Get an object of Catalog from this Catalog, without
   * looking at any nested Catalogs.
   *
   * <p>A NULL pointer should be returned if the object doesn't exist.
   *
   * <p>This is normally overriden in subclasses. The default implementations
   * always return null.
   *
   * @return Catalog object if found, NULL if not found
   */
  protected Catalog getCatalog(
      @SuppressWarnings("unused") String name, @SuppressWarnings("unused") FindOptions options) {
    return null;
  }

  private static boolean isIdentifier(String text) {
    if (text.isEmpty()) {
      return false;
    }
    if (!isLetter(text.charAt(0))) {
      return false;
    }
    for (char c : text.toCharArray()) {
      if (!isAlphanumeric(c)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isLetter(char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || (c == '_');
  }

  private static boolean isAlphanumeric(char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || (c == '_');
  }
}
