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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.zetasql.DescriptorPool.ZetaSQLDescriptor;
import com.google.zetasql.DescriptorPool.ZetaSQLEnumDescriptor;
import com.google.zetasql.FunctionProtos.FunctionProto;
import com.google.zetasql.FunctionProtos.ProcedureProto;
import com.google.zetasql.FunctionProtos.TableValuedFunctionProto;
import com.google.zetasql.ZetaSQLOptionsProto.ZetaSQLBuiltinFunctionOptionsProto;
import com.google.zetasql.ZetaSQLType.TypeProto;
import com.google.zetasql.LocalService.GetBuiltinFunctionsResponse;
import com.google.zetasql.LocalService.RegisterCatalogRequest;
import com.google.zetasql.LocalService.RegisterResponse;
import com.google.zetasql.LocalService.UnregisterRequest;
import com.google.zetasql.SimpleCatalogProtos.SimpleCatalogProto;
import com.google.zetasql.SimpleCatalogProtos.SimpleCatalogProto.NamedTypeProto;
import com.google.zetasql.SimpleConstantProtos.SimpleConstantProto;
import com.google.zetasql.SimpleTableProtos.SimpleTableProto;
import io.grpc.StatusRuntimeException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

/**
 * SimpleCatalog is a concrete implementation of the Catalog interface. It acts as a simple
 * container for objects in the Catalog.
 *
 * <p>This class and related SimpleX classes are final by design because a subclass is unlikely to
 * get serialize method right. The serialize method returns a SimpleCatalogProto. A subclass most
 * likely needs more fields and hence needs a different proto. But proto class is final so a
 * subclass cannot override this class's serialize method and return a subclass of
 * SimpleCatalogProto. TODO: Add findTableById.
 *
 * <p>To improve RPC performance, a SimpleCatalog can be registered on the local server, and later
 * requests involving this SimpleCatalog will only pass the registered ID instead of the fully
 * serialized form. A registered SimpleCatalog can and should be unregistered when it is no longer
 * used. The unregister() method is called in finalize(), but users shouldn't rely on that because
 * the JVM doesn't know of the memory resource taken by the registered catalog on the local server,
 * and may start the garbage collector later than necessary.
 */
public class SimpleCatalog extends Catalog {
  private static final Logger logger = Logger.getLogger(SimpleCatalog.class.getName());

  private final String name;
  private final TypeFactory typeFactory;
  // All String keys are stored in lower case.
  private final Map<String, Constant> constants = new HashMap<>();
  private final Map<String, SimpleTable> tables = new HashMap<>();
  private final Map<String, Type> types = new HashMap<>();
  private final Map<String, SimpleCatalog> catalogs = new HashMap<>();
  private final Map<Long, SimpleTable> tablesById = new HashMap<>();
  private final Map<String, Function> customFunctions = new HashMap<>();
  private final Map<String, TableValuedFunction> tvfs = new HashMap<>();
  private final Map<String, Function> functionsByFullName = new HashMap<>();
  private final Map<String, Procedure> procedures = new HashMap<>();
  private DescriptorPool descriptorPool;
  ZetaSQLBuiltinFunctionOptionsProto builtinFunctionOptions;

  boolean registered = false;
  long registeredId = -1;
  FileDescriptorSetsBuilder registeredFileDescriptorSetsBuilder = null;
  // If the catalog is registered, contains the descriptor pool ids that should be
  // used to serialize it in the future.
  ImmutableMap<DescriptorPool, Long> registeredDescriptorPoolIds = null;

  public SimpleCatalog(String name, TypeFactory typeFactory) {
    this.name = name;
    this.typeFactory = Preconditions.checkNotNull(typeFactory);
  }

  public SimpleCatalog(String name) {
    this(name, TypeFactory.nonUniqueNames());
  }

  /**
   * Register this catalog to local server, so that it can be reused without passing through RPC
   * every time.
   *
   * @return An AutoCloseable object that can be used to unregister the catalog in
   *     try-with-resources structure.
   */
  public AutoUnregister register() {
    return register(ImmutableMap.of());
  }

  /**
   * Register this catalog to local server, so that it can be reused without passing through RPC
   * every time.
   *
   * @param tablesContents The contents of the tables owned by this catalog. The key is the name of
   *     table, while the value is content for that table.
   * @return An AutoCloseable object that can be used to unregister the catalog in
   *     try-with-resources structure.
   */
  public AutoUnregister register(Map<String, TableContent> tablesContents) {
    Preconditions.checkState(!registered);
    try {
      RegisterResponse resp =
          Client.getStub().registerCatalog(createRegisterRequest(tablesContents));
      processRegisterResponse(resp);
    } catch (StatusRuntimeException e) {
      throw new SqlException(e);
    }

    return new AutoUnregister();
  }

  protected RegisterCatalogRequest createRegisterRequest() {
    return createRegisterRequest(ImmutableMap.of());
  }

  protected RegisterCatalogRequest createRegisterRequest(Map<String, TableContent> tablesContents) {
    Preconditions.checkState(!registered);
    Preconditions.checkNotNull(tablesContents);
    registeredFileDescriptorSetsBuilder = new FileDescriptorSetsBuilder();
    RegisterCatalogRequest.Builder builder = RegisterCatalogRequest.newBuilder();
    builder.setSimpleCatalog(serialize(registeredFileDescriptorSetsBuilder));
    tablesContents.forEach((t, c) -> builder.putTableContent(t, c.serialize()));
    registeredFileDescriptorSetsBuilder.build();
    builder.setDescriptorPoolList(
        DescriptorPoolSerializer.createDescriptorPoolList(registeredFileDescriptorSetsBuilder));
    return builder.build();
  }

  protected void processRegisterResponse(RegisterResponse resp) {
    registeredId = resp.getRegisteredId();
    ImmutableMap.Builder<DescriptorPool, Long> idsMapBuilder = ImmutableMap.builder();
    ImmutableList<DescriptorPool> pools = registeredFileDescriptorSetsBuilder.getDescriptorPools();
    List<Long> poolIds = resp.getDescriptorPoolIdList().getRegisteredIdsList();
    Preconditions.checkState(pools.size() == poolIds.size());
    for (int i = 0; i < pools.size(); ++i) {
      idsMapBuilder.put(pools.get(i), poolIds.get(i));
    }
    registeredDescriptorPoolIds = idsMapBuilder.build();
    registered = true;
  }

  public void unregister() {
    Preconditions.checkState(registered);
    try {
      Client.getStub()
          .unregisterCatalog(UnregisterRequest.newBuilder().setRegisteredId(registeredId).build());
    } catch (StatusRuntimeException e) {
      // Maybe caused by double unregistering (in race conditions) or RPC
      // failure. The latter may cause leak but it's likely due to more serious
      // problems and there is no good way to recover. Just log and ignore.
      logger.severe("Failed to unregister catalog: " + e.getMessage());
    } finally {
      registered = false;
      registeredId = -1;
      registeredFileDescriptorSetsBuilder = null;
      registeredDescriptorPoolIds = null;
    }
  }

  public boolean isRegistered() {
    return registered;
  }

  long getRegisteredId() {
    Preconditions.checkState(registered);
    return registeredId;
  }

  @Override
  protected void finalize() {
    if (registered) {
      unregister();
    }
  }

  FileDescriptorSetsBuilder getRegisteredFileDescriptorSetsBuilder() {
    Preconditions.checkState(registered);
    return registeredFileDescriptorSetsBuilder;
  }

  Map<DescriptorPool, Long> getRegisteredDescriptorPoolIds() {
    if (registeredDescriptorPoolIds == null) {
      return ImmutableMap.of();
    }
    return ImmutableMap.copyOf(registeredDescriptorPoolIds);
  }

  /**
   * Serialize this catalog into protobuf, with FileDescriptors emitted to the builder as needed.
   */
  public SimpleCatalogProto serialize(FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    SimpleCatalogProto.Builder builder = SimpleCatalogProto.newBuilder();
    builder.setName(name);
    // The built-in function definations are not serialized. Instead, the BuiltinFunctionOptions
    // which specify which functions to include and exclude will be serialized, and the C++
    // deserialization will recreate the same built-in function signatures according to this.
    if (builtinFunctionOptions != null) {
      builder.setBuiltinFunctionOptions(builtinFunctionOptions);
    }
    for (Entry<String, SimpleTable> table : tables.entrySet()) {
      SimpleTableProto.Builder tableBuilder = builder.addTableBuilder();
      tableBuilder.mergeFrom(table.getValue().serialize(fileDescriptorSetsBuilder));
      if (!table.getKey().equals(tableBuilder.getName().toLowerCase())) {
        tableBuilder.setNameInCatalog(table.getKey());
      }
    }
    for (Entry<String, Type> type : types.entrySet()) {
      TypeProto.Builder typeProtoBuilder = TypeProto.newBuilder();
      type.getValue().serialize(typeProtoBuilder, fileDescriptorSetsBuilder);
      builder.addNamedType(
          NamedTypeProto.newBuilder().setName(type.getKey()).setType(typeProtoBuilder.build()));
    }
    for (Entry<String, SimpleCatalog> catalog : catalogs.entrySet()) {
      builder.addCatalog(catalog.getValue().serialize(fileDescriptorSetsBuilder));
    }
    for (Entry<String, TableValuedFunction> tvf : tvfs.entrySet()) {
      builder.addCustomTvf(tvf.getValue().serialize(fileDescriptorSetsBuilder));
    }

    for (Entry<String, Constant> constant : constants.entrySet()) {
      builder.addConstant(constant.getValue().serialize(fileDescriptorSetsBuilder));
    }
    for (Entry<String, Function> function : customFunctions.entrySet()) {
      builder.addCustomFunction(function.getValue().serialize(fileDescriptorSetsBuilder));
    }

    for (Entry<String, Procedure> procedure : procedures.entrySet()) {
      builder.addProcedure(procedure.getValue().serialize(fileDescriptorSetsBuilder));
    }

    if (descriptorPool != null) {
      builder.setFileDescriptorSetIndex(
          fileDescriptorSetsBuilder.addAllFileDescriptors(descriptorPool));
    }

    return builder.build();
  }

  /** Add simple constant into this catalog. Constant names are case insensitive. */
  public void addConstant(Constant constant) {

    Preconditions.checkState(!registered);
    Preconditions.checkArgument(!constants.containsKey(constant.getFullName().toLowerCase()));
    List<String> namePath = constant.getNamePath();
    Preconditions.checkArgument(!namePath.isEmpty());
    constants.put(namePath.get(namePath.size() - 1).toLowerCase(), constant);
  }

  /**
   * Add simple table into this catalog. Table names are case insensitive.
   *
   * @param table
   */
  public void addSimpleTable(SimpleTable table) {
    addSimpleTable(table.getName(), table);
  }

  public void addSimpleTable(String name, SimpleTable table) {
    Preconditions.checkState(!registered);
    Preconditions.checkArgument(!tables.containsKey(name.toLowerCase()), "duplicate key: %s", name);
    tables.put(name.toLowerCase(), table);
    tablesById.put(table.getId(), table);
  }

  /**
   * Removes a simple table from this catalog.
   */
  public void removeSimpleTable(SimpleTable table) {
    removeSimpleTable(table.getName());
  }

  /**
   * Removes a simple table from this catalog. Table names are case insensitive.
   */
  public void removeSimpleTable(String name) {
    Preconditions.checkState(!registered);
    Preconditions.checkArgument(tables.containsKey(name.toLowerCase()), "missing key: %s", name);
    SimpleTable table = tables.remove(name.toLowerCase());
    tablesById.remove(table.getId());
  }

  /**
   * Add type into this catalog. Type names are case insensitive.
   *
   * @param name
   * @param type
   */
  public void addType(String name, Type type) {
    Preconditions.checkState(!registered);
    Preconditions.checkArgument(!types.containsKey(name.toLowerCase()), "duplicate key: %s", name);
    types.put(name.toLowerCase(), type);
  }

  /**
   * Removes the type with the passed name from this catalog. Type names are case insensitive.
   */
  public void removeType(String name) {
    Preconditions.checkState(!registered);
    Preconditions.checkArgument(types.containsKey(name.toLowerCase()), "missing key: %s", name);
    types.remove(name.toLowerCase());
  }

  /**
   * Add sub catalog into this catalog. Catalog names are case insensitive.
   *
   * @param catalog
   */
  public void addSimpleCatalog(SimpleCatalog catalog) {
    Preconditions.checkState(!registered);
    Preconditions.checkArgument(
        !catalogs.containsKey(catalog.getFullName().toLowerCase()),
        "duplicate key: %s",
        catalog.getFullName());
    catalogs.put(catalog.getFullName().toLowerCase(), catalog);
  }

  /**
   * Create a new catalog and add the new catalog into this catalog and return the new sub catalog.
   * Catalog names are case insensitive.
   *
   * @param name
   * @return the created new sub catalog
   */
  public SimpleCatalog addNewSimpleCatalog(String name) {
    Preconditions.checkState(!registered);
    SimpleCatalog newCatalog = new SimpleCatalog(name, getTypeFactory());
    addSimpleCatalog(newCatalog);
    return newCatalog;
  }

  /**
   * Removes the passed sub catalog from this catalog.
   */
  public void removeSimpleCatalog(SimpleCatalog catalog) {
    removeSimpleCatalog(catalog.getFullName());
  }

  /**
   * Removes the sub catalog with the passed name from this catalog. Catalog names are case
   * insensitive.
   */
  public void removeSimpleCatalog(String fullName) {
    Preconditions.checkState(!registered);
    Preconditions.checkArgument(catalogs.containsKey(fullName.toLowerCase()),
        "missing key: %s", fullName);
    catalogs.remove(fullName.toLowerCase());
  }

  /**
   * Add ZetaSQL built-in function definitions into this catalog. Function names are case
   * insensitive.
   *
   * @param options used to select which functions get loaded.
   */
  public void addZetaSQLFunctions(ZetaSQLBuiltinFunctionOptions options) {
    Preconditions.checkNotNull(options);
    Preconditions.checkState(builtinFunctionOptions == null);
    builtinFunctionOptions = options.serialize();

    try {
      GetBuiltinFunctionsResponse response =
          Client.getStub().getBuiltinFunctions(builtinFunctionOptions);

      processGetBuiltinFunctionsResponse(response);
    } catch (StatusRuntimeException e) {
      throw new SqlException(e);
    }
  }

  void processGetBuiltinFunctionsResponse(GetBuiltinFunctionsResponse response) {
    ImmutableList<DescriptorPool> pools = ImmutableList.of(BuiltinDescriptorPool.getInstance());
    for (FunctionProto proto : response.getFunctionList()) {
      addFunctionToFullNameMap(Function.deserialize(proto, pools));
    }
  }

  /**
   * Add function into this catalog. Function name and alias name will both be stored if different.
   * Names are case insensitive.
   *
   * @param function
   */
  public void addFunction(Function function) {
    Preconditions.checkState(!registered);
    Preconditions.checkArgument(!customFunctions.containsKey(function.getName().toLowerCase()));
    customFunctions.put(function.getName().toLowerCase(), function);
    if (!function.getOptions().getAliasName().isEmpty()
        && !function.getOptions().getAliasName().equals(function.getName())) {
      Preconditions.checkArgument(
          !customFunctions.containsKey(function.getOptions().getAliasName().toLowerCase()));
      customFunctions.put(function.getOptions().getAliasName().toLowerCase(), function);
    }
    addFunctionToFullNameMap(function);
  }

  private void addFunctionToFullNameMap(Function function) {
    String functionName = function.getFullName().toLowerCase();
    Preconditions.checkArgument(
        !functionsByFullName.containsKey(functionName), functionName + " already exists.");
    functionsByFullName.put(functionName, function);
  }

  /**
   * Removes the passed function from this catalog.
   */
  public void removeFunction(Function function) {
    removeFunction(function.getFullName());
  }

  /**
   * Removes the function with the passed full name from this catalog.
   */
  public void removeFunction(String fullName) {
    Preconditions.checkState(!registered);
    Function function = getFunctionByFullName(fullName);
    Preconditions.checkArgument(function != null);
    Preconditions.checkArgument(customFunctions.containsKey(function.getName().toLowerCase()));
    customFunctions.remove(function.getName().toLowerCase());
    if (!function.getOptions().getAliasName().isEmpty()) {
      customFunctions.remove(function.getOptions().getAliasName().toLowerCase());
    }
    removeFunctionFromFullNameMap(function);
  }

  /** Add the given {@code tvf} to this catalog. Names are case insensitive. */
  public void addTableValuedFunction(TableValuedFunction tvf) {
    Preconditions.checkState(!registered);
    String tvfName = tvf.getName().toLowerCase();
    Preconditions.checkArgument(!tvfs.containsKey(tvfName));
    tvfs.put(tvfName, tvf);
  }

  /** Removes the passed TVF from this catalog. */
  public void removeTableValuedFunction(TableValuedFunction tvf) {
    removeTableValuedFunction(tvf.getFullName());
  }

  /** Removes the TVF with the passed full name from this catalog. */
  public void removeTableValuedFunction(String name) {
    Preconditions.checkState(!registered);
    String lowerCaseName = name.toLowerCase();
    TableValuedFunction tvf = getTVFByName(lowerCaseName);
    Preconditions.checkArgument(tvf != null);
    tvfs.remove(lowerCaseName);
  }

  private void removeFunctionFromFullNameMap(Function function) {
    Preconditions.checkArgument(
        functionsByFullName.containsKey(function.getFullName().toLowerCase()));
    functionsByFullName.remove(function.getFullName().toLowerCase());
  }

  /**
   * Add procedure into this catalog. Names are case insensitive.
   *
   * @param procedure
   */
  public void addProcedure(Procedure procedure) {
    Preconditions.checkState(!registered);
    Preconditions.checkArgument(!procedures.containsKey(procedure.getName().toLowerCase()));
    procedures.put(procedure.getName().toLowerCase(), procedure);
  }

  /**
   * Removes the passed procedure from this catalog.
   */
  public void removeProcedure(Procedure procedure) {
    removeProcedure(procedure.getName());
  }

  /**
   * Removes the procedure with the passed name from this catalog. Names are case insensitive.
   */
  public void removeProcedure(String name) {
    Preconditions.checkState(!registered);
    Preconditions.checkArgument(procedures.containsKey(name.toLowerCase()));
    procedures.remove(name.toLowerCase());
  }

  public void setDescriptorPool(DescriptorPool descriptorPool) {
    Preconditions.checkState(!registered);
    Preconditions.checkState(this.descriptorPool == null);
    this.descriptorPool = descriptorPool;
  }

  @Override
  public String getFullName() {
    return name;
  }

  public TypeFactory getTypeFactory() {
    return typeFactory;
  }

  public ImmutableList<String> getTableNameList() {
    return ImmutableList.copyOf(tables.keySet());
  }

  public ImmutableList<SimpleTable> getTableList() {
    return ImmutableList.copyOf(tables.values());
  }

  public ImmutableList<Type> getTypeList() {
    return ImmutableList.copyOf(types.values());
  }

  public ImmutableList<SimpleCatalog> getCatalogList() {
    return ImmutableList.copyOf(catalogs.values());
  }

  public ImmutableList<Function> getFunctionList() {
    return ImmutableList.copyOf(functionsByFullName.values());
  }

  public ImmutableList<TableValuedFunction> getTVFList() {
    return ImmutableList.copyOf(tvfs.values());
  }

  public ImmutableList<String> getFunctionNameList() {
    return ImmutableList.copyOf(functionsByFullName.keySet());
  }

  public ImmutableList<String> getTVFNameList() {
    return ImmutableList.copyOf(tvfs.keySet());
  }

  public ImmutableList<Procedure> getProcedureList() {
    return ImmutableList.copyOf(procedures.values());
  }

  @Override
  protected Constant getConstant(String name, FindOptions options) {
    return constants.get(name.toLowerCase());
  }

  @Override
  public SimpleTable getTable(String name, FindOptions options) {
    return tables.get(name.toLowerCase());
  }

  @Override
  public Type getType(String name, FindOptions options) {
    if (types.containsKey(name.toLowerCase())) {
      return types.get(name.toLowerCase());
    }
    if (descriptorPool != null) {
      ZetaSQLDescriptor descriptor = descriptorPool.findMessageTypeByName(name);
      if (descriptor != null) {
        return getTypeFactory().createProtoType(descriptor);
      }
      ZetaSQLEnumDescriptor enumDescriptor = descriptorPool.findEnumTypeByName(name);
      if (enumDescriptor != null) {
        return getTypeFactory().createEnumType(enumDescriptor);
      }
    }
    return null;
  }

  @Override
  public SimpleCatalog getCatalog(String name, FindOptions options) {
    return catalogs.get(name.toLowerCase());
  }

  public Function getFunctionByFullName(String fullName) {
    Function function = functionsByFullName.get(fullName.toLowerCase());
    if (function == null) {
      for (SimpleCatalog catalog : catalogs.values()) {
        function = catalog.getFunctionByFullName(fullName);
        if (function != null) {
          break;
        }
      }
    }
    return function;
  }

  public TableValuedFunction getTVFByName(String name) {
    return tvfs.get(name.toLowerCase());
  }

  public SimpleTable getTableById(long serializationId) {
    SimpleTable table = tablesById.get(serializationId);
    if (table == null) {
      for (SimpleCatalog catalog : catalogs.values()) {
        table = catalog.getTableById(serializationId);
        if (table != null) {
          break;
        }
      }
    }
    return table;
  }

  public SimpleModel getModelById(long serializationId) {
    // TODO: Add support for Model in the Java implementation.
    return null;
  }

  public SimpleConnection getConnectionByFullName(String fullName) {
    // TODO: Add support for Connection in the Java implementation.
    return null;
  }

  @Override
  protected Procedure getProcedure(String name, FindOptions options) {
    return procedures.get(name.toLowerCase());
  }

  /**
   * AutoCloseable implementation that unregisters the SimpleCatalog automatically when used in
   * try-with-resources.
   */
  public class AutoUnregister implements AutoCloseable {
    @Override
    public void close() {
      unregister();
    }
  }

  public static SimpleCatalog deserialize(
      SimpleCatalogProto proto, ImmutableList<? extends DescriptorPool> pools) {
    SimpleCatalog catalog = new SimpleCatalog(proto.getName());

    for (SimpleTableProto tableProto : proto.getTableList()) {
      String name =
          tableProto.hasNameInCatalog() ? tableProto.getNameInCatalog() : tableProto.getName();
      catalog.addSimpleTable(
          name, SimpleTable.deserialize(tableProto, pools, catalog.getTypeFactory()));
    }

    for (NamedTypeProto typeProto : proto.getNamedTypeList()) {
      catalog.addType(
          typeProto.getName(), catalog.getTypeFactory().deserialize(typeProto.getType(), pools));
    }

    for (SimpleCatalogProto catalogProto : proto.getCatalogList()) {
      catalog.addSimpleCatalog(SimpleCatalog.deserialize(catalogProto, pools));
    }

    for (SimpleConstantProto constantProto : proto.getConstantList()) {
      catalog.addConstant(Constant.deserialize(constantProto, pools, catalog.getTypeFactory()));
    }

    for (TableValuedFunctionProto tvfProto : proto.getCustomTvfList()) {
      catalog.addTableValuedFunction(
          TableValuedFunction.deserialize(tvfProto, pools, catalog.getTypeFactory()));
    }

    for (FunctionProto functionProto : proto.getCustomFunctionList()) {
      catalog.addFunction(Function.deserialize(functionProto, pools));
    }

    for (ProcedureProto procedureProto : proto.getProcedureList()) {
      catalog.addProcedure(Procedure.deserialize(procedureProto, pools));
    }

    if (proto.hasBuiltinFunctionOptions()) {
      ZetaSQLBuiltinFunctionOptions options =
          new ZetaSQLBuiltinFunctionOptions(proto.getBuiltinFunctionOptions());
      catalog.addZetaSQLFunctions(options);
    }

    if (proto.hasFileDescriptorSetIndex()) {
      catalog.setDescriptorPool(pools.get(proto.getFileDescriptorSetIndex()));
    }

    return catalog;
  }
}
