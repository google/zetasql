//
// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "zetasql/testdata/sample_catalog.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor_database.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/annotation/collation.h"
#include "zetasql/public/anon_function.h"
#include "zetasql/public/builtin_function.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/deprecation_warning.pb.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/procedure.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/testdata/ambiguous_has.pb.h"
#include "zetasql/testdata/sample_annotation.h"
#include "zetasql/testdata/test_proto3.pb.h"
#include "zetasql/base/testing/status_matchers.h"
#include <cstdint>
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

SampleCatalog* SampleCatalog::instance_ = nullptr;

SampleCatalog::SampleCatalog()
    : internal_type_factory_(new TypeFactory),
      types_(internal_type_factory_.get()) {
  catalog_ = absl::make_unique<SimpleCatalog>("sample_catalog", types_);
  LoadCatalog(LanguageOptions());
}

SampleCatalog::SampleCatalog(const LanguageOptions& language_options,
                             TypeFactory* type_factory) {
  if (type_factory == nullptr) {
    internal_type_factory_ = absl::make_unique<TypeFactory>();
    types_ = internal_type_factory_.get();
  } else {
    types_ = type_factory;
  }
  catalog_ = absl::make_unique<SimpleCatalog>("sample_catalog", types_);
  LoadCatalog(language_options);
}

SampleCatalog::~SampleCatalog() {
}

SimpleTable* SampleCatalog::GetTableOrDie(const std::string& name) {
  return zetasql_base::FindOrDie(tables_, name);
}

absl::StatusOr<SimpleTable*> SampleCatalog::GetTable(const std::string& name) {
  SimpleTable** table = zetasql_base::FindOrNull(tables_, name);
  if (table != nullptr) {
    return *table;
  } else {
    return zetasql_base::NotFoundErrorBuilder()
           << "SampleCatalog: Table " << name << " not found";
  }
}

const SimpleCatalog* const SampleCatalog::Get() {
  if (instance_ == nullptr) {
    instance_ = new SampleCatalog();
  }
  return instance_->catalog();
}

const ProtoType* SampleCatalog::GetProtoType(
    const google::protobuf::Descriptor* descriptor) {
  const Type* type;
  ZETASQL_CHECK_OK(catalog_->FindType({descriptor->full_name()}, &type));
  ZETASQL_CHECK(type != nullptr);
  ZETASQL_CHECK(type->IsProto());
  return type->AsProto();
}

const EnumType* SampleCatalog::GetEnumType(
    const google::protobuf::EnumDescriptor* descriptor) {
  const Type* type;
  ZETASQL_CHECK_OK(catalog_->FindType({descriptor->full_name()}, &type));
  ZETASQL_CHECK(type != nullptr);
  ZETASQL_CHECK(type->IsEnum());
  return type->AsEnum();
}

static absl::StatusOr<const Type*> ComputeResultTypeCallbackForNullOfType(
    Catalog* catalog, TypeFactory* type_factory, CycleDetector* cycle_detector,
    const FunctionSignature& signature,
    const std::vector<InputArgumentType>& arguments,
    const AnalyzerOptions& analyzer_options) {
  ZETASQL_RET_CHECK_EQ(arguments.size(), 1);
  ZETASQL_RET_CHECK_EQ(signature.NumConcreteArguments(), arguments.size());
  const LanguageOptions& language_options = analyzer_options.language();
  if (!arguments[0].is_literal() || arguments[0].is_literal_null()) {
    return MakeSqlError()
           << "Argument to NULL_OF_TYPE must be a literal string";
  }
  ZETASQL_RET_CHECK(arguments[0].type()->IsString());
  const Value& value = *arguments[0].literal_value();
  ZETASQL_RET_CHECK(!value.is_null());
  const absl::string_view type_name = value.string_value();
  const TypeKind type_kind =
      Type::ResolveBuiltinTypeNameToKindIfSimple(type_name, language_options);
  if (type_kind == TYPE_UNKNOWN) {
    return MakeSqlError() << "Type not implemented for NULL_OF_TYPE: "
                          << ToStringLiteral(absl::AsciiStrToUpper(type_name));
  }
  // We could parse complex type names here too by calling the type analyzer.
  return type_factory->MakeSimpleType(type_kind);
}

// A ComputeResultTypeCallback that looks for the 'type_name' argument from the
// input list and uses its value to generate the result type.
static absl::StatusOr<const Type*> ComputeResultTypeFromStringArgumentValue(
    Catalog* catalog, TypeFactory* type_factory, CycleDetector* cycle_detector,
    const FunctionSignature& signature,
    const std::vector<InputArgumentType>& arguments,
    const AnalyzerOptions& analyzer_options) {
  ZETASQL_RET_CHECK_EQ(signature.NumConcreteArguments(), arguments.size());
  const LanguageOptions& language_options = analyzer_options.language();
  std::string type_name;
  for (int i = 0; i < arguments.size(); ++i) {
    if (!signature.ConcreteArgument(i).has_argument_name() ||
        signature.ConcreteArgument(i).argument_name() != "type_name") {
      continue;
    }

    const InputArgumentType& arg = arguments[i];
    ZETASQL_RET_CHECK(arg.type()->IsString());
    ZETASQL_RET_CHECK(arg.is_literal());
    if (arg.is_literal_null()) {
      return MakeSqlError() << "Argument 'type_name' cannot be NULL";
    }

    const Value& value = *arg.literal_value();
    ZETASQL_RET_CHECK(!value.is_null());
    type_name = value.string_value();
    const TypeKind type_kind =
        Type::ResolveBuiltinTypeNameToKindIfSimple(type_name, language_options);
    if (type_kind != TYPE_UNKNOWN) {
      return type_factory->MakeSimpleType(type_kind);
    }
    // Try to find the type in catalog.
    const Type* type = nullptr;
    std::vector<std::string> path;
    ZETASQL_RETURN_IF_ERROR(ParseIdentifierPath(type_name, &path));
    if (catalog->FindType(path, &type).ok()) {
      return type;
    }
    break;
  }
  if (!type_name.empty()) {
    return MakeSqlError() << "Invalid type name provided: " << type_name;
  }
  // Use INT64 as return type if not overridden.
  return type_factory->get_int64();
}

void SampleCatalog::LoadCatalog(const LanguageOptions& language_options) {
  // We split these up because these methods (particularly loading builtins)
  // use too much stack and may cause overflows.
  LoadCatalogBuiltins(language_options);
  LoadCatalogImpl(language_options);
}

void SampleCatalog::LoadCatalogBuiltins(
    const LanguageOptions& language_options) {
  // Populate the sample catalog with the ZetaSQL functions.
  catalog_->AddZetaSQLFunctions(
      ZetaSQLBuiltinFunctionOptions(language_options));
}

void SampleCatalog::LoadCatalogImpl(const LanguageOptions& language_options) {
  // Make all proto Descriptors linked into this binary available.
  catalog_->SetDescriptorPool(google::protobuf::DescriptorPool::generated_pool());

  // Create a Catalog called alt_descriptor_pool which has a duplicate copy
  // of all protos in the main catalog, but in a different DescriptorPool.
  alt_descriptor_database_ = absl::make_unique<google::protobuf::DescriptorPoolDatabase>(
      *google::protobuf::DescriptorPool::generated_pool());
  alt_descriptor_pool_ =
      absl::make_unique<google::protobuf::DescriptorPool>(alt_descriptor_database_.get());

  SimpleCatalog* alt_descriptor_pool_catalog =
      catalog_->MakeOwnedSimpleCatalog("alt_descriptor_pool");
  alt_descriptor_pool_catalog->SetDescriptorPool(alt_descriptor_pool_.get());

  // Create a Catalog called ambiguous_has_descriptor_pool which has the
  // (modified) AmbiguousHasPB proto, obtained by changing the
  // "confusing_name_to_be_rewritten" field's name changed to "confusing_name".
  // This makes it such that the modified AmbiguousHasPB has a field called
  // "has_confusing_name" and "confusing_name". Such proto descriptors can occur
  // in practice, but not when C++ code is generated, hence this hack.
  google::protobuf::FileDescriptorProto modified_descriptor_proto;
  zetasql_test__::AmbiguousHasPB::descriptor()->file()->CopyTo(
      &modified_descriptor_proto);
  bool found_message = false;
  for (google::protobuf::DescriptorProto& message_descriptor_proto :
       *modified_descriptor_proto.mutable_message_type()) {
    if (message_descriptor_proto.name() == "AmbiguousHasPB") {
      found_message = true;
      bool found_field = false;
      for (google::protobuf::FieldDescriptorProto& field_descriptor_proto :
           *message_descriptor_proto.mutable_field()) {
        if (field_descriptor_proto.name() == "confusing_name_to_be_rewritten") {
          found_field = true;
          field_descriptor_proto.set_name("confusing_name");
          break;
        }
      }
      ZETASQL_CHECK(found_field) << message_descriptor_proto.DebugString();
    }
  }
  ZETASQL_CHECK(found_message) << modified_descriptor_proto.DebugString();
  ambiguous_has_descriptor_pool_ = absl::make_unique<google::protobuf::DescriptorPool>();
  ambiguous_has_descriptor_pool_->BuildFile(modified_descriptor_proto);

  auto ambiguous_has_descriptor_pool_catalog =
      absl::make_unique<SimpleCatalog>("ambiguous_has_descriptor_pool");
  ambiguous_has_descriptor_pool_catalog->SetDescriptorPool(
      ambiguous_has_descriptor_pool_.get());
  catalog_->AddOwnedCatalog(ambiguous_has_descriptor_pool_catalog.release());

  // Add various kinds of objects to the catalog(s).
  LoadTypes();
  LoadTables();
  LoadConnections();
  LoadProtoTables();
  LoadNestedCatalogs();
  LoadFunctions();
  LoadExtendedSubscriptFunctions();
  LoadFunctionsWithDefaultArguments();
  LoadTemplatedSQLUDFs();
  LoadTableValuedFunctions1();
  LoadTableValuedFunctions2();
  LoadTableValuedFunctionsWithStructArgs();
  LoadTVFWithExtraColumns();
  LoadDescriptorTableValuedFunctions();
  LoadConnectionTableValuedFunctions();
  LoadTableValuedFunctionsWithDeprecationWarnings();
  LoadTemplatedSQLTableValuedFunctions();
  LoadTableValuedFunctionsWithAnonymizationUid();
  LoadProcedures();
  LoadConstants();
  LoadWellKnownLambdaArgFunctions();
  LoadContrivedLambdaArgFunctions();
  LoadSqlFunctions(language_options);
}

void SampleCatalog::LoadTypes() {
  enum_TestEnum_ = GetEnumType(zetasql_test__::TestEnum_descriptor());
  enum_AnotherTestEnum_ =
      GetEnumType(zetasql_test__::AnotherTestEnum_descriptor());
  enum_TestEnumWithAnnotations_ =
      GetEnumType(zetasql_test__::TestEnumWithAnnotations_descriptor());
  proto_KitchenSinkPB_ =
      GetProtoType(zetasql_test__::KitchenSinkPB::descriptor());
  proto_MessageWithKitchenSinkPB_ =
      GetProtoType(zetasql_test__::MessageWithKitchenSinkPB::descriptor());
  proto_CivilTimeTypesSinkPB_ =
      GetProtoType(zetasql_test__::CivilTimeTypesSinkPB::descriptor());
  proto_TestExtraPB_ = GetProtoType(zetasql_test__::TestExtraPB::descriptor());
  proto_abPB_ = GetProtoType(zetasql_test__::TestAbPB::descriptor());
  proto_bcPB_ = GetProtoType(zetasql_test__::TestBcPB::descriptor());
  proto_EmptyMessage_ =
      GetProtoType(zetasql_test__::EmptyMessage::descriptor());
  proto3_KitchenSinkPB_ =
      GetProtoType(zetasql_test__::Proto3KitchenSink::descriptor());
  proto3_MessageWithInvalidMap_ =
      GetProtoType(zetasql_test__::MessageWithInvalidMap::descriptor());
  proto_field_formats_proto_ =
      GetProtoType(zetasql_test__::FieldFormatsProto::descriptor());
  proto_MessageWithMapField_ =
      GetProtoType(zetasql_test__::MessageWithMapField::descriptor());

  // We want to pull AmbiguousHasPB from the descriptor pool where it was
  // modified, not the generated pool.
  const google::protobuf::Descriptor* ambiguous_has_descriptor =
      ambiguous_has_descriptor_pool_->FindMessageTypeByName(
          "zetasql_test__.AmbiguousHasPB");
  ZETASQL_CHECK(ambiguous_has_descriptor);
  ZETASQL_CHECK_OK(
      types_->MakeProtoType(ambiguous_has_descriptor, &proto_ambiguous_has_));

  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"a", types_->get_int32()}, {"b", types_->get_string()}},
      &struct_type_));
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"c", types_->get_int32()}, {"d", struct_type_}},
      &nested_struct_type_));
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"e", types_->get_int32()}, {"f", nested_struct_type_}},
      &doubly_nested_struct_type_));

  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_int32(), &int32array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_int64(), &int64array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_uint32(), &uint32array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_uint64(), &uint64array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_bytes(), &bytes_array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_bool(), &bool_array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_float(), &float_array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_double(), &double_array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_date(), &date_array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_string(), &string_array_type_));
  ZETASQL_CHECK_OK(
      types_->MakeArrayType(types_->get_timestamp(), &timestamp_array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(proto_TestExtraPB_, &proto_array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(struct_type_, &struct_array_type_));
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_json(), &json_array_type_));

  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"x", types_->get_int64()}, {"y", struct_type_},
       {"z", struct_array_type_}}, &struct_with_array_field_type_));

  ZETASQL_CHECK_OK(types_->MakeStructType({{"x", types_->get_int64()}},
                                  &struct_with_one_field_type_));

  const StructType* struct_with_just_kitchen_sink_type;
  ZETASQL_CHECK_OK(types_->MakeStructType({{"kitchen_sink", proto_KitchenSinkPB_}},
                                  &struct_with_just_kitchen_sink_type));
  ZETASQL_CHECK_OK(types_->MakeStructType({{"kitchen_sink", proto_KitchenSinkPB_},
                                   {"s", struct_with_just_kitchen_sink_type}},
                                  &struct_with_kitchen_sink_type_));

  // Add a named struct type for testing name collisions.
  const StructType* name_conflict_type;
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"Key", types_->get_int64()}, {"Value", types_->get_string()}},
      &name_conflict_type));
  catalog_->AddType("NameConflictType", name_conflict_type);

  // Add a simple type for testing alias type from engine catalog
  catalog_->AddType("INT64AliasType", types_->get_int64());
}

namespace {

// Implementation of EvaluatorTableIterator which ignores SetReadTime(), but
// delegates all other methods to an underlying iterator passed to the
// constructor.
class IgnoreReadTimeIterator : public EvaluatorTableIterator {
 public:
  explicit IgnoreReadTimeIterator(
      std::unique_ptr<EvaluatorTableIterator> iterator)
      : iterator_(std::move(iterator)) {}

  int NumColumns() const override { return iterator_->NumColumns(); }
  std::string GetColumnName(int i) const override {
    return iterator_->GetColumnName(i);
  }
  const Type* GetColumnType(int i) const override {
    return iterator_->GetColumnType(i);
  }
  absl::Status SetColumnFilterMap(
      absl::flat_hash_map<int, std::unique_ptr<ColumnFilter>> filter_map)
      override {
    return iterator_->SetColumnFilterMap(std::move(filter_map));
  }
  absl::Status SetReadTime(absl::Time read_time) override {
    return absl::OkStatus();
  }
  bool NextRow() override { return iterator_->NextRow(); }
  const Value& GetValue(int i) const override { return iterator_->GetValue(i); }
  absl::Status Status() const override { return iterator_->Status(); }
  absl::Status Cancel() override { return iterator_->Cancel(); }
  void SetDeadline(absl::Time deadline) override {
    iterator_->SetDeadline(deadline);
  }

 private:
  std::unique_ptr<EvaluatorTableIterator> iterator_;
};

// Minimal table implementation to support testing of FOR SYSTEM TIME AS OF.
// This is just a modified version of SimpleTable which ignores the read time.
class SimpleTableWithReadTimeIgnored : public SimpleTable {
 public:
  SimpleTableWithReadTimeIgnored(const std::string& name,
                                 const std::vector<NameAndType>& columns,
                                 const int64_t id = 0)
      : SimpleTable(name, columns, id) {}

  absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
  CreateEvaluatorTableIterator(
      absl::Span<const int> column_idxs) const override {
    std::unique_ptr<EvaluatorTableIterator> iterator;
    ZETASQL_ASSIGN_OR_RETURN(iterator,
                     SimpleTable::CreateEvaluatorTableIterator(column_idxs));
    return absl::make_unique<IgnoreReadTimeIterator>(std::move(iterator));
  }
};

}  // namespace

void SampleCatalog::LoadTables() {
  SimpleTable* value_table = new SimpleTable(
      "Value", {{"Value", types_->get_int64()},
                // to test while() loop in SQLBuilder::GetScanAlias
                {"Value_1", types_->get_int64()}});
  AddOwnedTable(value_table);

  SimpleTable* key_value_table = new SimpleTable(
      "KeyValue",
      {{"Key", types_->get_int64()}, {"Value", types_->get_string()}});
  AddOwnedTable(key_value_table);
  key_value_table_ = key_value_table;


  SimpleTable* multiple_columns_table =
      new SimpleTable("MultipleColumns", {{"int_a", types_->get_int64()},
                                          {"string_a", types_->get_string()},
                                          {"int_b", types_->get_int64()},
                                          {"string_b", types_->get_string()},
                                          {"int_c", types_->get_int64()},
                                          {"int_d", types_->get_int64()}});
  AddOwnedTable(multiple_columns_table);

  SimpleTable* ab_table = new SimpleTable(
      "abTable",
      {{"a", types_->get_int64()}, {"b", types_->get_string()}});
  AddOwnedTable(ab_table);
  SimpleTable* bc_table = new SimpleTable(
      "bcTable",
      {{"b", types_->get_int64()}, {"c", types_->get_string()}});
  AddOwnedTable(bc_table);

  SimpleTable* key_value_table_read_time_ignored =
      new SimpleTableWithReadTimeIgnored(
          "KeyValueReadTimeIgnored",
          {{"Key", types_->get_int64()}, {"Value", types_->get_string()}});
  AddOwnedTable(key_value_table_read_time_ignored);

  SimpleTable* another_key_value = new SimpleTable(
      "AnotherKeyValue",
      {{"Key", types_->get_int64()}, {"value", types_->get_string()}});
  AddOwnedTable(another_key_value);

  const SimpleModel* one_double_model =
      new SimpleModel("OneDoubleModel", {{"a", types_->get_double()}},
                      {{"label", types_->get_double()}});
  const SimpleModel* one_double_one_string_model = new SimpleModel(
      "OneDoubleOneStringModel",
      {{"a", types_->get_double()}, {"b", types_->get_string()}},
      {{"label", types_->get_double()}});
  const SimpleModel* one_double_two_output_model = new SimpleModel(
      "OneDoubleTwoOutputModel", {{"a", types_->get_double()}},
      {{"label1", types_->get_double()}, {"label2", types_->get_double()}});
  catalog_->AddOwnedModel(one_double_model);
  catalog_->AddOwnedModel(one_double_one_string_model);
  catalog_->AddOwnedModel(one_double_two_output_model);

  SimpleTable* key_value2_table = new SimpleTable(
      "KeyValue2",
      {{"Key", types_->get_int64()}, {"Value2", types_->get_string()}});
  AddOwnedTable(key_value2_table);

  SimpleTable* space_value_table = new SimpleTable(
      " Value",
      {{" Key", types_->get_int64()}, {" Value", types_->get_string()}});
  AddOwnedTable(space_value_table);

  auto collatedTable = new SimpleTable("CollatedTable");
  const AnnotationMap* annotation_map_string_ci;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(types_->get_string());
    annotation_map->SetAnnotation<CollationAnnotation>(
        SimpleValue::String("und:ci"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(annotation_map_string_ci,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  const AnnotationMap* annotation_map_string_binary;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(types_->get_string());
    annotation_map->SetAnnotation<CollationAnnotation>(
        SimpleValue::String("binary"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(annotation_map_string_binary,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  const AnnotationMap* annotation_map_struct_with_string_ci;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(struct_type_);
    annotation_map->AsStructMap()
        ->mutable_field(1)
        ->SetAnnotation<CollationAnnotation>(SimpleValue::String("und:ci"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(annotation_map_struct_with_string_ci,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  const AnnotationMap* annotation_map_array_with_string_ci;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(string_array_type_);
    annotation_map->AsArrayMap()
        ->mutable_element()
        ->SetAnnotation<CollationAnnotation>(SimpleValue::String("und:ci"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(annotation_map_array_with_string_ci,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  auto string_ci = new SimpleColumn(
      collatedTable->Name(), "string_ci",
      AnnotatedType(types_->get_string(), annotation_map_string_ci));

  auto string_cs = new SimpleColumn(
      collatedTable->Name(), "string_binary",
      AnnotatedType(types_->get_string(), annotation_map_string_binary));

  auto struct_ci = new SimpleColumn(
      collatedTable->Name(), "struct_with_string_ci",
      AnnotatedType(struct_type_, annotation_map_struct_with_string_ci));

  auto array_ci = new SimpleColumn(
      collatedTable->Name(), "array_with_string_ci",
      AnnotatedType(string_array_type_, annotation_map_array_with_string_ci));

  ZETASQL_CHECK_OK(collatedTable->AddColumn(string_ci, /*is_owned=*/true));
  ZETASQL_CHECK_OK(collatedTable->AddColumn(string_cs, /*is_owned=*/true));
  ZETASQL_CHECK_OK(collatedTable->AddColumn(struct_ci, /*is_owned=*/true));
  ZETASQL_CHECK_OK(collatedTable->AddColumn(array_ci, /*is_owned=*/true));
  AddOwnedTable(collatedTable);

  auto complex_collated_table = new SimpleTable("ComplexCollatedTable");

  const StructType* struct_with_bool_string_type;
  // We let the first field have bool (rather than int32_t) type to avoid
  // implicitly coercing ARRAY<STRUCT<a INT64, b STRING>> to
  // ARRAY<STRUCT<a INT32, b STRING>> in the testing, which is currently not
  // supported.
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"a", types_->get_bool()}, {"b", types_->get_string()}},
      &struct_with_bool_string_type));
  const ArrayType* array_of_struct_type;
  ZETASQL_CHECK_OK(types_->MakeArrayType(struct_with_bool_string_type,
                                 &array_of_struct_type));
  const StructType* struct_with_array_of_struct_type;
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"a", types_->get_int32()}, {"b", array_of_struct_type}},
      &struct_with_array_of_struct_type));

  const StructType* struct_of_multiple_string_type;
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"a", types_->get_string()}, {"b", types_->get_string()}},
      &struct_of_multiple_string_type));

  const AnnotationMap* annotation_map_struct_with_array_of_struct_ci;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(struct_with_array_of_struct_type);
    annotation_map->AsStructMap()
        ->mutable_field(1)
        ->AsArrayMap()
        ->mutable_element()
        ->AsStructMap()
        ->mutable_field(1)
        ->SetAnnotation<CollationAnnotation>(SimpleValue::String("und:ci"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(annotation_map_struct_with_array_of_struct_ci,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  const AnnotationMap* annotation_map_struct_of_struct_ci;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(nested_struct_type_);
    annotation_map->AsStructMap()
        ->mutable_field(1)
        ->AsStructMap()
        ->mutable_field(1)
        ->SetAnnotation<CollationAnnotation>(SimpleValue::String("und:ci"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(annotation_map_struct_of_struct_ci,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  const AnnotationMap* annotation_map_struct_with_string_ci_binary;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(struct_of_multiple_string_type);
    annotation_map->AsStructMap()
        ->mutable_field(0)
        ->SetAnnotation<CollationAnnotation>(SimpleValue::String("und:ci"));
    annotation_map->AsStructMap()
        ->mutable_field(1)
        ->SetAnnotation<CollationAnnotation>(SimpleValue::String("binary"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(annotation_map_struct_with_string_ci_binary,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  auto string_no_collation = new SimpleColumn(
      complex_collated_table->Name(), "string_no_collation",
      AnnotatedType(types_->get_string(), /*annotation_map=*/nullptr));

  auto struct_with_array_of_struct_ci = new SimpleColumn(
      complex_collated_table->Name(), "struct_with_array_of_struct_ci",
      AnnotatedType(struct_with_array_of_struct_type,
                    annotation_map_struct_with_array_of_struct_ci));

  auto struct_of_struct_ci = new SimpleColumn(
      complex_collated_table->Name(), "struct_of_struct_ci",
      AnnotatedType(nested_struct_type_, annotation_map_struct_of_struct_ci));

  auto struct_with_string_ci_binary = new SimpleColumn(
      complex_collated_table->Name(), "struct_with_string_ci_binary",
      AnnotatedType(struct_of_multiple_string_type,
                    annotation_map_struct_with_string_ci_binary));

  ZETASQL_CHECK_OK(complex_collated_table->AddColumn(string_no_collation,
                                             /*is_owned=*/true));
  ZETASQL_CHECK_OK(complex_collated_table->AddColumn(struct_with_array_of_struct_ci,
                                             /*is_owned=*/true));
  ZETASQL_CHECK_OK(complex_collated_table->AddColumn(struct_of_struct_ci,
                                             /*is_owned=*/true));
  ZETASQL_CHECK_OK(complex_collated_table->AddColumn(struct_with_string_ci_binary,
                                             /*is_owned=*/true));
  AddOwnedTable(complex_collated_table);

  auto generic_annotation_test_table = new SimpleTable("AnnotatedTable");

  const AnnotationMap* generic_test_annotation_map_for_string_field;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(types_->get_string());
    annotation_map->SetAnnotation<SampleAnnotation>(SimpleValue::Int64(0));
    ZETASQL_ASSERT_OK_AND_ASSIGN(generic_test_annotation_map_for_string_field,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  const AnnotationMap* generic_test_annotation_map_for_int_field;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(types_->get_int64());
    annotation_map->SetAnnotation<SampleAnnotation>(SimpleValue::Int64(1));
    ZETASQL_ASSERT_OK_AND_ASSIGN(generic_test_annotation_map_for_int_field,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  const AnnotationMap* generic_annotation_map_for_struct_field;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(struct_type_);
    annotation_map->AsStructMap()
        ->mutable_field(1)
        ->SetAnnotation<SampleAnnotation>(SimpleValue::Int64(2));
    ZETASQL_ASSERT_OK_AND_ASSIGN(generic_annotation_map_for_struct_field,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  const AnnotationMap* generic_test_annotation_map_for_string_array_field;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(string_array_type_);
    annotation_map->SetAnnotation<SampleAnnotation>(SimpleValue::Int64(3));
    ZETASQL_ASSERT_OK_AND_ASSIGN(generic_test_annotation_map_for_string_array_field,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  const AnnotationMap* generic_annotation_map_for_nested_struct_field;
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(nested_struct_type_);
    annotation_map->AsStructMap()
        ->mutable_field(0)
        ->SetAnnotation<SampleAnnotation>(SimpleValue::Int64(4));
    annotation_map->AsStructMap()
        ->mutable_field(1)
        ->AsStructMap()
        ->mutable_field(1)
        ->SetAnnotation<SampleAnnotation>(SimpleValue::Int64(5));
    ZETASQL_ASSERT_OK_AND_ASSIGN(generic_annotation_map_for_nested_struct_field,
                         types_->TakeOwnership(std::move(annotation_map)));
  }

  auto generic_annotation_test_string_column = new SimpleColumn(
      generic_annotation_test_table->Name(), "string",
      AnnotatedType(types_->get_string(),
                    generic_test_annotation_map_for_string_field));
  auto generic_annotation_test_int_column = new SimpleColumn(
      generic_annotation_test_table->Name(), "int_a",
      AnnotatedType(types_->get_int64(),
                    generic_test_annotation_map_for_int_field));
  auto generic_annotation_test_int_column_unannotated = new SimpleColumn(
      generic_annotation_test_table->Name(), "int_b", types_->get_int64());
  auto generic_annotation_test_struct_column = new SimpleColumn(
      generic_annotation_test_table->Name(), "struct_a",
      AnnotatedType(struct_type_, generic_annotation_map_for_struct_field));
  auto generic_annotation_test_string_array_column = new SimpleColumn(
      generic_annotation_test_table->Name(), "string_array",
      AnnotatedType(string_array_type_,
                    generic_test_annotation_map_for_string_array_field));
  auto generic_annotation_test_nested_struct_column = new SimpleColumn(
      generic_annotation_test_table->Name(), "struct_b",
      AnnotatedType(nested_struct_type_,
                    generic_annotation_map_for_nested_struct_field));

  ZETASQL_CHECK_OK(generic_annotation_test_table->AddColumn(
      generic_annotation_test_string_column, /*is_owned=*/true));
  ZETASQL_CHECK_OK(generic_annotation_test_table->AddColumn(
      generic_annotation_test_int_column, /*is_owned=*/true));
  ZETASQL_CHECK_OK(generic_annotation_test_table->AddColumn(
      generic_annotation_test_int_column_unannotated, /*is_owned=*/true));
  ZETASQL_CHECK_OK(generic_annotation_test_table->AddColumn(
      generic_annotation_test_struct_column, /*is_owned=*/true));
  ZETASQL_CHECK_OK(generic_annotation_test_table->AddColumn(
      generic_annotation_test_string_array_column, /*is_owned=*/true));
  ZETASQL_CHECK_OK(generic_annotation_test_table->AddColumn(
      generic_annotation_test_nested_struct_column, /*is_owned=*/true));
  AddOwnedTable(generic_annotation_test_table);

  AddOwnedTable(new SimpleTable(
      "SimpleTypes",
      {{"int32", types_->get_int32()},
       {"int64", types_->get_int64()},
       {"uint32", types_->get_uint32()},
       {"uint64", types_->get_uint64()},
       {"string", types_->get_string()},
       {"bytes", types_->get_bytes()},
       {"bool", types_->get_bool()},
       {"float", types_->get_float()},
       {"double", types_->get_double()},
       {"date", types_->get_date()},
       // These types were removed, but we keep the fields in the sample
       // table in order not to disturb analyzer test results too much (all
       // the variable ids would change if we were to remove them).
       // TODO: Remove them when all other changes settled down.
       {"timestamp_seconds", types_->get_timestamp()},
       {"timestamp_millis", types_->get_timestamp()},
       {"timestamp_micros", types_->get_timestamp()},
       {"timestamp_nanos", types_->get_timestamp()},
       // Real types resume here.
       {"timestamp", types_->get_timestamp()},
       {"numeric", types_->get_numeric()},
       {"bignumeric", types_->get_bignumeric()},
       {"json", types_->get_json()}}));

  {
    auto simple_table_with_uid =
        absl::make_unique<SimpleTable>("SimpleTypesWithAnonymizationUid",
                                       std::vector<SimpleTable::NameAndType>{
                                           {"int32", types_->get_int32()},
                                           {"int64", types_->get_int64()},
                                           {"uint32", types_->get_uint32()},
                                           {"uint64", types_->get_uint64()},
                                           {"string", types_->get_string()},
                                           {"bytes", types_->get_bytes()},
                                           {"bool", types_->get_bool()},
                                           {"float", types_->get_float()},
                                           {"double", types_->get_double()},
                                           {"date", types_->get_date()},
                                           {"uid", types_->get_int64()},
                                           {"numeric", types_->get_numeric()}});
    ZETASQL_CHECK_OK(simple_table_with_uid->SetAnonymizationInfo("uid"));
    AddOwnedTable(simple_table_with_uid.release());
  }

  {
    auto array_table_with_uid = absl::make_unique<SimpleTable>(
        "ArrayWithAnonymizationUid", std::vector<SimpleTable::NameAndType>{
                                         {"int64_array", int64array_type_},
                                         {"double_array", double_array_type_},
                                         {"uid", types_->get_int64()}});
    ZETASQL_CHECK_OK(array_table_with_uid->SetAnonymizationInfo("uid"));
    AddOwnedTable(array_table_with_uid.release());
  }

  {
    auto table_with_string_uid = absl::make_unique<SimpleTable>(
        "T1StringAnonymizationUid",
        std::vector<SimpleTable::NameAndType>{{"uid", types_->get_string()}});
    ZETASQL_CHECK_OK(table_with_string_uid->SetAnonymizationInfo("uid"));
    AddOwnedTable(table_with_string_uid.release());
  }

  {
    auto table_with_string_uid = absl::make_unique<SimpleTable>(
        "T2StringAnonymizationUid",
        std::vector<SimpleTable::NameAndType>{{"uid", types_->get_string()}});
    ZETASQL_CHECK_OK(table_with_string_uid->SetAnonymizationInfo("uid"));
    AddOwnedTable(table_with_string_uid.release());
  }

  {
    auto table_with_proto_uid = absl::make_unique<SimpleTable>(
        "ProtoAnonymizationUid",
        std::vector<SimpleTable::NameAndType>{{"uid", proto_KitchenSinkPB_}});
    ZETASQL_CHECK_OK(table_with_proto_uid->SetAnonymizationInfo("uid"));
    AddOwnedTable(table_with_proto_uid.release());
  }

  {
    auto value_table_with_uid = absl::make_unique<SimpleTable>(
        "KitchenSinkWithUidValueTable", proto_KitchenSinkPB_);
    ZETASQL_CHECK_OK(value_table_with_uid->SetAnonymizationInfo("string_val"));
    AddOwnedTable(value_table_with_uid.release());
  }

  {
    auto value_table_with_uid = absl::make_unique<SimpleTable>(
        "TestStructWithUidValueTable", struct_type_);
    ZETASQL_CHECK_OK(value_table_with_uid->SetAnonymizationInfo("a"));
    AddOwnedTable(value_table_with_uid.release());
  }

  {
    auto value_table_with_doubly_nested_uid = absl::make_unique<SimpleTable>(
        "TestWithDoublyNestedStructUidValueTable", doubly_nested_struct_type_);
    ZETASQL_CHECK_OK(value_table_with_doubly_nested_uid->SetAnonymizationInfo(
        {"f", "d", "a"}));
    AddOwnedTable(value_table_with_doubly_nested_uid.release());
  }

  {
    auto value_table_with_proto_uid = absl::make_unique<SimpleTable>(
        "TestWithProtoUidValueTable", proto_MessageWithKitchenSinkPB_);
    ZETASQL_CHECK_OK(value_table_with_proto_uid->SetAnonymizationInfo(
        {"kitchen_sink", "nested_value", "nested_int64"}));
    AddOwnedTable(value_table_with_proto_uid.release());
  }

  {
    auto value_table_with_proto_uid_of_wrong_type =
        absl::make_unique<SimpleTable>("TestWithWrongTypeProtoUidValueTable",
                                       proto_MessageWithKitchenSinkPB_);
    ZETASQL_CHECK_OK(value_table_with_proto_uid_of_wrong_type->SetAnonymizationInfo(
        std::vector<std::string>({"kitchen_sink", "nested_value"})));
    AddOwnedTable(value_table_with_proto_uid_of_wrong_type.release());
  }

  AddOwnedTable(
      new SimpleTable("GeographyTable", {{"key", types_->get_int64()},
                                         {"text", types_->get_string()},
                                         {"geo1", types_->get_geography()},
                                         {"geo2", types_->get_geography()}}));

  AddOwnedTable(new SimpleTable("NumericTypeTable",
                                {{"numeric_col", types_->get_numeric()}}));

  AddOwnedTable(new SimpleTable(
      "BigNumericTypeTable", {{"bignumeric_col", types_->get_bignumeric()}}));

  AddOwnedTable(
      new SimpleTable("JSONTable", {{"json_col", types_->get_json()}}));

  SimpleTable* two_integers = new SimpleTable(
      "TwoIntegers",
      {{"key", types_->get_int64()}, {"value", types_->get_int64()}});
  ZETASQL_CHECK_OK(two_integers->SetPrimaryKey({0}));
  AddOwnedTable(two_integers);

  SimpleTable* four_integers =
      new SimpleTable("FourIntegers", {{"key1", types_->get_int64()},
                                       {"value1", types_->get_int64()},
                                       {"key2", types_->get_int64()},
                                       {"value2", types_->get_int64()}});
  ZETASQL_CHECK_OK(four_integers->SetPrimaryKey({0, 2}));
  AddOwnedTable(four_integers);

  // Tables with no columns are legal.
  AddOwnedTable(new SimpleTable("NoColumns"));

  // Add tables for testing name collisions.
  AddOwnedTable(
      new SimpleTable("NameConflictTable", {{"key", types_->get_int32()}}));
  AddOwnedTable(new SimpleTable(
      "name_conflict_table", {{"a", types_->get_string()},
                              {"name_conflict_field", types_->get_string()}}));
}

void SampleCatalog::LoadProtoTables() {
  // Add a named struct type.
  const StructType* struct_TestStruct;
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"Key", types_->get_int64()}, {"Value", types_->get_string()}},
      &struct_TestStruct));
  catalog_->AddType("TestStruct", struct_TestStruct);

  const StructType* struct_AnotherTestStruct;
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"K", types_->get_int32()}, {"v", types_->get_bytes()}},
      &struct_AnotherTestStruct));
  catalog_->AddType("AnotherTestStruct", struct_TestStruct);

  AddOwnedTable(
      new SimpleTable("TestTable", {{"key", types_->get_int32()},
                                    {"TestEnum", enum_TestEnum_},
                                    {"KitchenSink", proto_KitchenSinkPB_}}));

  // We want to be sure that AmbiguousHasTestTable is not the first table
  // serialized by SimpleCatalog::Serialize. See b/125914476 for more detail.
  AddOwnedTable(new SimpleTable(
      "ZZZ_AmbiguousHasTestTable",
      {{"key", types_->get_int32()}, {"AmbiguousHas", proto_ambiguous_has_}}));

  AddOwnedTable(
      new SimpleTable("CivilTimeTestTable",
                      {{"key", types_->get_int32()},
                       {"CivilTimeTypesSink", proto_CivilTimeTypesSinkPB_}}));

  AddOwnedTable(
      new SimpleTable("FieldFormatsTable",
                      {{"key", types_->get_int32()},
                       {"FieldFormatsProto", proto_field_formats_proto_}}));

  // EnumTable has two pseudo-columns Filename and RowId.
  AddOwnedTable(new SimpleTable(
      "EnumTable",
      {new SimpleColumn("EnumTable", "key", types_->get_int32()),
       new SimpleColumn("EnumTable", "TestEnum", enum_TestEnum_),
       new SimpleColumn("EnumTable", "AnotherTestEnum", enum_AnotherTestEnum_),
       new SimpleColumn("EnumTable", "Filename", types_->get_string(),
                        true /* is_pseudo_column */),
       new SimpleColumn("EnumTable", "RowId", types_->get_bytes(),
                        true /* is_pseudo_column */)},
      true /* take_ownership */));

  AddOwnedTable(new SimpleTable(
      "MapFieldTable", {{"key", types_->get_int32()},
                        {"MessageWithMapField", proto_MessageWithMapField_}}));

  AddOwnedTable(new SimpleTable(
      "Proto3Table", {{"key", types_->get_int32()},
                      {"Proto3KitchenSink", proto3_KitchenSinkPB_}}));

  AddOwnedTable(new SimpleTable(
      "Proto3InvalidMapTable",
      {{"key", types_->get_int32()},
       {"MessageWithInvalidMap", proto3_MessageWithInvalidMap_}}));

  // This table only has pseudo-columns.
  AddOwnedTable(new SimpleTable(
      "AllPseudoColumns",
      {
          new SimpleColumn("AllPseudoColumns", "Key", types_->get_int32(),
                           true /* is_pseudo_column */),
          new SimpleColumn("AllPseudoColumns", "Value", types_->get_string(),
                           true /* is_pseudo_column */),
      },
      true /* take_ownership */));

  // Another table with only pseudo-columns, this time with a repeated field. We
  // don't extend AllPseudoColumns to avoid breaking pre-existing tests.
  AddOwnedTable(new SimpleTable(
      "AllPseudoColumnsWithRepeated",
      {
          new SimpleColumn("AllPseudoColumns", "Key", types_->get_int32(),
                           true /* is_pseudo_column */),
          new SimpleColumn("AllPseudoColumns", "Value", types_->get_string(),
                           true /* is_pseudo_column */),
          new SimpleColumn("AllPseudoColumns", "RepeatedValue",
                           string_array_type_, true /* is_pseudo_column */),
      },
      true /* take_ownership */));

  {
    // This table has an anonymous pseudo-column, which should be inaccessible.
    auto table = new SimpleTable("AnonymousPseudoColumn");
    AddOwnedTable(table);
    ZETASQL_CHECK_OK(table->set_allow_anonymous_column_name(true));
    ZETASQL_CHECK_OK(table->AddColumn(
        new SimpleColumn("AnonymousPseudoColumn", "key", types_->get_int32()),
        true /* take_ownership */));
    ZETASQL_CHECK_OK(table->AddColumn(
        new SimpleColumn("AnonymousPseudoColumn", "", types_->get_string(),
                         true /* is_pseudo_column */),
        true /* take_ownership */));
  }

  AddOwnedTable(new SimpleTable(
      "AllNonKeysNonWritable",
      {
          new SimpleColumn("AllNonKeysNonWritable", "Key", types_->get_int32(),
                           /* is_pseudo_column = */ false,
                           /* is_writable_column = */ true),
          new SimpleColumn("AllNonKeysNonWritable", "Value",
                           types_->get_string(),
                           /* is_pseudo_column = */ false,
                           /* is_writable_column = */ false),
          new SimpleColumn("AllNonKeysNonWritable", "RepeatedValue",
                           int32array_type_, /* is_pseudo_column = */ false,
                           /* is_writable_column = */ false),
          new SimpleColumn("AllNonKeysNonWritable", "ProtoValue",
                           proto_TestExtraPB_, /* is_pseudo_column = */ false,
                           /* is_writable_column = */ false),
          new SimpleColumn("AllNonKeysNonWritable", "StructValue", struct_type_,
                           /* is_pseudo_column = */ false,
                           /* is_writable_column = */ false),
      },
      true /* take_ownership */));

  SimpleTable* complex_types =
      new SimpleTable("ComplexTypes", {{"key", types_->get_int32()},
                                       {"TestEnum", enum_TestEnum_},
                                       {"KitchenSink", proto_KitchenSinkPB_},
                                       {"Int32Array", int32array_type_},
                                       {"TestStruct", nested_struct_type_},
                                       {"TestProto", proto_TestExtraPB_}});
  ZETASQL_CHECK_OK(complex_types->SetPrimaryKey({0}));
  AddOwnedTable(complex_types);

  AddOwnedTable(new SimpleTable(
      "MoreComplexTypes",
      {{"key", types_->get_int32()},
       {"ArrayOfStruct", struct_array_type_},
       {"StructOfArrayOfStruct", struct_with_array_field_type_}}));

  AddOwnedTable(new SimpleTable("StructWithKitchenSinkTable",
                                {{"kitchen_sink", proto_KitchenSinkPB_},
                                 {"s", struct_with_kitchen_sink_type_}}));

  AddOwnedTable(
      new SimpleTable("DoublyNestedStructTable",
                      {{"key", types_->get_int32()},
                       {"doubly_nested_struct", doubly_nested_struct_type_}}));

  AddOwnedTable(new SimpleTable("KitchenSinkValueTable", proto_KitchenSinkPB_));

  AddOwnedTable(new SimpleTable("MessageWithKitchenSinkValueTable",
                                proto_MessageWithKitchenSinkPB_));

  AddOwnedTable(new SimpleTable("EmptyMessageValueTable", proto_EmptyMessage_));

  catalog_->AddOwnedTable(
      new SimpleTable("TestExtraPBValueTable", proto_TestExtraPB_));

  catalog_->AddOwnedTable(
      new SimpleTable("TestAbPBValueTable", proto_abPB_));

  catalog_->AddOwnedTable(
      new SimpleTable("TestBcPBValueTable", proto_bcPB_));

  catalog_->AddOwnedTable(new SimpleTable("TestBcPBValueProtoTable",
                                          {{"value", proto_bcPB_},
                                           {"Filename", types_->get_string()},
                                           {"RowId", types_->get_int64()}}));

  // TestExtraValueTable also has pseudo-columns Filename and RowID.
  SimpleTable* extra_value_table;
  AddOwnedTable((
      extra_value_table = new SimpleTable(
          "TestExtraValueTable",
          {new SimpleColumn("TestExtraValueTable", "value", proto_TestExtraPB_),
           new SimpleColumn("TestExtraValueTable", "Filename",
                            types_->get_string(), true /* is_pseudo_column */),
           new SimpleColumn("TestExtraValueTable", "RowId", types_->get_bytes(),
                            true /* is_pseudo_column */)},
          true /* take_ownership */)));
  extra_value_table->set_is_value_table(true);

  // AmbiguousFieldValueTable has a pseudo-column int32_val1 that is
  // also a field name.
  SimpleTable* ambiguous_field_value_table;
  AddOwnedTable((ambiguous_field_value_table = new SimpleTable(
                     "AmbiguousFieldValueTable",
                     {
                         new SimpleColumn("TestExtraValueTable", "value",
                                          proto_TestExtraPB_),
                         new SimpleColumn("TestExtraValueTable", "int32_val1",
                                          types_->get_string(),
                                          true /* is_pseudo_column */),
                     },
                     true /* take_ownership */)));
  ambiguous_field_value_table->set_is_value_table(true);

  SimpleTable* int64_value_table;
  AddOwnedTable(int64_value_table = new SimpleTable(
                    "Int64ValueTable",
                    {new SimpleColumn("Int64ValueTable", "IntValue",
                                      types_->get_int64())},
                    /*take_ownership=*/true));
  int64_value_table->set_is_value_table(true);
  ZETASQL_CHECK_OK(int64_value_table->SetPrimaryKey({0}));

  AddOwnedTable(new SimpleTable(
      "ArrayTypes",
      {{"Int32Array", int32array_type_},
       {"Int64Array", int64array_type_},
       {"UInt32Array", uint32array_type_},
       {"UInt64Array", uint64array_type_},
       {"StringArray", string_array_type_},
       {"BytesArray", bytes_array_type_},
       {"BoolArray", bool_array_type_},
       {"FloatArray", float_array_type_},
       {"DoubleArray", double_array_type_},
       {"DateArray", date_array_type_},
       // These corresponding legacy types were removed, but we keep the fields
       // in the sample table in order not to disturb analyzer test results too
       // much (all the variable ids would change if we were to remove them).
       // TODO: Eventually remove these.
       {"TimestampSecondsArray", timestamp_array_type_},
       {"TimestampMillisArray", timestamp_array_type_},
       {"TimestampMicrosArray", timestamp_array_type_},
       // Real types resume here.
       {"TimestampArray", timestamp_array_type_},
       {"ProtoArray", proto_array_type_},
       {"StructArray", struct_array_type_},
       {"JsonArray", json_array_type_}}));

  const EnumType* enum_TestEnum =
      GetEnumType(zetasql_test__::TestEnum_descriptor());
  AddOwnedTable(new SimpleTable("SimpleTypesWithStruct",
                                {{"key", types_->get_int32()},
                                 {"TestEnum", enum_TestEnum},
                                 {"TestStruct", nested_struct_type_}}));

  const ProtoType* proto_recursive_type =
      GetProtoType(zetasql_test__::RecursivePB::descriptor());
  AddOwnedTable(new SimpleTable("RecursivePBTable",
                                {{"RecursivePB", proto_recursive_type}}));

  AddOwnedTable(new SimpleTable(
      "KeywordTable",
      {{"current_date", types_->get_date()},
       {"current_timestamp", types_->get_timestamp()},
       // The corresponding legacy types were removed, but we keep the fields in
       // the sample table in order not to disturb analyzer test results too
       // much (all the variable ids would change if we were to remove them).
       // TODO: Eventually remove these.
       {"current_timestamp_seconds", types_->get_timestamp()},
       {"current_timestamp_millis", types_->get_timestamp()},
       {"current_timestamp_micros", types_->get_timestamp()}}));

  AddOwnedTable(new SimpleTable("TestStructValueTable", struct_type_));

  AddOwnedTable(new SimpleTable("TestNestedStructValueTable",
                                doubly_nested_struct_type_));

  AddOwnedTable(new SimpleTable("StructWithOneFieldValueTable",
                                struct_with_one_field_type_));

  AddOwnedTable(new SimpleTable("Int32ValueTable", types_->get_int32()));

  AddOwnedTable(new SimpleTable("Int32ArrayValueTable", int32array_type_));

  AddOwnedTable(
      new SimpleTable("AnnotatedEnumTable", enum_TestEnumWithAnnotations_));
}

void SampleCatalog::LoadNestedCatalogs() {
  SimpleCatalog* nested_catalog =
      catalog_->MakeOwnedSimpleCatalog("nested_catalog");

  // Add nested_catalog with some tables with the same and different names.
  nested_catalog->AddTable(key_value_table_);
  nested_catalog->AddTable("NestedKeyValue", key_value_table_);

  // Add nested_catalog with some connections with the same and different names.
  nested_catalog->AddConnection(owned_connections_.begin()->second.get());
  nested_catalog->AddConnection("NestedConnection",
                                owned_connections_.begin()->second.get());

  // Add recursive_catalog which points back to the same catalog.
  // This allows resolving names like
  //   recursive_catalog.recursive_catalog.recursive_catalog.TestTable
  catalog_->AddCatalog("recursive_catalog", catalog_.get());

  // Add a function to the nested catalog:
  //   nested_catalog.nested_function(<int64_t>) -> <int64_t>
  FunctionSignature signature(
      {types_->get_int64(), {types_->get_int64()}, /*context_id=*/-1});
  std::vector<std::string> function_name_path = {"nested_catalog",
                                                 "nested_function"};
  Function* function =
      new Function(function_name_path, "sample_functions",
                   Function::SCALAR, {signature});
  nested_catalog->AddOwnedFunction(function);
  // Add a procedure to the nested catalog:
  //   nested_catalog.nested_procedure(<int64_t>) -> <int64_t>
  Procedure* procedure =
      new Procedure({"nested_catalog", "nested_procedure"}, signature);
  nested_catalog->AddOwnedProcedure(procedure);

  // Add a doubly nested catalog, and a function to the doubly nested catalog:
  //   nested_catalog.nested_nested_catalog.nested_function(<int64_t>) -> <int64_t>
  SimpleCatalog* nested_nested_catalog =
      nested_catalog->MakeOwnedSimpleCatalog("nested_nested_catalog");
  function_name_path =
      {"nested_catalog", "nested_nested_catalog", "nested_function"};
  function = new Function(function_name_path, "sample_functions",
                          Function::SCALAR, {signature});
  nested_nested_catalog->AddOwnedFunction(function);

  // Add a struct-typed constant to the doubly nested catalog.
  const StructType* nested_constant_struct_type;
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"eee", types_->get_int32()}, {"fff", nested_struct_type_}},
      &nested_constant_struct_type));
  std::unique_ptr<SimpleConstant> constant_struct;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"nested_catalog", "nested_nested_catalog",
                               "TestConstantStruct"},
      Value::Struct(nested_constant_struct_type,
                    {Value::Int32(-3456),
                     Value::Struct(nested_struct_type_,
                                   {Value::Int32(3),
                                    Value::Struct(struct_type_,
                                                  {Value::Int32(223),
                                                   Value::String("foo")})})}),
      &constant_struct));
  nested_nested_catalog->AddOwnedConstant(constant_struct.release());

  // Add an enum and a proto to the nested catalog.
  nested_catalog->AddType(enum_TestEnum_->enum_descriptor()->full_name(),
                          enum_TestEnum_);
  nested_catalog->AddType(proto_KitchenSinkPB_->descriptor()->full_name(),
                          proto_KitchenSinkPB_);
  nested_catalog->AddType(
      proto_CivilTimeTypesSinkPB_->descriptor()->full_name(),
      proto_CivilTimeTypesSinkPB_);

  // Add TVFs to the nested catalogs. We use this to test name resolution during
  // serialization/deserialization.
  const std::string kColumnNameKey = "key";
  TVFRelation::ColumnList columns;
  columns.emplace_back(kColumnNameKey, types::Int64Type());
  TVFRelation single_key_col_schema(columns);

  int context_id = -1;
  nested_catalog->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"nested_catalog", "nested_tvf_one"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              single_key_col_schema,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kColumnNameKey, zetasql::types::Int64Type()}}),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id),
      single_key_col_schema));
  nested_nested_catalog->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"nested_catalog", "nested_nested_catalog", "nested_tvf_two"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              single_key_col_schema,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kColumnNameKey, zetasql::types::Int64Type()}}),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id),
      single_key_col_schema));

  // Load a nested catalog with a constant whose names conflict with a table
  // and its field.
  SimpleCatalog* name_conflict_catalog =
      catalog_->MakeOwnedSimpleCatalog("name_conflict_table");
  std::unique_ptr<SimpleConstant> constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      {"name_conflict_table", "name_conflict_field"}, Value::Bool(false),
      &constant));
  name_conflict_catalog->AddOwnedConstant(constant.release());

  // Add <nested_catalog_with_constant> for testing named constants in catalogs.
  SimpleCatalog* nested_catalog_with_constant =
      catalog_->MakeOwnedSimpleCatalog("nested_catalog_with_constant");
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      {"nested_catalog_with_constant", "KnownConstant"}, Value::Bool(false),
      &constant));
  nested_catalog_with_constant->AddOwnedConstant(constant.release());

  // Add <nested_catalog_with_catalog> for testing conflicts with named
  // constants.
  SimpleCatalog* nested_catalog_with_catalog =
      catalog_->MakeOwnedSimpleCatalog("nested_catalog_with_catalog");
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      {"nested_catalog_with_catalog", "TestConstantBool"}, Value::Bool(false),
      &constant));
  nested_catalog_with_catalog->AddOwnedConstant(constant.release());
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      {"nested_catalog_with_catalog", "c"}, Value::Double(-9999.999),
      &constant));
  nested_catalog_with_catalog->AddOwnedConstant(constant.release());
  SimpleCatalog* nested_catalog_catalog =
      nested_catalog_with_catalog->MakeOwnedSimpleCatalog(
          "nested_catalog_catalog");
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      {"nested_catalog_with_catalog", "nested_catalog_catalog", "a"},
      Value::Float(-1.4987f), &constant));
  nested_catalog_catalog->AddOwnedConstant(constant.release());
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      {"nested_catalog_with_catalog", "nested_catalog_catalog", "c"},
      Value::String("foo"), &constant));
  nested_catalog_catalog->AddOwnedConstant(constant.release());

  // Add a constant to <nested_catalog>.
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      {"nested_catalog", "TestConstantBool"}, Value::Bool(false),
      &constant));
  nested_catalog->AddOwnedConstant(constant.release());

  // Add another constant to <nested_catalog> that conflicts with a procedure.
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      {"nested_catalog", "nested_procedure"}, Value::Int64(2345),
      &constant));
  nested_catalog->AddOwnedConstant(constant.release());

  // Add a constant to <nested_catalog> which requires backticks.
  std::unique_ptr<SimpleConstant> string_constant_nonstandard_name;

  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"nested_catalog", "Test Constant-String"},
      Value::String("Test constant in nested catalog"),
      &string_constant_nonstandard_name));
  nested_catalog->AddOwnedConstant(string_constant_nonstandard_name.release());

  // Add struct constant with the same name as a nested catalog
  const StructType* nested_nested_catalog_type;
  ZETASQL_CHECK_OK(types_->MakeStructType({{"xxxx", types_->get_int64()}},
                                  &nested_nested_catalog_type));

  SimpleCatalog* wwww_catalog = nested_catalog->MakeOwnedSimpleCatalog("wwww");

  std::unique_ptr<SimpleConstant> wwww_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"nested_catalog", "wwww"},
      Value::Struct(nested_nested_catalog_type, {Value::Int64(8)}),
      &wwww_constant));
  nested_catalog->AddOwnedConstant(wwww_constant.release());

  std::unique_ptr<SimpleConstant> xxxx_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"nested_catalog", "wwww", "xxxx"},
      Value::Struct(nested_nested_catalog_type, {Value::Int64(8)}),
      &xxxx_constant));
  wwww_catalog->AddOwnedConstant(xxxx_constant.release());

  // Load a nested catalog with a name that resembles a system variable.
  SimpleCatalog* at_at_nested_catalog =
      catalog_->MakeOwnedSimpleCatalog("@@nested_catalog");
  std::unique_ptr<SimpleConstant> at_at_nested_catalog_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"@@nested_catalog", "sysvar2"}, Value::Int64(8),
      &at_at_nested_catalog_constant));
  at_at_nested_catalog->AddOwnedConstant(
      at_at_nested_catalog_constant.release());
}

static FreestandingDeprecationWarning CreateDeprecationWarning(
    int id,
    DeprecationWarning_Kind kind = DeprecationWarning::PROTO3_FIELD_PRESENCE) {
  FreestandingDeprecationWarning warning;
  const std::string foo_id = absl::StrCat("foo_", id);
  warning.set_message(absl::StrCat("Operation <foo", id, "> is deprecated"));
  warning.set_caret_string(absl::StrCat("some caret string for ", foo_id, "\n",
                                        "                      ^"));
  warning.mutable_deprecation_warning()->set_kind(kind);

  ErrorLocation* warning_location = warning.mutable_error_location();
  warning_location->set_line(10 + id);
  warning_location->set_column(20 + id);
  warning_location->set_filename(absl::StrCat("module", id, ".sqlm"));

  return warning;
}

void SampleCatalog::AddFunctionWithArgumentType(std::string type_name,
                                                const Type* arg_type) {
  auto function = absl::make_unique<Function>(
      absl::StrCat("fn_on_", type_name), "sample_functions", Function::SCALAR);
  function->AddSignature({types_->get_bool(), {arg_type}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(std::move(function));
}

void SampleCatalog::LoadExtendedSubscriptFunctions() {
  // Add new signatures for '$subscript_with_offset' so we can do some
  // additional testing.  The signatures are:
  // 1) <string>[OFFSET(<int64_t>)]:
  //    $subscript_with_offset(string, int64_t) -> (string)
  // 2) <string>[OFFSET(<string>)]:
  //    $subscript_with_offset(string, string) -> (string)
  const Function* subscript_offset_function;
  ZETASQL_CHECK_OK(catalog_->GetFunction("$subscript_with_offset",
                                 &subscript_offset_function));
  ZETASQL_CHECK(subscript_offset_function != nullptr);
  // If we ever update the builtin function implementation to actually include
  // a signature, then take a look at this code to see if it is still needed.
  ZETASQL_CHECK_EQ(subscript_offset_function->NumSignatures(), 0);
  Function* mutable_subscript_offset_function =
      const_cast<Function*>(subscript_offset_function);
  mutable_subscript_offset_function->AddSignature(
      {types_->get_string(),
       {types_->get_string(), types_->get_int64()},
       /*context_id=*/-1});
  mutable_subscript_offset_function->AddSignature(
      {types_->get_string(),
       {types_->get_string(), types_->get_string()},
       /*context_id=*/-1});
}

void SampleCatalog::LoadFunctions() {
  // Add a function to illustrate how repeated/optional arguments are resolved.
  Function* function = new Function("test_function", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_int64(),
        {{types_->get_int64(), FunctionArgumentType::REQUIRED},
           {types_->get_int64(), FunctionArgumentType::REPEATED},
           {types_->get_int64(), FunctionArgumentType::REPEATED},
           {types_->get_int64(), FunctionArgumentType::REQUIRED},
           {types_->get_int64(), FunctionArgumentType::OPTIONAL}},
         /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  function = new Function(
      "volatile_function", "sample_functions", Function::SCALAR,
      {{types_->get_int64(),
        {{types_->get_int64(), FunctionArgumentType::REQUIRED}},
         /*context_id=*/-1}},
      FunctionOptions().set_volatility(FunctionEnums::VOLATILE));
  catalog_->AddOwnedFunction(function);

  function = new Function(
      "stable_function", "sample_functions", Function::SCALAR,
      {{types_->get_int64(),
        {{types_->get_int64(), FunctionArgumentType::REQUIRED}},
         /*context_id=*/-1}},
      FunctionOptions().set_volatility(FunctionEnums::STABLE));
  catalog_->AddOwnedFunction(function);

  // Add a function that takes a specific proto as an argument.
  function = new Function("fn_on_KitchenSinkPB", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_bool(), {proto_KitchenSinkPB_}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes a specific enum as an argument.
  function = new Function("fn_on_TestEnum", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_bool(), {enum_TestEnum_}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // These sample functions are named 'fn_on_<typename>' with one argument of
  // type <typename> that returns a bool.
  AddFunctionWithArgumentType("bool", types_->get_bool());
  AddFunctionWithArgumentType("int32", types_->get_int32());
  AddFunctionWithArgumentType("int64", types_->get_int64());
  AddFunctionWithArgumentType("uint32", types_->get_uint32());
  AddFunctionWithArgumentType("uint64", types_->get_uint64());
  AddFunctionWithArgumentType("float", types_->get_float());
  AddFunctionWithArgumentType("double", types_->get_double());
  AddFunctionWithArgumentType("date", types_->get_date());
  AddFunctionWithArgumentType("timestamp", types_->get_timestamp());
  AddFunctionWithArgumentType("string", types_->get_string());

  // Add a function that takes an arbitrary type argument.
  function = new Function("fn_on_arbitrary_type_argument", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_bool(), {ARG_TYPE_ARBITRARY}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes any type enum.
  function = new Function("fn_on_any_enum", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_bool(), {ARG_ENUM_ANY}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes any type proto.
  function = new Function("fn_on_any_proto", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_bool(), {ARG_PROTO_ANY}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes any type struct.
  function = new Function("fn_on_any_struct", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_bool(), {ARG_STRUCT_ANY}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes any array and returns element type.
  function = new Function("fn_on_any_array_returns_element", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {{ARG_TYPE_ANY_1}, {ARG_ARRAY_TYPE_ANY_1}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes any type and returns an array of that type.
  function = new Function("fn_on_any_element_returns_array", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {{ARG_ARRAY_TYPE_ANY_1}, {ARG_TYPE_ANY_1}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes an array<int32_t> and returns an int32_t type.
  function = new Function("fn_on_int32_array_returns_int32", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {{types_->get_int32()}, {int32array_type_}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes an array<int64_t> and returns an int64_t type.
  function = new Function("fn_on_int64_array_returns_int64", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {{types_->get_int64()}, {int64array_type_}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes a STRUCT<int32_t, string> and returns bool.
  function = new Function("fn_on_struct_int32_string", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_bool(), {struct_type_}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  const StructType* struct_int32_date_type;
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"a", types_->get_int32()}, {"b", types_->get_date()}},
      &struct_int32_date_type));

  // Add a function that takes a STRUCT<int32_t, date> and returns bool.
  function = new Function("fn_on_struct_int32_date", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_bool(), {struct_int32_date_type}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  const StructType* struct_int64_string_type;
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"a", types_->get_int64()}, {"b", types_->get_string()}},
      &struct_int64_string_type));

  // Add a function that takes a STRUCT<int64_t, string> and returns bool.
  function = new Function("fn_on_struct_int64_string", "sample_functions",
                          Function::SCALAR);
  function->AddSignature(
      {types_->get_bool(), {struct_int64_string_type}, /*context_id=*/-1});
  catalog_->AddOwnedFunction(absl::WrapUnique(function));

  // Adds an scalar function that takes multiple repeated and optional
  // arguments.
  function = new Function(
      "fn_rep_opt", "sample_functions", Function::SCALAR,
      /*function_signatures=*/
      {
          {/*result_type=*/types_->get_int64(),
           /*arguments=*/
           {
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("a0")
                    .set_cardinality(FunctionArgumentType::REQUIRED)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("r0")
                    .set_cardinality(FunctionArgumentType::REPEATED)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("r1")
                    .set_cardinality(FunctionArgumentType::REPEATED)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("r2")
                    .set_cardinality(FunctionArgumentType::REPEATED)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("a1")
                    .set_cardinality(FunctionArgumentType::REQUIRED)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("o0")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("o1")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
           },
           /*context_id=*/-1},
      },
      FunctionOptions());
  catalog_->AddOwnedFunction(function);
  ZETASQL_CHECK_OK(function->signatures()[0].IsValid(ProductMode::PRODUCT_EXTERNAL));

  // Adds an aggregate function that takes no argument but supports order by.
  function = new Function(
      "sort_count", "sample_functions", Function::AGGREGATE,
      {{types_->get_int64(), {}, /*context_id=*/-1}},
      FunctionOptions().set_supports_order_by(true));
  catalog_->AddOwnedFunction(function);

  // Adds an aggregate function that takes multiple arguments and supports
  // order by arguments.
  function = new Function(
      "multi_sort_count", "sample_functions", Function::AGGREGATE,
      {{types_->get_int64(),
        {types_->get_int32(), types_->get_int64(), types_->get_string()},
        /*context_id=*/-1}},
      FunctionOptions().set_supports_order_by(true));
  catalog_->AddOwnedFunction(function);

  // Adds fn_agg_string_string_collation(STRING, STRING) -> INT64.
  // Enables uses_operation_collation to test collation resolution.
  function = new Function(
      "fn_agg_string_string_collation", "sample_functions", Function::AGGREGATE,
      {{types_->get_int64(),
        {types_->get_string(), types_->get_string()},
        /*context_id=*/-1,
        FunctionSignatureOptions().set_uses_operation_collation(true)}},
      FunctionOptions(FunctionOptions::ORDER_UNSUPPORTED,
                      /*window_framing_support_in=*/true));
  catalog_->AddOwnedFunction(function);

  // Adds fn_reject_collation(STRING, ANY TYPE) -> INT64.
  // Enables rejects_collation to test collation resolution.
  function =
      new Function("fn_reject_collation", "sample_functions", Function::SCALAR,
                   {{types_->get_int64(),
                     {types_->get_string(),
                      {ARG_TYPE_ANY_1,
                       FunctionArgumentTypeOptions()
                           .set_argument_name("second_arg")
                           .set_cardinality(FunctionArgumentType::OPTIONAL)}},
                     /*context_id=*/-1,
                     FunctionSignatureOptions().set_rejects_collation(true)}});
  catalog_->AddOwnedFunction(function);

  // Add the following test analytic functions. All functions have the same
  // list of function signatures:
  //     arguments: (), (ARG_TYPE_ANY_1) and (<int64_t>, <string>))
  //     return: <int64_t>
  //
  // They differ in the window support:
  // ---------------------------------------------------------------------------
  //                           Mode      ORDER BY    Window Frame  Null Handling
  //                                                                 Modifier
  //                         --------   -----------  ---------------------------
  //  afn_order              ANALYTIC    Required    Unsupported   Unsupported
  //  afn_no_order_no_frame  ANALYTIC   Unsupported  Unsupported   Unsupported
  //  afn_agg                AGGREGATE   Optional     Supported    Unsupported
  //  afn_null_handling      AGGREGATE   Optional    Unsupported    Supported
  //  --------------------------------------------------------------------------
  std::vector<FunctionSignature> function_signatures;
  function_signatures.push_back({types_->get_int64(), {}, /*context_id=*/-1});
  function_signatures.push_back(
      {types_->get_int64(), {ARG_TYPE_ANY_1}, /*context_id=*/-1});
  function_signatures.push_back(
      {types_->get_int64(), {types_->get_int64(), types_->get_string()},
       -1  /* context */});

  function = new Function(
      "afn_order", "sample_functions", Function::ANALYTIC,
      function_signatures,
      FunctionOptions(FunctionOptions::ORDER_REQUIRED,
                      /*window_framing_support_in=*/false));
  catalog_->AddOwnedFunction(function);

  function = new Function(
      "afn_no_order_no_frame", "sample_functions", Function::ANALYTIC,
      function_signatures,
      FunctionOptions(FunctionOptions::ORDER_UNSUPPORTED,
                      /*window_framing_support_in=*/false));
  catalog_->AddOwnedFunction(function);

  function = new Function(
      "afn_agg", "sample_functions", Function::AGGREGATE, function_signatures,
      FunctionOptions(FunctionOptions::ORDER_OPTIONAL,
                      /*window_framing_support_in=*/true));
  catalog_->AddOwnedFunction(function);

  function = new Function(
      "afn_null_handling", "sample_functions", Function::AGGREGATE,
      function_signatures,
      FunctionOptions(FunctionOptions::ORDER_OPTIONAL,
                      /*window_framing_support_in=*/false)
                          .set_supports_order_by(true)
                          .set_supports_limit(true)
                          .set_supports_null_handling_modifier(true));
  catalog_->AddOwnedFunction(function);

  // NULL_OF_TYPE(string) -> (a NULL of type matching the named simple type).
  // This is testing resolving functions where the return type is determined
  // dynamically based on literal values of the arguments.
  // The callback overrides the INT64 return type in the signature.
  function = new Function(
      "null_of_type", "sample_functions", Function::SCALAR,
      {{{types::Int64Type()}, {types::StringType()}, /*context_id=*/-1}},
      FunctionOptions().set_compute_result_type_callback(
          &ComputeResultTypeCallbackForNullOfType));
  catalog_->AddOwnedFunction(function);

  catalog_->AddOwnedFunction(new Function(
      "safe_supported_function", "sample_functions", Function::SCALAR,
      {{types_->get_int64(), {}, /*context_id=*/-1}}, FunctionOptions()));

  catalog_->AddOwnedFunction(new Function(
      "safe_unsupported_function", "sample_functions", Function::SCALAR,
      {{types_->get_int64(), {}, /*context_id=*/-1}},
      FunctionOptions().set_supports_safe_error_mode(false)));

  // Add a function that triggers a deprecation warning.
  function =
      new Function("deprecation_warning", "sample_functions", Function::SCALAR);

  FunctionSignature deprecation_warning_signature(
      types::Int64Type(), /*arguments=*/{}, /*context_id=*/-1);
  deprecation_warning_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/1)});
  function->AddSignature(deprecation_warning_signature);
  catalog_->AddOwnedFunction(function);

  function = new Function("deprecation_warning2", "sample_functions",
                          Function::SCALAR);
  FunctionSignature deprecation_warning2_signature(
      types::Int64Type(), /*arguments=*/{}, /*context_id=*/-1);
  deprecation_warning2_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/2)});
  function->AddSignature(deprecation_warning2_signature);
  catalog_->AddOwnedFunction(function);

  // Add a function that triggers two deprecation warnings with the same kind.
  function = new Function("two_deprecation_warnings_same_kind",
                          "sample_functions", Function::SCALAR);

  FunctionSignature two_deprecation_warnings_same_kind_signature(
      types::Int64Type(), /*arguments=*/{}, /*context_id=*/-1);
  two_deprecation_warnings_same_kind_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/2), CreateDeprecationWarning(/*id=*/3)});
  function->AddSignature(two_deprecation_warnings_same_kind_signature);
  catalog_->AddOwnedFunction(function);

  // Add a function that triggers two deprecation warnings with different kinds.
  function = new Function("two_deprecation_warnings", "sample_functions",
                          Function::SCALAR);
  FunctionSignature two_deprecation_warnings_signature(
      types::Int64Type(), /*arguments=*/{}, /*context_id=*/-1);
  two_deprecation_warnings_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/4),
       CreateDeprecationWarning(
           /*id=*/5, DeprecationWarning::DEPRECATED_FUNCTION_SIGNATURE)});
  function->AddSignature(two_deprecation_warnings_signature);
  catalog_->AddOwnedFunction(function);

  function = new AnonFunction(
      "anon_test", "sample_functions",
      {{types_->get_int64(),
        {/*expr=*/types_->get_int64(),
         /*lower_bound=*/{types_->get_int64(), FunctionArgumentType::OPTIONAL},
         /*upper_bound=*/{types_->get_int64(), FunctionArgumentType::OPTIONAL}},
        /*context_id=*/-1}},
      FunctionOptions()
          .set_supports_clamped_between_modifier(true)
          .set_supports_over_clause(false),
      "sum");
  catalog_->AddOwnedFunction(function);

  // Add a function that takes two named arguments with one signature.
  const auto named_required_format_arg = zetasql::FunctionArgumentType(
      types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                .set_argument_name("format_string"));
  const auto named_required_date_arg = zetasql::FunctionArgumentType(
      types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                .set_argument_name("date_string"));
  const auto named_required_format_arg_error_if_positional =
      zetasql::FunctionArgumentType(
          types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                    .set_argument_name("format_string")
                                    .set_argument_name_is_mandatory(true));
  const auto named_required_date_arg_error_if_positional =
      zetasql::FunctionArgumentType(
          types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                    .set_argument_name("date_string")
                                    .set_argument_name_is_mandatory(true));
  const auto named_optional_date_arg_error_if_positional =
      zetasql::FunctionArgumentType(
          types_->get_string(),
          zetasql::FunctionArgumentTypeOptions()
              .set_cardinality(FunctionArgumentType::OPTIONAL)
              .set_argument_name("date_string")
              .set_argument_name_is_mandatory(true));
  const auto named_optional_format_arg = zetasql::FunctionArgumentType(
      types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                .set_cardinality(FunctionArgumentType::OPTIONAL)
                                .set_argument_name("format_string"));
  const auto named_optional_date_arg = zetasql::FunctionArgumentType(
      types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                .set_cardinality(FunctionArgumentType::OPTIONAL)
                                .set_argument_name("date_string"));
  const auto named_optional_const_format_arg = zetasql::FunctionArgumentType(
      types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                .set_cardinality(FunctionArgumentType::OPTIONAL)
                                .set_must_be_constant()
                                .set_argument_name("format_string"));
  const auto non_named_required_format_arg = zetasql::FunctionArgumentType(
          types_->get_string(),
          zetasql::FunctionArgumentTypeOptions());
  const auto non_named_required_date_arg = zetasql::FunctionArgumentType(
          types_->get_string(),
          zetasql::FunctionArgumentTypeOptions());
  const auto non_named_optional_format_arg = zetasql::FunctionArgumentType(
      types_->get_string(),
      zetasql::FunctionArgumentTypeOptions()
          .set_cardinality(FunctionArgumentType::OPTIONAL));
  const auto non_named_optional_date_arg = zetasql::FunctionArgumentType(
      types_->get_string(),
      zetasql::FunctionArgumentTypeOptions()
          .set_cardinality(FunctionArgumentType::OPTIONAL));
  const auto named_optional_arg_named_not_null =
      zetasql::FunctionArgumentType(
          types_->get_string(),
          zetasql::FunctionArgumentTypeOptions()
              .set_cardinality(FunctionArgumentType::OPTIONAL)
              .set_must_be_non_null()
              .set_argument_name("arg")
              .set_argument_name_is_mandatory(true));
  const auto mode = Function::SCALAR;

  function = new Function("fn_named_args", "sample_functions", mode);
  function->AddSignature({types_->get_bool(),
                          {named_required_format_arg, named_required_date_arg},
                          /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);
  function = new Function("fn_const_named_arg", "sample_functions", mode);
  function->AddSignature(
      {types_->get_bool(),
       {named_optional_const_format_arg, named_optional_date_arg},
       /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add functions with two named optional/repeated arguments on one signature.
  function = new Function("fn_named_args_optional", "sample_functions", mode);
  function->AddSignature({types_->get_bool(),
                          {named_optional_format_arg, named_optional_date_arg},
                          /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes two named arguments with two signatures with
  // optional argument types.
  function =
      new Function("fn_named_args_two_signatures", "sample_functions", mode);
  function->AddSignature({types_->get_bool(),
                          {named_required_format_arg, named_required_date_arg},
                          /*context_id=*/-1});
  function->AddSignature({types_->get_bool(),
                          {named_required_date_arg, named_required_format_arg},
                          /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function that takes two non-named arguments and one named argument in
  // each of two signatures.
  function = new Function("fn_three_named_args_two_signatures",
                          "sample_functions", mode);
  function->AddSignature(
      {types_->get_bool(),
       {non_named_required_format_arg, non_named_required_date_arg,
        named_required_format_arg},
       /*context_id=*/-1});
  function->AddSignature(
      {types_->get_bool(),
       {non_named_required_format_arg, non_named_required_date_arg,
        named_required_date_arg},
       /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function with two named arguments where neither may be specified
  // positionally.
  function = new Function("fn_named_args_error_if_positional",
                          "sample_functions", mode);
  function->AddSignature({types_->get_bool(),
                          {named_required_format_arg_error_if_positional,
                           named_required_date_arg_error_if_positional},
                          /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function with two named arguments where the first may not be
  // specified positionally.
  function = new Function("fn_named_args_error_if_positional_first_arg",
                          "sample_functions", mode);
  function->AddSignature({types_->get_bool(),
                          {named_required_format_arg_error_if_positional,
                           named_required_date_arg},
                          /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function with two named arguments where the second may not be
  // specified positionally.
  function = new Function("fn_named_args_error_if_positional_second_arg",
                          "sample_functions", mode);
  function->AddSignature({types_->get_bool(),
                          {named_required_format_arg,
                           named_required_date_arg_error_if_positional},
                          /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function with two named arguments, one required and one optional,
  // and neither may be specified positionally.
  function = new Function("fn_named_optional_args_error_if_positional",
                          "sample_functions", mode);
  function->AddSignature({types_->get_bool(),
                          {named_required_format_arg_error_if_positional,
                           named_optional_date_arg_error_if_positional},
                          /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function with two optional arguments, one regular and one named that
  // cannot be specified positionally.
  function =
      new Function("fn_optional_named_optional_args", "sample_functions", mode);
  function->AddSignature(
      {types_->get_bool(),
       {{types_->get_string(), FunctionArgumentType::OPTIONAL},
        named_optional_date_arg_error_if_positional},
       /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function with three optional arguments, one regular and two named,
  // both cannot be specified positionally and first cannot be NULL.
  function = new Function("fn_optional_named_optional_not_null_args",
                          "sample_functions", mode);
  function->AddSignature(
      {types_->get_bool(),
       {{types_->get_string(), FunctionArgumentType::OPTIONAL},
        named_optional_arg_named_not_null,
        named_optional_date_arg_error_if_positional},
       /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function with two signatures, one using regular arguments and one
  // using named arguments that cannot be specified positionally.
  function = new Function("fn_regular_and_named_signatures",
                          "sample_functions", mode);
  function->AddSignature(
      {types_->get_bool(),
       {{types_->get_string(), FunctionArgumentType::REQUIRED},
        {types_->get_string(), FunctionArgumentType::OPTIONAL}},
       /*context_id=*/-1});
  function->AddSignature(
      {types_->get_bool(),
       {{types_->get_string(), FunctionArgumentType::REQUIRED},
        named_optional_date_arg_error_if_positional},
       /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);

  // Add a function with one unnamed and two named STRING arguments and
  // returning a STRING.
  function = new Function("fn_named_arguments_returns_string",
                          "sample_functions", mode);
  function->AddSignature(
      {types_->get_string(),
       {{types_->get_string(), FunctionArgumentType::REQUIRED},
        named_required_format_arg,
        named_required_date_arg},
       /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);


  // A FunctionSignatureArgumentConstraintsCallback that checks for NULL
  // arguments.
  auto sanity_check_nonnull_arg_constraints =
      [](const FunctionSignature& signature,
         const std::vector<InputArgumentType>& arguments) {
        ZETASQL_CHECK(signature.IsConcrete());
        ZETASQL_CHECK_EQ(signature.NumConcreteArguments(), arguments.size());
        for (int i = 0; i < arguments.size(); ++i) {
          ZETASQL_CHECK(
              arguments[i].type()->Equals(signature.ConcreteArgumentType(i)));
          if (arguments[i].is_null()) {
            return false;
          }
        }
        return true;
      };

  // A PostResolutionArgumentConstraintsCallback that restricts all the provided
  // INT64 arguments to be nonnegative if they are literals.
  auto post_resolution_arg_constraints =
      [](const FunctionSignature& signature,
         const std::vector<InputArgumentType>& arguments,
         const LanguageOptions& language_options) -> absl::Status {
        for (int i = 0; i < arguments.size(); ++i) {
          ZETASQL_CHECK(
              arguments[i].type()->Equals(signature.ConcreteArgumentType(i)));
          if (!arguments[i].type()->IsInt64() || !arguments[i].is_literal()) {
            continue;
          }
          if (arguments[i].literal_value()->int64_value() < 0) {
            return MakeSqlError()
                   << "Argument "
                   << (signature.ConcreteArgument(i).has_argument_name()
                           ? signature.ConcreteArgument(i).argument_name()
                           : std::to_string(i+1))
                   << " must not be negative";
          }
        }
        return absl::OkStatus();
      };

  // Add a function with an argument constraint that verifies the concrete
  // arguments in signature matches the input argument list, and rejects
  // any NULL arguments.
  function =
      new Function("fn_named_opt_args_nonnull_nonnegative_constraints",
                   "sample_functions", mode,
                   FunctionOptions().set_post_resolution_argument_constraint(
                       post_resolution_arg_constraints));
  FunctionSignature signature_with_constraints{
      types_->get_bool(),
      {{types_->get_string(),
        FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
            .set_argument_name("o1_string")},
       {types_->get_int64(),
        FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
            .set_argument_name("o2_int64")},
       {types_->get_double(),
        FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
            .set_argument_name("o3_double")}},
      /*context_id=*/-1,
      FunctionSignatureOptions().set_constraints(
          sanity_check_nonnull_arg_constraints)};
  function->AddSignature(signature_with_constraints);
  catalog_->AddOwnedFunction(function);

  // Similar as the previous function, but the arguments are unnamed.
  function =
      new Function("fn_unnamed_opt_args_nonnull_nonnegative_constraints",
                   "sample_functions", mode,
                   FunctionOptions().set_post_resolution_argument_constraint(
                       post_resolution_arg_constraints));
  FunctionSignature signature_with_unnamed_args_constraints{
      types_->get_bool(),
      {{types_->get_string(), FunctionArgumentType::OPTIONAL},
       {types_->get_int64(), FunctionArgumentType::OPTIONAL},
       {types_->get_double(), FunctionArgumentType::OPTIONAL}},
      /*context_id=*/-1,
      FunctionSignatureOptions().set_constraints(
          sanity_check_nonnull_arg_constraints)};
  function->AddSignature(signature_with_unnamed_args_constraints);
  catalog_->AddOwnedFunction(function);

  // Adds a templated function that generates its result type via the callback.
  function = new Function(
      "fn_result_type_from_arg", "sample_functions", mode,
      FunctionOptions().set_compute_result_type_callback(
          &ComputeResultTypeFromStringArgumentValue));
  function->AddSignature(
      {{types_->get_string()},
       {{types_->get_string(),
         FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
             .set_argument_name("o1")},
        {types_->get_string(),
         FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
             .set_argument_name("type_name")}},
       /*context_id=*/-1});
  catalog_->AddOwnedFunction(function);
}  // NOLINT(readability/fn_size)

void SampleCatalog::LoadFunctionsWithDefaultArguments() {
  // Adds an scalar function that takes multiple optional named arguments with
  // some of them having default values.
  Function* function = new Function(
      "fn_optional_named_default_args", "sample_functions", Function::SCALAR,
      /*function_signatures=*/
      {
          {/*result_type=*/types_->get_int64(),
           /*arguments=*/
           {
               {types_->get_string(),
                FunctionArgumentTypeOptions()
                    .set_argument_name("a0")
                    .set_cardinality(FunctionArgumentType::REQUIRED)},
               {types_->get_string(),
                FunctionArgumentTypeOptions()
                    .set_argument_name("o0")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
               {types_->get_string(),
                FunctionArgumentTypeOptions()
                    .set_argument_name("o1")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::String("o1_default"))},
               {types_->get_double(),
                FunctionArgumentTypeOptions()
                    .set_argument_name("o2")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::Double(0.2))},
           },
           /*context_id=*/-1},
      },
      FunctionOptions());
  catalog_->AddOwnedFunction(function);

  // Similar to the one above, but the optional arguments are templated.
  function = new Function(
      "fn_optional_named_default_args_templated", "sample_functions",
      Function::SCALAR,
      /*function_signatures=*/
      {
          {/*result_type=*/types_->get_int64(),
           /*arguments=*/
           {
               {types_->get_string(),
                FunctionArgumentTypeOptions()
                    .set_argument_name("a0")
                    .set_cardinality(FunctionArgumentType::REQUIRED)},
               {ARG_TYPE_ANY_1,
                FunctionArgumentTypeOptions()
                    .set_argument_name("o0")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
               {ARG_TYPE_ANY_2,
                FunctionArgumentTypeOptions()
                    .set_argument_name("o1")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::String("o1_default"))},
               {ARG_TYPE_ANY_1,
                FunctionArgumentTypeOptions()
                    .set_argument_name("o2")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::Int32(2))},
           },
           /*context_id=*/-1},
      },
      FunctionOptions());
  catalog_->AddOwnedFunction(function);

  // Similar to the one above, but the default argument value is -2 rather than
  // 2.
  function = new Function(
      "fn_optional_named_default_args_templated_negative", "sample_functions",
      Function::SCALAR,
      /*function_signatures=*/
      {
          {/*result_type=*/types_->get_int64(),
           /*arguments=*/
           {
               {types_->get_string(),
                FunctionArgumentTypeOptions()
                    .set_argument_name("a0")
                    .set_cardinality(FunctionArgumentType::REQUIRED)},
               {ARG_TYPE_ANY_1,
                FunctionArgumentTypeOptions()
                    .set_argument_name("o0")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
               {ARG_TYPE_ANY_2,
                FunctionArgumentTypeOptions()
                    .set_argument_name("o1")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::String("o1_default"))},
               {ARG_TYPE_ANY_1,
                FunctionArgumentTypeOptions()
                    .set_argument_name("o2")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::Int32(-2))},
           },
           /*context_id=*/-1},
      },
      FunctionOptions());
  catalog_->AddOwnedFunction(function);

  // Adds an scalar function that takes multiple unnamed optional arguments with
  // some of them having default values.
  function = new Function(
      "fn_optional_unnamed_default_args", "sample_functions", Function::SCALAR,
      /*function_signatures=*/
      {
          {/*result_type=*/types_->get_int64(),
           /*arguments=*/
           {
               {types_->get_string(),
                FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::REQUIRED)},
               {types_->get_string(),
                FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
               {types_->get_string(),
                FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::String("o1_default"))},
               {types_->get_double(),
                FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::Double(0.3))},
           },
           /*context_id=*/-1},
      },
      FunctionOptions());
  catalog_->AddOwnedFunction(function);

  // Similar to the one above, but the optional arguments are templated.
  function = new Function(
      "fn_optional_unnamed_default_args_templated", "sample_functions",
      Function::SCALAR,
      /*function_signatures=*/
      {
          {/*result_type=*/types_->get_int64(),
           /*arguments=*/
           {
               {types_->get_string(),
                FunctionArgumentTypeOptions().set_cardinality(
                    FunctionArgumentType::REQUIRED)},
               {ARG_TYPE_ANY_1, FunctionArgumentTypeOptions().set_cardinality(
                                    FunctionArgumentType::OPTIONAL)},
               {ARG_TYPE_ANY_2,
                FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::String("o1_default"))},
               {ARG_TYPE_ANY_1,
                FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::Int32(5))},
           },
           /*context_id=*/-1},
      },
      FunctionOptions());
  catalog_->AddOwnedFunction(function);

  // A scalar function with both named and unnamed optional arguments plus the
  // last argument with a default value.
  function = new Function(
      "fn_req_opt_unnamed_named_default", "sample_functions", Function::SCALAR,
      /*function_signatures=*/
      {
          {/*result_type=*/types_->get_int64(),
           /*arguments=*/
           {
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::REQUIRED)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("o1")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("o2")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)
                    .set_default(values::String("dv"))},
           },
           /*context_id=*/-1},
      },
      FunctionOptions());
  ZETASQL_CHECK_OK(function->signatures()[0].IsValid(ProductMode::PRODUCT_EXTERNAL));
  catalog_->AddOwnedFunction(function);

  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_optional_named_default_args"},
      FunctionSignature(
          /*result_type=*/ARG_TYPE_RELATION,
          /*arguments=*/
          {
              {ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("relation")
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {types_->get_bool(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("r1")
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {types_->get_string(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("o0")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)},
              {types_->get_double(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("o1")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)
                   .set_default(values::Double(3.14))},
              {types_->get_uint32(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("o2")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)
                   .set_default(values::Uint32(10086))},
          },
          /*context_id=*/-1)));

  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_optional_named_default_args_templated"},
      FunctionSignature(
          /*result_type=*/ARG_TYPE_RELATION,
          /*arguments=*/
          {
              {ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("relation")
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {types_->get_bool(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("r1")
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {ARG_TYPE_ANY_1,
               FunctionArgumentTypeOptions()
                   .set_argument_name("o0")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)},
              {ARG_TYPE_ANY_2,
               FunctionArgumentTypeOptions()
                   .set_argument_name("o1")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)
                   .set_default(values::Double(3.14))},
              {ARG_TYPE_ANY_1,
               FunctionArgumentTypeOptions()
                   .set_argument_name("o2")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)
                   .set_default(values::String("abc"))},
          },
          /*context_id=*/-1)));

  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_optional_unnamed_default_args"},
      FunctionSignature(
          /*result_type=*/ARG_TYPE_RELATION,
          /*arguments=*/
          {
              {ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {types_->get_bool(),
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {types_->get_string(),
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::OPTIONAL)},
              {types_->get_float(),
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::OPTIONAL)
                   .set_default(values::Float(0.618))},
              {types_->get_uint32(),
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::OPTIONAL)
                   .set_default(values::Uint32(168))},
          },
          /*context_id=*/-1)));

  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_optional_unnamed_default_args_templated"},
      FunctionSignature(
          /*result_type=*/ARG_TYPE_RELATION,
          /*arguments=*/
          {
              {ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {types_->get_bool(),
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {ARG_TYPE_ANY_1,
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::OPTIONAL)},
              {ARG_TYPE_ANY_2,
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::OPTIONAL)
                   .set_default(values::String("xyz"))},
              {ARG_TYPE_ANY_1,
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::OPTIONAL)
                   .set_default(values::Int32(-1))},
          },
          /*context_id=*/-1)));

  // A TVF with a combined of named and unnamed optional arguments.
  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_optional_unnamed_named"},
      FunctionSignature(
          /*result_type=*/ARG_TYPE_RELATION,
          /*arguments=*/
          {
              {ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {types_->get_bool(),
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::REQUIRED)},
              {types_->get_string(),
               FunctionArgumentTypeOptions()
                   .set_cardinality(FunctionArgumentType::OPTIONAL)},
              {types_->get_string(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("o1")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)},
          },
          /*context_id=*/-1)));

  // A TVF with a combined of named and unnamed optional arguments plus a
  // default argument at last.
  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_optional_unnamed_named_default"},
      FunctionSignature(
          /*result_type=*/ARG_TYPE_RELATION,
          /*arguments=*/
          {
              {ARG_TYPE_RELATION, FunctionArgumentTypeOptions().set_cardinality(
                                      FunctionArgumentType::REQUIRED)},
              {types_->get_bool(),
               FunctionArgumentTypeOptions().set_cardinality(
                   FunctionArgumentType::REQUIRED)},
              {types_->get_string(),
               FunctionArgumentTypeOptions().set_cardinality(
                   FunctionArgumentType::OPTIONAL)},
              {types_->get_string(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("o1")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)},
              {types_->get_int32(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("o2")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)
                   .set_default(values::Int32(314))},
          },
          /*context_id=*/-1)));

  // A scalar function with both unnamed and named optional arguments.
  function = new Function(
      "fn_req_opt_unnamed_named", "sample_functions", Function::SCALAR,
      /*function_signatures=*/
      {
          {/*result_type=*/types_->get_int64(),
           /*arguments=*/
           {
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::REQUIRED)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
               {types_->get_string(),
                zetasql::FunctionArgumentTypeOptions()
                    .set_argument_name("o1")
                    .set_cardinality(FunctionArgumentType::OPTIONAL)},
           },
           /*context_id=*/-1},
      },
      FunctionOptions());
  catalog_->AddOwnedFunction(function);
  ZETASQL_CHECK_OK(function->signatures()[0].IsValid(ProductMode::PRODUCT_EXTERNAL));
}

void SampleCatalog::LoadTemplatedSQLUDFs() {
  // Return an empty struct as the result type for now.
  // The function resolver will dynamically compute a different result type at
  // analysis time based on the function SQL body.
  const FunctionArgumentType result_type(ARG_TYPE_ARBITRARY);
  int context_id = 0;

  // Add a UDF with a simple valid templated SQL body.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_return_one"},
      FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{}, ParseResumeLocation::FromString("1")));

  // Add a templated SQL function that calls another templated SQL function.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_call_udf_templated_return_one"},
      FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{},
      ParseResumeLocation::FromString("udf_templated_return_one()")));

  // Add a templated SQL function that calls another templated SQL function
  // twice.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_call_udf_templated_return_one_twice"},
      FunctionSignature(result_type, {}, context_id++), /*argument_names=*/{},
      ParseResumeLocation::FromString("udf_call_udf_templated_return_one() + "
                                      "udf_call_udf_templated_return_one()")));

  // Add a UDF with a valid templated SQL body that refers to an argument.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_return_bool_arg"},
      FunctionSignature(result_type, {FunctionArgumentType(types::BoolType())},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("x")));

  // Add a UDF with a valid templated SQL body that refers to an argument.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_return_int64_arg"},
      FunctionSignature(result_type, {FunctionArgumentType(types::Int64Type())},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("x")));

  // Add a UDF with a valid templated SQL body that refers to an argument.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_return_any_scalar_arg"},
      FunctionSignature(result_type, {FunctionArgumentType(ARG_TYPE_ARBITRARY)},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("x")));

  // Add a UDF with a valid templated SQL body that performs addition on an
  // argument. The function signature accepts a single argument of any type.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_arg_plus_integer"},
      FunctionSignature(result_type,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("x + 42")));

  // Add a UDF with a valid templated SQL body that accepts an input argument
  // where the name contains '$'. The function signature accepts a single
  // argument of any type.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_arg_plus_integer_accept_dollars_col_name"},
      FunctionSignature(result_type,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"$col1"},
      ParseResumeLocation::FromString("`$col1`")));

  // Add a UDF with a valid templated SQL body that performs concatenation on an
  // argument. The function signature accepts a single argument of any type.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_arg_concat_string"},
      FunctionSignature(result_type,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"},
      ParseResumeLocation::FromString("concat(x, 'abc')")));

  // Add a UDF with a valid templated SQL body that performs concatenation on
  // two arguments. The function signature accepts two arguments of any type.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_arg_concat_two_strings"},
      FunctionSignature(result_type,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED),
                         FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x", "y"},
      ParseResumeLocation::FromString("concat(x, y)")));

  // Add a UDF with a valid templated SQL body that performs a proto field
  // access on an argument. The function signature accepts a single argument of
  // any type.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_arg_proto_field_access"},
      FunctionSignature(result_type,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"},
      ParseResumeLocation::FromString("x.int32_field")));

  // Add an invalid templated SQL function with a parse error in the function
  // body.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_parse_error"},
      FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{}, ParseResumeLocation::FromString("a b c d e")));

  // Add an invalid templated SQL function with an analysis error in the
  // function body.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_analysis_error"},
      FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{}, ParseResumeLocation::FromString("'abc' + 42")));

  // Add a UDF that refers to 'udf_templated_analysis_error' to show two levels
  // of nested error messages.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_call_udf_templated_analysis_error"},
      FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{},
      ParseResumeLocation::FromString("udf_templated_analysis_error() + 1")));

  // Add a UDF that refers to 'udf_call_udf_templated_analysis_error' to show
  // three levels of nested error messages.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_call_udf_call_udf_templated_analysis_error"},
      FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{},
      ParseResumeLocation::FromString(
          "udf_call_udf_templated_analysis_error() + 1")));

  // Add a UDF that refers to 'udf_call_udf_call_udf_templated_analysis_error'
  // to show four levels of nested error messages.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_call_udf_call_udf_call_udf_templated_analysis_error"},
      FunctionSignature(result_type, {}, context_id++), /*argument_names=*/{},
      ParseResumeLocation::FromString(
          "udf_call_udf_call_udf_templated_analysis_error() + 1")));

  // Add an invalid templated SQL function that attempts to refer to a query
  // parameter.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_function_body_refer_to_parameter"},
      FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{},
      ParseResumeLocation::FromString("@test_param_bool")));

  // Add an invalid templated SQL function where the function body is empty.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_function_body_empty"},
      FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{}, ParseResumeLocation::FromString("")));

  // Add an invalid templated SQL function that directly calls itself.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_recursive"}, FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{},
      ParseResumeLocation::FromString("udf_recursive()")));

  // Add two invalid templated SQL functions that indirectly call themselves
  // through one or more other templated SQL function calls.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_calls_self_indirectly_1"},
      FunctionSignature(result_type, {}, context_id++), /*argument_names=*/{},
      ParseResumeLocation::FromString("udf_calls_self_indirectly_2()")));

  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_calls_self_indirectly_2"},
      FunctionSignature(result_type, {}, context_id++),
      /*argument_names=*/{},
      ParseResumeLocation::FromString("udf_calls_self_indirectly_1()")));

  // Add a templated SQL function that calls a templated SQL TVF that calls the
  // original templated SQL function again, to make sure cycle detection works.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_calls_tvf_calls_same_udf"},
      FunctionSignature(result_type, {}, context_id++), /*argument_names=*/{},
      ParseResumeLocation::FromString(
          "(select * from tvf_calls_udf_calls_same_tvf())")));

  // Add a templated SQL function with duplicate argument names.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_duplicate_arg_names"},
      FunctionSignature(result_type,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED),
                         FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x", "x"},
      ParseResumeLocation::FromString("concat(x, 'abc')")));

  // Add a templated SQL function that triggers a deprecation warning.
  FunctionSignature deprecation_warning_signature(result_type, /*arguments=*/{},
                                                  /*context_id=*/-1);
  deprecation_warning_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/101)});
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_calls_deprecation_warning"}, deprecation_warning_signature,
      /*argument_names=*/{},
      ParseResumeLocation::FromString("deprecation_warning()")));

  // Add a UDF with a simple valid templated SQL body that returns its one input
  // argument, and has an expected result type of a 32-bit integer. We will use
  // this to test comparing the result type against the expected type when
  // testing templated SQL function calls.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_one_templated_arg_return_int32"},
      FunctionSignature(types::Int32Type(),
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("x")));

  // Add a UDF with a simple valid templated SQL body that returns its one input
  // argument, and has an expected result type of a 64-bit integer. We will use
  // this to test comparing the result type against the expected type when
  // testing templated SQL function calls.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_one_templated_arg_return_int64"},
      FunctionSignature(types::Int64Type(),
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("x")));

  // Add a UDF with a simple valid templated SQL body that returns its one input
  // argument, and has an expected result type of a date. We will use this to
  // test comparing the result type against the expected type when testing
  // templated SQL function calls.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_one_templated_arg_return_date"},
      FunctionSignature(types::DateType(),
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("x")));

  // Add a UDF with a simple valid templated SQL body that returns its one input
  // argument, and has an expected result type of a struct. We will use
  // this to test comparing the result type against the expected type when
  // testing templated SQL function calls.
  const StructType* return_type = nullptr;
  std::vector<StructType::StructField> fields;
  fields.emplace_back("int_field", types::Int64Type());
  fields.emplace_back("string_field", types::StringType());
  ZETASQL_CHECK_OK(type_factory()->MakeStructType(fields, &return_type));
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_one_templated_arg_return_struct_int64_string"},
      FunctionSignature(return_type,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("x")));

  // Add a UDF with a simple valid templated SQL body that returns a constant
  // integer, and has an expected result type of a string. We will use
  // this to test comparing the result type against the expected type when
  // testing templated SQL function calls.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_return_42_return_type_string"},
      FunctionSignature(types::StringType(),
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("42")));

  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_return_42_return_type_int32"},
      FunctionSignature(types::Int32Type(),
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("42")));

  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"udf_templated_return_999999999999999_return_type_int32"},
      FunctionSignature(types::Int32Type(),
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*argument_names=*/{"x"},
      ParseResumeLocation::FromString("999999999999999")));

  // Add a SQL UDA with a valid templated SQL body that refers to an aggregate
  // argument only.
  FunctionArgumentType int64_aggregate_arg_type(types::Int64Type());
  FunctionArgumentTypeOptions agg_options;
  agg_options.set_is_not_aggregate();
  FunctionArgumentType int64_not_aggregate_arg_type(types::Int64Type(),
                                                    agg_options);

  FunctionOptions fn_opts_allow_null_handling;
  fn_opts_allow_null_handling.set_supports_null_handling_modifier(true);

  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"uda_valid_templated_return_sum_int64_arg"},
      FunctionSignature(result_type, {int64_aggregate_arg_type}, context_id++),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("sum(x)"),
      Function::AGGREGATE, fn_opts_allow_null_handling));

  // Add a SQL UDA with a valid templated SQL body that refers to a NOT
  // AGGREGATE argument only.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"uda_valid_templated_return_int64_not_aggregate_arg"},
      FunctionSignature(result_type, {int64_not_aggregate_arg_type},
                        context_id++),
      /*argument_names=*/{"y"}, ParseResumeLocation::FromString("y"),
      Function::AGGREGATE));

  // Add a SQL UDA with a valid templated SQL body that refers to an AGGREGATE
  // argument and also a NOT AGGREGATE argument in the same script.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"uda_valid_templated_return_int64_aggregate_and_not_aggregate_arg"},
      FunctionSignature(
          result_type, {int64_aggregate_arg_type, int64_not_aggregate_arg_type},
          context_id++),
      /*argument_names=*/{"x", "y"},
      ParseResumeLocation::FromString("sum(x) + y"), Function::AGGREGATE));

  // Add a SQL UDA with an invalid templated SQL body that refers to an
  // AGGREGATE argument and also a NOT AGGREGATE argument in the same script.
  // The function attempts to refer to the AGGREGATE argument outside of an
  // aggregate function, which is invalid. Note that NOT AGGREGATE arguments can
  // still be aggregated.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"uda_invalid_templated_return_int64_aggregate_and_not_aggregate_arg"},
      FunctionSignature(
          result_type, {int64_aggregate_arg_type, int64_not_aggregate_arg_type},
          context_id++),
      /*argument_names=*/{"x", "y"},
      ParseResumeLocation::FromString("sum(y) + x"), Function::AGGREGATE));

  // Add a SQL UDA with an invalid templated SQL body, since there is an ORDER
  // BY clause in an aggregate function nested inside.
  catalog_->AddOwnedFunction(new TemplatedSQLFunction(
      {"uda_invalid_templated_return_sum_int64_arg_nested_order_by"},
      FunctionSignature(result_type, {int64_aggregate_arg_type}, context_id++),
      /*argument_names=*/{"x"},
      ParseResumeLocation::FromString("sum(x order by x)"),
      Function::AGGREGATE));
}

namespace {
const char kColumnNameBool[] = "column_bool";
const char kColumnNameBytes[] = "column_bytes";
const char kColumnNameDate[] = "column_date";
const char kColumnNameDouble[] = "column_double";
const char kColumnNameFloat[] = "column_float";
const char kColumnNameInt32[] = "column_int32";
const char kColumnNameInt64[] = "column_int64";
const char kColumnNameString[] = "column_string";
const char kColumnNameTime[] = "column_time";
const char kColumnNameUInt32[] = "column_uint32";
const char kColumnNameUInt64[] = "column_uint64";
const char kColumnNameKey[] = "key";
const char kColumnNameValue[] = "value";
const char kColumnNameFilename[] = "filename";

const char kTypeBool[] = "bool";
const char kTypeBytes[] = "bytes";
const char kTypeDate[] = "date";
const char kTypeDouble[] = "double";
const char kTypeFloat[] = "float";
const char kTypeInt32[] = "int32";
const char kTypeInt64[] = "int64";
const char kTypeString[] = "string";
const char kTypeTime[] = "time";
const char kTypeUInt32[] = "uint32";
const char kTypeUInt64[] = "uint64";
const char kTypeInterval[] = "interval";

struct OutputColumn {
  std::string description;
  std::string name;
  const Type* type;
};

struct TVFArgument {
  std::string description;
  const Type* type;
};

}  // namespace

static std::vector<OutputColumn> GetOutputColumnsForAllTypes(
    TypeFactory* types) {
  return {{kTypeBool, kColumnNameBool, types->get_bool()},
          {kTypeBytes, kColumnNameBytes, types->get_bytes()},
          {kTypeDate, kColumnNameDate, types->get_date()},
          {kTypeDouble, kColumnNameDouble, types->get_double()},
          {kTypeFloat, kColumnNameFloat, types->get_float()},
          {kTypeInt32, kColumnNameInt32, types->get_int32()},
          {kTypeInt64, kColumnNameInt64, types->get_int64()},
          {kTypeString, kColumnNameString, types->get_string()},
          {kTypeTime, kColumnNameTime, types->get_time()},
          {kTypeUInt32, kColumnNameUInt32, types->get_uint32()},
          {kTypeUInt64, kColumnNameUInt64, types->get_uint64()}};
}

static std::vector<TVFArgument> GetTVFArgumentsForAllTypes(
    TypeFactory* types) {
  return {{kTypeBool, types->get_bool()},
          {kTypeBytes, types->get_bytes()},
          {kTypeDate, types->get_date()},
          {kTypeDouble, types->get_double()},
          {kTypeFloat, types->get_float()},
          {kTypeInt32, types->get_int32()},
          {kTypeInt64, types->get_int64()},
          {kTypeString, types->get_string()},
          {kTypeTime, types->get_time()},
          {kTypeUInt32, types->get_uint32()},
          {kTypeUInt64, types->get_uint64()},
          {kTypeInterval, types->get_interval()}};
}

static TVFRelation GetOutputSchemaWithTwoTypes(
    const std::vector<OutputColumn>& output_columns_for_all_types) {
  TVFRelation::ColumnList columns;
  for (int i = 0; i < 2; ++i) {
    columns.emplace_back(output_columns_for_all_types[i].name,
                         output_columns_for_all_types[i].type);
  }
  return TVFRelation(columns);
}

void SampleCatalog::LoadTableValuedFunctions1() {
  TVFRelation empty_output_schema({});

  const std::vector<OutputColumn> kOutputColumnsAllTypes =
      GetOutputColumnsForAllTypes(types_);

  const std::vector<TVFArgument> kArgumentsAllTypes =
      GetTVFArgumentsForAllTypes(types_);

  TVFRelation output_schema_two_types =
      GetOutputSchemaWithTwoTypes(kOutputColumnsAllTypes);

  // Generate an output schema that returns an int64_t value table.
  TVFRelation output_schema_int64_value_table =
      TVFRelation::ValueTable(types_->get_int64());

  // Generate an output schema that returns a proto value table.
  TVFRelation output_schema_proto_value_table = TVFRelation::ValueTable(
      GetProtoType(zetasql_test__::TestExtraPB::descriptor()));

  // Generate an output schema that returns every possible type.
  TVFRelation::ColumnList columns;
  columns.reserve(kOutputColumnsAllTypes.size());
  for (const auto& kv : kOutputColumnsAllTypes) {
    columns.emplace_back(kv.name, kv.type);
  }
  TVFRelation output_schema_all_types(columns);

  // Add a TVF that takes no arguments.
  int context_id = 0;
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_no_args"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        FunctionArgumentTypeList(), context_id++),
      output_schema_two_types));

  // Add a TVF that returns an empty output schema.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_empty_output_schema"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            empty_output_schema,
                            /*extra_relation_input_columns_allowed=*/false),
                        FunctionArgumentTypeList(), context_id++),
      empty_output_schema));

  // Add a TVF that takes no arguments and returns all POD types.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_no_args_return_all_pod_types"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_all_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        FunctionArgumentTypeList(), context_id++),
      output_schema_all_types));

  // Add a TVF for each POD type that accepts exactly one argument of that type.
  for (const auto& kv : kArgumentsAllTypes) {
    catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
        {absl::StrCat("tvf_exactly_1_", kv.description, "_arg")},
        FunctionSignature(FunctionArgumentType::RelationWithSchema(
                              output_schema_two_types,
                              /*extra_relation_input_columns_allowed=*/false),
                          {FunctionArgumentType(kv.type)}, context_id++),
        output_schema_two_types));
  }

  // Add TVFs that accept between two and nine INT64 arguments.
  for (int i = 2; i < 10; ++i) {
    catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
        {absl::StrCat("tvf_exactly_", i, "_int64_args")},
        FunctionSignature(
            FunctionArgumentType::RelationWithSchema(
                output_schema_two_types,
                /*extra_relation_input_columns_allowed=*/false),
            FunctionArgumentTypeList(i, zetasql::types::Int64Type()),
            context_id++),
        output_schema_two_types));
  }

  // For each templated argument type, add a TVF that accepts exactly one
  // argument of that type.
  const std::vector<std::pair<std::string, SignatureArgumentKind>>
      kSignatureArgumentKinds = {
          {"arg_type_any_1", ARG_TYPE_ANY_1},
          {"arg_type_any_2", ARG_TYPE_ANY_2},
          {"arg_array_type_any_1", ARG_ARRAY_TYPE_ANY_1},
          {"arg_array_type_any_2", ARG_ARRAY_TYPE_ANY_2},
          {"arg_proto_any", ARG_PROTO_ANY},
          {"arg_struct_any", ARG_STRUCT_ANY},
          {"arg_enum_any", ARG_ENUM_ANY},
          {"arg_type_relation", ARG_TYPE_RELATION},
          {"arg_type_arbitrary", ARG_TYPE_ARBITRARY},
      };
  for (const std::pair<std::string, SignatureArgumentKind>& kv :
       kSignatureArgumentKinds) {
    catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
        {absl::StrCat("tvf_exactly_one_", kv.first)},
        FunctionSignature(FunctionArgumentType::RelationWithSchema(
                              output_schema_two_types,
                              /*extra_relation_input_columns_allowed=*/false),
                          {FunctionArgumentType(kv.second)}, context_id++),
        output_schema_two_types));
  }

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_exactly_one_proto"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType(proto_KitchenSinkPB_)},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with a repeating final argument of type int64_t.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_repeating_int64_args"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType(zetasql::types::Int64Type(),
                                              FunctionArgumentType::REPEATED)},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with a repeating final argument of ARG_TYPE_ANY_1.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_repeating_any_one_type_args"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType(ARG_TYPE_ANY_1,
                                              FunctionArgumentType::REPEATED)},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with a repeating final argument of ARG_TYPE_ARBITRARY.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_repeating_arbitrary_type_args"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REPEATED)},
                        context_id++),
      output_schema_two_types));

  // Stop here to avoid hitting lint limits for function length.
}

void SampleCatalog::LoadTableValuedFunctions2() {
  TVFRelation empty_output_schema({});

  const std::vector<OutputColumn> kOutputColumnsAllTypes =
      GetOutputColumnsForAllTypes(types_);

  TVFRelation output_schema_two_types =
      GetOutputSchemaWithTwoTypes(kOutputColumnsAllTypes);

  // Generate an output schema that returns an int64_t value table.
  TVFRelation output_schema_int64_value_table =
      TVFRelation::ValueTable(types_->get_int64());

  // Generate an output schema that returns a proto value table.
  TVFRelation output_schema_proto_value_table = TVFRelation::ValueTable(
      GetProtoType(zetasql_test__::TestExtraPB::descriptor()));

  TVFRelation output_schema_two_types_with_pseudo_columns(
      {TVFSchemaColumn(kOutputColumnsAllTypes[0].name,
                       kOutputColumnsAllTypes[0].type),
       TVFSchemaColumn(kOutputColumnsAllTypes[1].name,
                       kOutputColumnsAllTypes[1].type),
       TVFSchemaColumn("RowId", types_->get_int64(),
                       /*is_pseudo_column_in=*/true),
       TVFSchemaColumn("PartitionName", types_->get_string(),
                       /*is_pseudo_column_in=*/true)});

  // Generate an output schema that returns an int64_t value table.
  TVFRelation output_schema_value_table_with_pseudo_columns =
      TVFRelation::ValueTable(
          GetProtoType(zetasql_test__::TestExtraPB::descriptor()),
          {TVFSchemaColumn("RowId", types_->get_int64(),
                           /*is_pseudo_column_in=*/true),
           TVFSchemaColumn("PartitionName", types_->get_string(),
                           /*is_pseudo_column_in=*/true)})
          .value();

  int64_t context_id = 0;

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_no_arg_returning_fixed_output_with_pseudo_columns"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types_with_pseudo_columns,
                            /*extra_relation_input_columns_allowed=*/false),
                        FunctionArgumentTypeList(), context_id++),
      output_schema_two_types_with_pseudo_columns));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_no_arg_returning_value_table_with_pseudo_columns"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_value_table_with_pseudo_columns,
                            /*extra_relation_input_columns_allowed=*/false),
                        FunctionArgumentTypeList(), context_id++),
      output_schema_value_table_with_pseudo_columns));

  // Add a TVF with exactly one relation argument.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_with_fixed_output"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::AnyRelation()}, context_id++),
      output_schema_two_types));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_two_models_with_fixed_output"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              TVFRelation({{"label", zetasql::types::DoubleType()}}),
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyModel(), FunctionArgumentType::AnyModel()},
          context_id++),
      TVFRelation({{"label", zetasql::types::DoubleType()}})));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_one_model_arg_with_fixed_output"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kTypeDouble, zetasql::types::DoubleType()},
                           {kTypeString, zetasql::types::StringType()}}),
              /*extra_relation_input_columns_allowed=*/false),
          {
              FunctionArgumentType::RelationWithSchema(
                  TVFRelation(
                      {{kColumnNameKey, zetasql::types::Int64Type()},
                       {kColumnNameValue, zetasql::types::StringType()}}),
                  /*extra_relation_input_columns_allowed=*/false),
              FunctionArgumentType::AnyModel(),
          },
          context_id++),
      TVFRelation({{kTypeDouble, zetasql::types::DoubleType()},
                   {kTypeString, zetasql::types::StringType()}})));

  // Add a TVF with exactly one relation argument. The output schema is set to
  // be the same as the input schema.
  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_one_relation_arg_output_schema_is_input_schema"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation()}, context_id++)));

  // Add a TVF with exactly one optional relation argument. The output schema is
  // set to be the same as the input schema.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_optional_relation_arg_return_int64_value_table"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_int64_value_table,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType(ARG_TYPE_RELATION,
                                              FunctionArgumentType::OPTIONAL)},
                        context_id++),
      output_schema_int64_value_table));

  // Add a TVF with one relation argument and one integer argument. The output
  // schema is set to be the same as the input schema of the relation argument.
  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_one_relation_arg_output_schema_is_input_schema_plus_int64_arg"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType(zetasql::types::Int64Type())},
                        context_id++)));

  // Add one TVF with two relation arguments that forwards the schema of the
  // first relation argument to the output of the TVF.
  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_two_relation_args_output_schema_is_input_schema"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType::AnyRelation()},
                        context_id++)));

  // Add one TVF with two relation arguments with the second one optional that
  // forwards the schema of the first relation argument to the output of the
  // TVF.
  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_two_relation_args_second_optional_output_schema_is_input_schema"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType(ARG_TYPE_RELATION,
                                              FunctionArgumentType::OPTIONAL)},
                        context_id++)));

  // Add one TVF with three arguments: The first one is required model; The
  // second is optional table; The third is optional struct.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_model_evaluation_args"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::AnyModel(),
                         FunctionArgumentType(ARG_TYPE_RELATION,
                                              FunctionArgumentType::OPTIONAL),
                         FunctionArgumentType(ARG_STRUCT_ANY,
                                              FunctionArgumentType::OPTIONAL)},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with exactly two relation arguments.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_two_relation_args"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType::AnyRelation()},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with exactly two relation arguments that returns an int64_t value
  // table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_two_relation_args_return_int64_value_table"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_int64_value_table,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType::AnyRelation()},
                        context_id++),
      output_schema_int64_value_table));

  // Add a TVF with exactly two relation arguments that returns a proto value
  // table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_two_relation_args_return_proto_value_table"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_proto_value_table,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType::AnyRelation()},
                        context_id++),
      output_schema_proto_value_table));

  // Add a TVF with exactly one argument of ARG_TYPE_RELATION and another
  // argument of type int64_t.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_int64_arg_one_relation_arg"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType(zetasql::types::Int64Type()),
                         FunctionArgumentType::AnyRelation()},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one argument of ARG_TYPE_RELATION and another
  // argument of type int64_t.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_one_int64_arg"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType(zetasql::types::Int64Type())},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument and repeating int64_t arguments.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_repeating_int64_args"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType(zetasql::types::Int64Type(),
                                              FunctionArgumentType::REPEATED)},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with one relation argument and also a repeating final argument of
  // ARG_TYPE_ANY_1.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_repeating_any_one_type_args"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType(ARG_TYPE_ANY_1,
                                              FunctionArgumentType::REPEATED)},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one int64_t column and one string column.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_int64_string_input_columns"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kTypeInt64, zetasql::types::Int64Type()},
                           {kTypeString, zetasql::types::StringType()}}),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one int64_t column and one string column, and no extra columns are allowed
  // in the input relation.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_only_int64_string_input_columns"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kTypeInt64, zetasql::types::Int64Type()},
                           {kTypeString, zetasql::types::StringType()}}),
              /*extra_relation_input_columns_allowed=*/false)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with two relation arguments, one with a required input schema of
  // one uint64_t column and one string column, and the other with a required
  // input schema of one date column and one string column.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_two_relation_args_uint64_string_and_date_string_input_columns"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
               TVFRelation({{kTypeUInt64, zetasql::types::Uint64Type()},
                            {kTypeString, zetasql::types::StringType()}}),
               /*extra_relation_input_columns_allowed=*/true),
           FunctionArgumentType::RelationWithSchema(
               TVFRelation({{kTypeDate, zetasql::types::DateType()},
                            {kTypeString, zetasql::types::StringType()}}),
               /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF one relation argument with a required input schema of many
  // supported types.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_required_input_schema_many_types"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::RelationWithSchema(
                            TVFRelation({{kTypeBool, types_->get_bool()},
                                         {kTypeBytes, types_->get_bytes()},
                                         {kTypeDate, types_->get_date()},
                                         {kTypeDouble, types_->get_double()},
                                         {kTypeFloat, types_->get_float()},
                                         {kTypeInt32, types_->get_int32()},
                                         {kTypeInt64, types_->get_int64()},
                                         {kTypeString, types_->get_string()},
                                         {kTypeTime, types_->get_timestamp()},
                                         {kTypeUInt32, types_->get_uint32()},
                                         {kTypeUInt64, types_->get_uint64()}}),
                            /*extra_relation_input_columns_allowed=*/true)},
                        context_id++),
      output_schema_two_types));

  const std::string kMyEnum = "myenum";
  const std::string kMyDate = "mydate";
  const std::string kInt64a = "int64a";
  const std::string kInt64b = "int64b";
  const std::string kInt64c = "int64c";

  // Add a TVF with exactly one relation argument with a required input schema
  // of one enum column.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_one_enum_input_column"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({TVFRelation::Column(
                  kMyEnum,
                  GetEnumType(zetasql_test__::TestEnum_descriptor()))}),
              /*extra_relation_input_columns_allowed=*/true
              )},
          context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one date column.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_one_date_input_column"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({TVFRelation::Column(kMyDate, types_->get_date())}),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of three int64_t columns.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_three_int64_input_columns"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({TVFRelation::Column(kInt64a, types_->get_int64()),
                           TVFRelation::Column(kInt64b, types_->get_int64()),
                           TVFRelation::Column(kInt64c, types_->get_int64())}),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one proto column value table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_input_proto_value_table"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::RelationWithSchema(
                            TVFRelation::ValueTable(GetProtoType(
                                zetasql_test__::TestExtraPB::descriptor())),
                            /*extra_relation_input_columns_allowed=*/true)},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one int64_t column value table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_int64_input_value_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation::ValueTable(zetasql::types::Int64Type()),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one int64_t column value table, and forwards the input schema to the
  // output schema.
  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_one_relation_arg_int64_input_value_table_forward_schema"},
      FunctionSignature(
          ARG_TYPE_RELATION,
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation::ValueTable(zetasql::types::Int64Type()),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id++)));

  // Add a TVF with exactly one relation argument with a fixed schema that
  // returns a proto value table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_return_proto_value_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_proto_value_table,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation(
                  {TVFRelation::Column(kTypeString, types_->get_string()),
                   TVFRelation::Column(kTypeInt64, types_->get_int64())}),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_proto_value_table));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one int64_t column, and extra input columns are allowed.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_key_input_column_extra_input_columns_allowed"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kColumnNameKey, zetasql::types::Int64Type()}}),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one string column, and extra input columns are allowed.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_filename_input_column_extra_input_columns_allowed"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::RelationWithSchema(
                            TVFRelation({{kColumnNameFilename,
                                          zetasql::types::StringType()}}),
                            /*extra_relation_input_columns_allowed=*/true)},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one int64_t column, and extra input columns are not allowed.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_key_input_column_extra_input_columns_banned"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kColumnNameKey, zetasql::types::Int64Type()}}),
              /*extra_relation_input_columns_allowed=*/false)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one column whose name is "uint32" but whose type is actually uint64_t.
  // Then it is possible to call this TVF with the SimpleTypes table and type
  // coercion should coerce the provided column named "uint32" to type uint64_t.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_uint64_input_column_named_uint32"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kTypeUInt32, zetasql::types::Uint64Type()}}),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with exactly one relation argument with a required input schema
  // of one int64_t column and one string column.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_key_filename_input_columns"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
              TVFRelation(
                  {{kColumnNameKey, zetasql::types::Int64Type()},
                   {kColumnNameFilename, zetasql::types::StringType()}}),
              /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF that takes two scalar named arguments.
  const auto named_required_format_arg = zetasql::FunctionArgumentType(
      types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                .set_argument_name("format_string"));
  const auto named_required_date_arg = zetasql::FunctionArgumentType(
      types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                .set_argument_name("date_string"));
  const auto named_required_any_relation_arg = zetasql::FunctionArgumentType(
      ARG_TYPE_RELATION,
      zetasql::FunctionArgumentTypeOptions().set_argument_name(
          "any_relation_arg"));
  const auto named_required_schema_relation_arg =
      zetasql::FunctionArgumentType(
          ARG_TYPE_RELATION, FunctionArgumentTypeOptions(
                                 output_schema_two_types,
                                 /*extra_relation_input_columns_allowed=*/false)
                                 .set_argument_name("schema_relation_arg"));
  const auto named_required_value_table_relation_arg =
      zetasql::FunctionArgumentType(
          ARG_TYPE_RELATION,
          FunctionArgumentTypeOptions(
              output_schema_proto_value_table,
              /*extra_relation_input_columns_allowed=*/false)
              .set_argument_name("value_table_relation_arg"));
  const auto named_optional_string_arg = zetasql::FunctionArgumentType(
      types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                .set_cardinality(FunctionArgumentType::OPTIONAL)
                                .set_argument_name("format_string"));
  const auto named_optional_date_arg = zetasql::FunctionArgumentType(
      types_->get_string(), zetasql::FunctionArgumentTypeOptions()
                                .set_cardinality(FunctionArgumentType::OPTIONAL)
                                .set_argument_name("date_string"));
  const auto named_optional_any_relation_arg = zetasql::FunctionArgumentType(
      ARG_TYPE_RELATION, zetasql::FunctionArgumentTypeOptions()
                             .set_cardinality(FunctionArgumentType::OPTIONAL)
                             .set_argument_name("any_relation_arg"));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_required_scalar_args"},
      {FunctionArgumentType::RelationWithSchema(
           output_schema_two_types,
           /*extra_relation_input_columns_allowed=*/false),
       {named_required_format_arg, named_required_date_arg},
       /*context_id=*/-1},
      output_schema_two_types));

  // Add a TVF with two named optional arguments.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_optional_scalar_args"},
      {FunctionArgumentType::RelationWithSchema(
           output_schema_two_types,
           /*extra_relation_input_columns_allowed=*/false),
       {named_optional_string_arg, named_optional_date_arg},
       /*context_id=*/-1},
      output_schema_two_types));

  // Add a TVF with one optional named "any table" relation argument.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_optional_any_relation_arg"},
      {FunctionArgumentType::RelationWithSchema(
           output_schema_two_types,
           /*extra_relation_input_columns_allowed=*/false),
       {named_optional_any_relation_arg},
       /*context_id=*/-1},
      output_schema_two_types));

  // Add a TVF with one optional named "any table" relation argument and an
  // optional scalar argument.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_optional_any_relation_arg_optional_scalar_arg"},
      {FunctionArgumentType::RelationWithSchema(
           output_schema_two_types,
           /*extra_relation_input_columns_allowed=*/false),
       {named_optional_any_relation_arg, named_optional_string_arg},
       /*context_id=*/-1},
      output_schema_two_types));

  // Add a TVF with one required named "any table" relation argument.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_required_any_relation_arg"},
      {FunctionArgumentType::RelationWithSchema(
           output_schema_two_types,
           /*extra_relation_input_columns_allowed=*/false),
       {named_required_any_relation_arg},
       /*context_id=*/-1},
      output_schema_two_types));

  // Add a TVF with one named relation argument with a required schema.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_required_schema_relation_arg"},
      {FunctionArgumentType::RelationWithSchema(
           output_schema_two_types,
           /*extra_relation_input_columns_allowed=*/false),
       {named_required_schema_relation_arg},
       /*context_id=*/-1},
      output_schema_two_types));

  // Add a TVF with one named relation argument with a value table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_required_value_table_relation_arg"},
      {FunctionArgumentType::RelationWithSchema(
           output_schema_two_types,
           /*extra_relation_input_columns_allowed=*/false),
       {named_required_value_table_relation_arg},
       /*context_id=*/-1},
      output_schema_two_types));

  // Add a TVF with a combination of named scalar and relation arguments.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_scalar_and_relation_args"},
      {FunctionArgumentType::RelationWithSchema(
           output_schema_two_types,
           /*extra_relation_input_columns_allowed=*/false),
       {named_required_format_arg, named_required_schema_relation_arg},
       /*context_id=*/-1},
      output_schema_two_types));
}

void SampleCatalog::LoadTableValuedFunctionsWithStructArgs() {
  const std::vector<OutputColumn> kOutputColumnsAllTypes =
      GetOutputColumnsForAllTypes(types_);
  TVFRelation output_schema_two_types =
      GetOutputSchemaWithTwoTypes(kOutputColumnsAllTypes);

  const Type* array_string_type = nullptr;
  ZETASQL_CHECK_OK(types_->MakeArrayType(types_->get_string(), &array_string_type));

  const Type* struct_type1 = nullptr;
  ZETASQL_CHECK_OK(types_->MakeStructType({{"field1", array_string_type},
                                   {"field2", array_string_type}},
                                  &struct_type1));
  const Type* struct_type2 = nullptr;
  ZETASQL_CHECK_OK(types_->MakeStructType({{"field1", array_string_type},
                                   {"field2", array_string_type},
                                   {"field3", array_string_type}},
                                  &struct_type2));

  const auto named_struct_arg1 = zetasql::FunctionArgumentType(
      struct_type1, zetasql::FunctionArgumentTypeOptions().set_argument_name(
                        "struct_arg1"));
  const auto named_struct_arg2 = zetasql::FunctionArgumentType(
      struct_type2, zetasql::FunctionArgumentTypeOptions().set_argument_name(
                        "struct_arg2"));

  // A TVF with struct args.
  auto tvf = absl::make_unique<FixedOutputSchemaTVF>(
      std::vector<std::string>{"tvf_named_struct_args"},
      FunctionSignature{FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {named_struct_arg1, named_struct_arg2},
                        /*context_id=*/-1},
      output_schema_two_types);
  catalog_->AddOwnedTableValuedFunction(tvf.release());
}

void SampleCatalog::LoadTVFWithExtraColumns() {
  int64_t context_id = 0;

  // Add a TVF with appended columns of valid ZetaSQL types.
  catalog_->AddOwnedTableValuedFunction(
      new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          {"tvf_append_columns"},
          FunctionSignature(ARG_TYPE_RELATION, {ARG_TYPE_RELATION},
                            context_id++),
          {TVFSchemaColumn("append_col_int64", zetasql::types::Int64Type()),
           TVFSchemaColumn("append_col_int32", zetasql::types::Int32Type()),
           TVFSchemaColumn("append_col_uint32", zetasql::types::Uint32Type()),
           TVFSchemaColumn("append_col_uint64", zetasql::types::Uint64Type()),
           TVFSchemaColumn("append_col_bytes", zetasql::types::BytesType()),
           TVFSchemaColumn("append_col_bool", zetasql::types::BoolType()),
           TVFSchemaColumn("append_col_float", zetasql::types::FloatType()),
           TVFSchemaColumn("append_col_double", zetasql::types::DoubleType()),
           TVFSchemaColumn("append_col_date", zetasql::types::DateType()),
           TVFSchemaColumn("append_col_timestamp",
                           zetasql::types::TimestampType()),
           TVFSchemaColumn("append_col_numeric",
                           zetasql::types::NumericType()),
           TVFSchemaColumn("append_col_bignumeric",
                           zetasql::types::BigNumericType()),
           TVFSchemaColumn("append_col_json",
                           zetasql::types::JsonType()),
           TVFSchemaColumn("append_col_string",
                           zetasql::types::StringType())}));

  // Add a TVF with an appended column that has empty name.
  catalog_->AddOwnedTableValuedFunction(
      new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          {"tvf_append_no_column"},
          FunctionSignature(ARG_TYPE_RELATION, {ARG_TYPE_RELATION},
                            context_id++),
          {}));

  const auto named_required_any_relation_arg = zetasql::FunctionArgumentType(
      ARG_TYPE_RELATION,
      zetasql::FunctionArgumentTypeOptions().set_argument_name(
          "any_relation_arg"));

  // Add a TVF with one required named "any table" relation argument.
  catalog_->AddOwnedTableValuedFunction(
      new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          {"tvf_append_columns_any_relation_arg"},
          FunctionSignature(ARG_TYPE_RELATION,
                            {named_required_any_relation_arg},
                            /*context_id=*/context_id++),
          {TVFSchemaColumn("append_col_int32",
                           zetasql::types::Int32Type())}));
}

void SampleCatalog::LoadDescriptorTableValuedFunctions() {
  int64_t context_id = 0;
  const std::vector<OutputColumn> kOutputColumnsAllTypes =
      GetOutputColumnsForAllTypes(types_);

  TVFRelation output_schema_two_types =
      GetOutputSchemaWithTwoTypes(kOutputColumnsAllTypes);

  const std::string kInt64a = "int64a";
  const std::string kInt64b = "int64b";

  // Add a TVF with a table parameter and a descriptor with -1 table offset.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_one_descriptor"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
               TVFRelation({TVFRelation::Column(kInt64a, types_->get_int64())}),
               /*extra_relation_input_columns_allowed=*/true),
           FunctionArgumentType::AnyDescriptor()},
          context_id++),
      output_schema_two_types));

  // Add a TVF with two table parameters, one descriptor with 0 table offset
  // and one descriptor with 1 table offset.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_two_relations_arg_two_descriptors_resolved_names"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
               TVFRelation({TVFRelation::Column(kInt64a, types_->get_int64())}),
               /*extra_relation_input_columns_allowed=*/true),
           FunctionArgumentType::RelationWithSchema(
               TVFRelation({TVFRelation::Column(kInt64b, types_->get_int64())}),
               /*extra_relation_input_columns_allowed=*/true),
           FunctionArgumentType::AnyDescriptor(0),
           FunctionArgumentType::AnyDescriptor(1)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with a table parameter and a descriptor with 0 table offset.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_one_descriptor_resolved_names"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::RelationWithSchema(
               TVFRelation({TVFRelation::Column(kInt64a, types_->get_int64())}),
               /*extra_relation_input_columns_allowed=*/true),
           FunctionArgumentType::AnyDescriptor(0)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with a descriptor with 1 table offset and a table parameter.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_descriptor_resolved_names_one_relation_arg"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyDescriptor(1),
           FunctionArgumentType::RelationWithSchema(
               TVFRelation({TVFRelation::Column(kInt64a, types_->get_int64()),
                            TVFRelation::Column(kInt64b, types_->get_int64())}),
               /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with a descriptor with 1 table offset and a table parameter with
  // ambiguous column naming problem.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_descriptor_resolved_names_one_relation_arg_ambiguous_naming"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyDescriptor(1),
           FunctionArgumentType::RelationWithSchema(
               TVFRelation({TVFRelation::Column(kInt64a, types_->get_int64()),
                            TVFRelation::Column(kInt64b, types_->get_int64())}),
               /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      output_schema_two_types));

  // Add a TVF with a descriptor with 1 table offset, a table parameter and a
  // descriptor with -1 table offset.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_descriptor_resolved_names_one_relation_arg_one_descriptor_arg"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyDescriptor(1),
           FunctionArgumentType::RelationWithSchema(
               TVFRelation({TVFRelation::Column(kInt64a, types_->get_int64())}),
               /*extra_relation_input_columns_allowed=*/true),
           FunctionArgumentType::AnyDescriptor()},
          context_id++),
      output_schema_two_types));
}

void SampleCatalog::LoadConnectionTableValuedFunctions() {
  int64_t context_id = 0;

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_connection_arg_with_fixed_output"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kTypeString, zetasql::types::StringType()}}),
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyConnection()}, context_id++),
      TVFRelation({{kTypeString, zetasql::types::StringType()}})));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_connection_one_string_arg_with_fixed_output"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kTypeInt64, zetasql::types::Int64Type()},
                           {kTypeString, zetasql::types::StringType()}}),
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyConnection(),
           FunctionArgumentType(zetasql::types::StringType())},
          context_id++),
      TVFRelation({{kTypeInt64, zetasql::types::Int64Type()},
                   {kTypeString, zetasql::types::StringType()}})));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_two_connections_with_fixed_output"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              TVFRelation({{kTypeDouble, zetasql::types::DoubleType()},
                           {kTypeString, zetasql::types::StringType()}}),
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyConnection(),
           FunctionArgumentType::AnyConnection()},
          context_id++),
      TVFRelation({{kTypeDouble, zetasql::types::DoubleType()},
                   {kTypeString, zetasql::types::StringType()}})));
}

void SampleCatalog::LoadTableValuedFunctionsWithDeprecationWarnings() {
  // Generate an empty output schema.
  TVFRelation empty_output_schema({});

  const std::vector<OutputColumn> kOutputColumnsAllTypes =
      GetOutputColumnsForAllTypes(types_);

  TVFRelation output_schema_two_types =
      GetOutputSchemaWithTwoTypes(kOutputColumnsAllTypes);

  int context_id = 0;

  // Add a TVF that triggers a deprecation warning.
  FunctionSignature deprecation_warning_signature(
      FunctionArgumentType::RelationWithSchema(
          empty_output_schema,
          /*extra_relation_input_columns_allowed=*/false),
      FunctionArgumentTypeList(), context_id++);
  deprecation_warning_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/11)});
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_deprecation_warning"}, deprecation_warning_signature,
      output_schema_two_types));

  // Add a TVF that triggers two deprecation warnings with the same kind.
  FunctionSignature two_deprecation_warnings_same_kind_signature(
      FunctionArgumentType::RelationWithSchema(
          empty_output_schema,
          /*extra_relation_input_columns_allowed=*/false),
      FunctionArgumentTypeList(), context_id++);
  two_deprecation_warnings_same_kind_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/12),
       CreateDeprecationWarning(/*id=*/13)});
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_two_deprecation_warnings_same_kind"},
      two_deprecation_warnings_same_kind_signature, output_schema_two_types));

  // Add a TVF that triggers two deprecation warnings with different kinds.
  FunctionSignature two_deprecation_warnings_signature(
      FunctionArgumentType::RelationWithSchema(
          empty_output_schema,
          /*extra_relation_input_columns_allowed=*/false),
      FunctionArgumentTypeList(), context_id++);
  two_deprecation_warnings_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/14),
       CreateDeprecationWarning(
           /*id=*/15, DeprecationWarning::DEPRECATED_FUNCTION_SIGNATURE)});
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_two_deprecation_warnings"}, two_deprecation_warnings_signature,
      output_schema_two_types));

  // Add a TVF with exactly one relation argument. The output schema is set to
  // be the same as the input schema. The TVF also triggers a deprecation
  // warning.
  FunctionSignature forward_deprecation_signature(
      ARG_TYPE_RELATION, {FunctionArgumentType::AnyRelation()}, context_id++);
  forward_deprecation_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/16)});
  catalog_->AddOwnedTableValuedFunction(new ForwardInputSchemaToOutputSchemaTVF(
      {"tvf_one_relation_arg_output_schema_is_input_schema_deprecation"},
      forward_deprecation_signature));

  // Add a TVF with three arguments: The first one is required model; The
  // second is optional table; The third is named optional struct.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_model_optional_table_named_struct"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyModel(),
           FunctionArgumentType(ARG_TYPE_RELATION,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               ARG_STRUCT_ANY,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with two arguments: The first is named optional struct. The
  // second is a named optional table;
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_struct_named_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(
               ARG_STRUCT_ANY,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("barfoo")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with two arguments: The first is named optional struct. The
  // second is a named optional table;
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_named_proto_named_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(
               ARG_PROTO_ANY,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("barfoo")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is required model; The
  // second is optional scalar; The third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_model_optional_scalar_named_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyModel(),
           FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first two are optional scalars; The
  // third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_optional_scalars_named_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is required table; The
  // second is optional scalar; The third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_table_optional_scalar_named_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyRelation(),
           FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is optional table; The
  // second is optional scalar; The third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_optional_table_optional_scalar_named_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(ARG_TYPE_RELATION,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is required scalar; The
  // second is optional model; The third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_scalar_optional_model_named_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(ARG_TYPE_ARBITRARY),
           FunctionArgumentType(ARG_TYPE_MODEL, FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is required scalar; The
  // second is optional model; The third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_optional_scalar_optional_model_named_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(ARG_TYPE_MODEL, FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is optional scalar; The
  // second is named optional table; The third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_optional_scalar_named_tables"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("barfoo")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is optional scalar; The
  // second is named optional model; The third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_optional_scalar_named_model_named_table"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               ARG_TYPE_MODEL,
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("barfoo")
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with a required table, a named optional table, a mandatory
  // named table and a required mandatory named table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_required_named_optional_required_tables"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(ARG_TYPE_RELATION,
                                FunctionArgumentType::REQUIRED),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("table2")
                   .set_argument_name_is_mandatory(true)
                   .set_cardinality(FunctionArgumentType::REQUIRED)),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("table3")
                   .set_cardinality(FunctionArgumentType::OPTIONAL)),
           FunctionArgumentType(
               ARG_TYPE_RELATION,
               FunctionArgumentTypeOptions()
                   .set_argument_name("table4")
                   .set_argument_name_is_mandatory(true)
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is required model; The
  // second is optional string; The third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_model_optional_string_optional_table"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_two_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        {FunctionArgumentType::AnyModel(),
                         FunctionArgumentType(zetasql::types::StringType(),
                                              FunctionArgumentType::OPTIONAL),
                         FunctionArgumentType(ARG_TYPE_RELATION,
                                              FunctionArgumentType::OPTIONAL)},
                        context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is required model; The
  // second is optional table; The third is named optional string with default.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_model_optional_table_named_string_default"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType::AnyModel(),
           FunctionArgumentType(ARG_TYPE_RELATION,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               zetasql::types::StringType(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_default(zetasql::values::String("default"))
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));

  // Add a TVF with three arguments: The first one is required model; The
  // second is a string with default; The third is named optional table.
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_optional_table_default_mandatory_string"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_two_types,
              /*extra_relation_input_columns_allowed=*/false),
          {FunctionArgumentType(ARG_TYPE_RELATION,
                                FunctionArgumentType::OPTIONAL),
           FunctionArgumentType(
               zetasql::types::StringType(),
               FunctionArgumentTypeOptions()
                   .set_argument_name("foobar")
                   .set_argument_name_is_mandatory(true)
                   .set_default(zetasql::values::String("default"))
                   .set_cardinality(FunctionArgumentType::OPTIONAL))},
          context_id++),
      output_schema_two_types));
}

void SampleCatalog::LoadTemplatedSQLTableValuedFunctions() {
  const std::string kColumnNameKey = "key";
  const std::string kColumnNameDate = "date";
  int context_id = 0;

  // Add a TVF with a simple valid templated SQL body.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_one"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{}, ParseResumeLocation::FromString("select 1 as x")));

  // Add a templated SQL TVF that calls another templated SQL TVF.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_call_tvf_templated_select_one"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{},
      ParseResumeLocation::FromString(
          "select * from tvf_templated_select_one()")));

  // Add a templated SQL TVF that calls another templated SQL TVF twice.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_call_tvf_templated_select_one_twice"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{},
      ParseResumeLocation::FromString(
          "select * from tvf_templated_select_one() union all "
          "select * from tvf_templated_select_one()")));

  // Add a TVF with a valid templated SQL body that refers to a scalar argument.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_int64_arg"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(types::Int64Type())},
                        context_id++),
      /*arg_name_list=*/{"x"}, ParseResumeLocation::FromString("select x")));

  // Add a TVF with a valid templated SQL body that refers to a scalar argument
  // with a name that is potentially ambiguous with an in scope column name.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_int64_arg_with_name_ambiguity"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(types::Int64Type())},
                        context_id++),
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("SELECT x FROM (SELECT -99 AS x)")));

  // Add a TVF with a valid templated SQL body that refers to a scalar argument.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_any_scalar_arg"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY)},
                        context_id++),
      /*arg_name_list=*/{"x"}, ParseResumeLocation::FromString("select x")));

  // Add a TVF with a valid templated SQL body that performs addition on a
  // scalar argument. The function signature accepts a single argument of any
  // scalar type.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_scalar_arg_plus_integer"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("select x + 42")));

  // Add a TVF with a valid templated SQL body that accepts an input argument
  // where the name contains '$'. The function signature accepts a single
  // argument of any scalar type.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_scalar_arg_plus_integer_accept_dollars_col_name"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*arg_name_list=*/{"$col1"},
      ParseResumeLocation::FromString("select `$col1` as x")));

  // Add a TVF with a valid templated SQL body that returns an output column
  // where the name contains '$'. The function signature accepts a single
  // argument of any scalar type.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_scalar_arg_plus_integer_return_dollars_col_name"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("select x as `$col1`")));

  // Add a TVF with a valid templated SQL body that performs concatenation on a
  // scalar argument. The function signature accepts a single argument of any
  // scalar type.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_scalar_arg_concat_string"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("select concat(x, 'abc') as y")));

  // Add a TVF with a valid templated SQL body that performs a proto field
  // access on a scalar argument. The function signature accepts a single
  // argument of any scalar type.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_scalar_arg_proto_field_access"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        context_id++),
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("select x.int32_field as y")));

  // Add a TVF with a valid templated SQL body that refers to a relation
  // argument using specific column names.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_relation_arg_using_column_names"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation()}, context_id++),
      /*arg_name_list=*/{"t"},
      ParseResumeLocation::FromString("select key, value from t")));

  // Add a TVF with a simple valid templated SQL body that selects a name from
  // a templated input table argument.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_a"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation()}, context_id++),
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("select a from x")));

  // Add a TVF with a valid templated SQL body that refers to a relation
  // argument using "select *".
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_relation_arg_using_select_star"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation()}, context_id++),
      /*arg_name_list=*/{"t"},
      ParseResumeLocation::FromString("select * from t")));

  // Add a TVF with a templated SQL body that refers to a relation argument
  // using "select 1". The TVF is missing an output column name.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_relation_arg_using_select_one"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation()}, context_id++),
      /*arg_name_list=*/{"t"},
      ParseResumeLocation::FromString("(select 1 from t limit 1)")));

  // Add a TVF with a valid templated SQL body that refers to a relation
  // argument and also a table in the catalog.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_relation_arg_and_catalog_table"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation()}, context_id++),
      /*arg_name_list=*/{"t"},
      ParseResumeLocation::FromString(
          "(select * from t) union all (select * from keyvalue)")));

  // Add a TVF with a valid templated SQL body that refers to two relation
  // arguments and uses a SQL WITH clause.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_two_relation_args"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType::AnyRelation(),
                         FunctionArgumentType::AnyRelation()},
                        context_id++),
      /*arg_name_list=*/{"s", "t"},
      ParseResumeLocation::FromString(
          "with w1 as (select * from s),\n"
          "     w2 as (select * from t)\n"
          "select * from w1 inner join w2 using (key) order by key limit 1")));

  // Add a TVF with a valid templated SQL body that refers to both a scalar
  // argument and a relation argument.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_scalar_and_relation_args"},
      FunctionSignature(
          ARG_TYPE_RELATION,
          {FunctionArgumentType(types::Int64Type()),
           FunctionArgumentType::RelationWithSchema(
               TVFRelation({{kColumnNameKey, zetasql::types::Int64Type()}}),
               /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      /*arg_name_list=*/{"x", "t"},
      ParseResumeLocation::FromString("select key from t where key < x")));

  // This is the same TVF as above, but without a schema specified for the
  // relation argument.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_scalar_and_relation_args_no_schema"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(types::Int64Type()),
                         FunctionArgumentType::AnyRelation()},
                        context_id++),
      /*arg_name_list=*/{"x", "t"},
      ParseResumeLocation::FromString("select key from t where key < x")));

  // This is the same TVF as above, but without a schema specified for the
  // relation argument.
  const StructType* arg_type = nullptr;
  std::vector<StructType::StructField> fields{{"y", types::Int64Type()},
                                              {"z", types::StringType()}};
  ZETASQL_CHECK_OK(type_factory()->MakeStructType(fields, &arg_type));
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_with_struct_param"},
      FunctionSignature(
          ARG_TYPE_RELATION,
          {FunctionArgumentType(arg_type), FunctionArgumentType::AnyRelation()},
          context_id++),
      /*arg_name_list=*/{"x", "t"},
      ParseResumeLocation::FromString("select x.y from t as x")));

  // Add a TVF with a valid templated SQL body that refers to both a scalar
  // date argument and a relation argument.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_scalar_date_and_relation_args"},
      FunctionSignature(
          ARG_TYPE_RELATION,
          {FunctionArgumentType(types::DateType()),
           FunctionArgumentType::RelationWithSchema(
               TVFRelation({{kColumnNameDate, zetasql::types::DateType()}}),
               /*extra_relation_input_columns_allowed=*/true)},
          context_id++),
      /*arg_name_list=*/{"d", "t"},
      ParseResumeLocation::FromString(
          "select `date` from t where `date` < d")));

  // Add a TVF with a valid templated SQL body that refers to both a scalar
  // argument of any type and a relation argument of any table.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_any_scalar_and_relation_args"},
      FunctionSignature(ARG_TYPE_RELATION,
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY),
                         FunctionArgumentType::AnyRelation()},
                        context_id++),
      /*arg_name_list=*/{"s", "t"},
      ParseResumeLocation::FromString("select *, s from t")));

  // Add an invalid TVF with a simple templated SQL body missing an output
  // column name.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_one_missing_col_name"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{}, ParseResumeLocation::FromString("select 1")));

  // Add an invalid templated SQL TVF with a parse error in the function body.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_parse_error"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{}, ParseResumeLocation::FromString("a b c d e")));

  // Add an invalid templated SQL TVF with an analysis error in the function
  // body.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_analysis_error"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{},
      ParseResumeLocation::FromString("select * from invalidtable")));

  // Add an invalid templated SQL TVF where the function body is not a query.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_function_body_not_query"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{},
      ParseResumeLocation::FromString(
          "insert keyvalue (key, value) values (1, 'one')")));

  // Add an invalid templated SQL TVF that attempts to refer to a query
  // parameter.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_function_body_refer_to_parameter"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{},
      ParseResumeLocation::FromString(
          "select @test_param_bool from keyvalue")));

  // Add an invalid templated SQL TVF where the function body is empty.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_function_body_empty"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{}, ParseResumeLocation::FromString("")));

  // Add an invalid templated SQL TVF that directly calls itself.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_recursive"}, FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{},
      ParseResumeLocation::FromString("select * from tvf_recursive()")));

  // Add two invalid templated SQL TVFs that indirectly call themselves.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_calls_self_indirectly_1"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{},
      ParseResumeLocation::FromString(
          "select * from tvf_calls_self_indirectly_2()")));

  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_calls_self_indirectly_2"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{},
      ParseResumeLocation::FromString(
          "select * from tvf_calls_self_indirectly_1()")));

  // Add a templated SQL TVF that calls a templated SQL function that calls the
  // original templated SQL TVF again, to make sure cycle detection works.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_calls_udf_calls_same_tvf"},
      FunctionSignature(ARG_TYPE_RELATION, {}, context_id++),
      /*arg_name_list=*/{},
      ParseResumeLocation::FromString(
          "select udf_calls_tvf_calls_same_udf()")));

  // Add a templated SQL TVF that calls a TVF that triggers a deprecation
  // warning.
  FunctionSignature deprecation_warning_signature(
      ARG_TYPE_RELATION, /*arguments=*/{}, context_id++);
  deprecation_warning_signature.SetAdditionalDeprecationWarnings(
      {CreateDeprecationWarning(/*id=*/1001)});
  catalog_->AddOwnedTableValuedFunction(
      new TemplatedSQLTVF({"tvf_templated_calls_tvf_deprecation_warning"},
                          deprecation_warning_signature,
                          /*arg_name_list=*/{},
                          ParseResumeLocation::FromString(
                              "select * from tvf_deprecation_warning()")));

  FunctionSignature signature_return_key_int64_col(
      FunctionArgumentType::RelationWithSchema(
          TVFRelation({{kColumnNameKey, zetasql::types::Int64Type()}}),
          /*extra_relation_input_columns_allowed=*/true),
      /*arguments=*/{FunctionArgumentType(ARG_TYPE_ARBITRARY)}, context_id++);
  FunctionSignature signature_return_key_int64_and_value_string_cols(
      FunctionArgumentType::RelationWithSchema(
          TVFRelation({{kColumnNameKey, zetasql::types::Int64Type()},
                       {kColumnNameValue, zetasql::types::StringType()}}),
          /*extra_relation_input_columns_allowed=*/true),
      /*arguments=*/
      {FunctionArgumentType(ARG_TYPE_ARBITRARY),
       FunctionArgumentType(ARG_TYPE_ARBITRARY)},
      context_id++);
  FunctionSignature signature_return_value_table_string_col(
      FunctionArgumentType::RelationWithSchema(
          TVFRelation::ValueTable(zetasql::types::StringType()),
          /*extra_relation_input_columns_allowed=*/true),
      /*arguments=*/{FunctionArgumentType(ARG_TYPE_ARBITRARY)}, context_id++);

  // Add a templated TVF with a required signature of a single INT64 column
  // named "key".
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_x_with_required_output_schema"},
      signature_return_key_int64_col,
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("select x as key")));

  // Add an invalid templated TVF that returns a duplicate column name.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_duplicate_output_column_with_required_output_schema"},
      signature_return_key_int64_col,
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("select x as key, 42 as key")));

  // Add a templated TVF with a required non-value-table output schema.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_missing_output_column_with_required_output_schema"},
      signature_return_key_int64_col,
      /*arg_name_list=*/{"x"}, ParseResumeLocation::FromString("select x")));

  // Add a templated TVF with a required value-table output schema that returns
  // a value table.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_as_value_x_with_required_"
       "value_table_output_schema"},
      signature_return_value_table_string_col,
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("select as value x")));

  // Add a templated TVF with a required value-table output schema that returns
  // a non-value table of the same type.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_x_with_required_value_table_output_schema"},
      signature_return_value_table_string_col,
      /*arg_name_list=*/{"x"}, ParseResumeLocation::FromString("select x")));

  // Add a templated TVF with a required value-table output schema that returns
  // NULL.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_null_with_required_value_table_output_schema"},
      signature_return_value_table_string_col,
      /*arg_name_list=*/{"x"}, ParseResumeLocation::FromString("select null")));

  // Add a templated TVF with a required value-table output schema that returns
  // NULL casted to string type.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_select_null_str_with_required_value_table_output_schema"},
      signature_return_value_table_string_col,
      /*arg_name_list=*/{"x"},
      ParseResumeLocation::FromString("select cast(null as string)")));

  // Add a templated TVF with a required output schema with two columns. The
  // function body returns the two columns in opposite order.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_return_swapped_cols_required_output_schema"},
      signature_return_key_int64_and_value_string_cols,
      /*arg_name_list=*/{"key", "value"},
      ParseResumeLocation::FromString("select value, key")));

  // Add a templated TVF with a required output schema with two columns. The
  // function body returns the two columns in opposite order, plus an extra
  // column.
  catalog_->AddOwnedTableValuedFunction(new TemplatedSQLTVF(
      {"tvf_templated_return_swapped_cols_plus_extra_required_output_schema"},
      signature_return_key_int64_and_value_string_cols,
      /*arg_name_list=*/{"key", "value"},
      ParseResumeLocation::FromString("select value, key, 42 as x")));
}

void SampleCatalog::LoadTableValuedFunctionsWithAnonymizationUid() {
  // Generate an output schema that returns every possible type.
  const std::vector<OutputColumn> output_columns =
      GetOutputColumnsForAllTypes(types_);
  TVFRelation::ColumnList columns;
  columns.reserve(output_columns.size());
  for (const auto& kv : output_columns) {
    columns.emplace_back(kv.name, kv.type);
  }
  TVFRelation output_schema_all_types(columns);

  int context_id = 0;
  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_no_args_with_anonymization_uid"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_all_types,
                            /*extra_relation_input_columns_allowed=*/false),
                        FunctionArgumentTypeList(), context_id++),
      AnonymizationInfo::Create({"column_int64"}).value_or(nullptr),
      output_schema_all_types));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_relation_arg_with_anonymization_uid"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_all_types,
              /*extra_relation_input_columns_allowed=*/false),
          FunctionArgumentTypeList({FunctionArgumentType(ARG_TYPE_RELATION)}),
          context_id++),
      AnonymizationInfo::Create({"column_int64"}).value_or(nullptr),
      output_schema_all_types));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_one_templated_arg_with_anonymization_uid"},
      FunctionSignature(
          FunctionArgumentType::RelationWithSchema(
              output_schema_all_types,
              /*extra_relation_input_columns_allowed=*/false),
          FunctionArgumentTypeList({FunctionArgumentType(ARG_TYPE_ARBITRARY)}),
          context_id++),
      AnonymizationInfo::Create({"column_int64"}).value_or(nullptr),
      output_schema_all_types));

  TVFRelation output_schema_proto(
      {{"user_info", GetProtoType(zetasql_test__::TestExtraPB::descriptor())}});

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_no_args_with_nested_anonymization_uid"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_proto,
                            /*extra_relation_input_columns_allowed=*/false),
                        FunctionArgumentTypeList(), context_id++),
      AnonymizationInfo::Create({"user_info", "int32_val1"}).value_or(nullptr),
      output_schema_proto));

  TVFRelation output_schema_proto_value_table = TVFRelation::ValueTable(
      GetProtoType(zetasql_test__::TestExtraPB::descriptor()));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_no_args_value_table_with_anonymization_uid"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_proto_value_table,
                            /*extra_relation_input_columns_allowed=*/false),
                        FunctionArgumentTypeList(), context_id++),
      AnonymizationInfo::Create({"int32_val1"}).value_or(nullptr),
      output_schema_proto_value_table));

  TVFRelation output_schema_proto_value_table_with_nested_int =
      TVFRelation::ValueTable(
          GetProtoType(zetasql_test__::KitchenSinkPB::descriptor()));

  catalog_->AddOwnedTableValuedFunction(new FixedOutputSchemaTVF(
      {"tvf_no_args_value_table_with_nested_anonymization_uid"},
      FunctionSignature(FunctionArgumentType::RelationWithSchema(
                            output_schema_proto_value_table_with_nested_int,
                            /*extra_relation_input_columns_allowed=*/false),
                        FunctionArgumentTypeList(), context_id++),
      AnonymizationInfo::Create({"nested_value", "nested_int64"})
          .value_or(nullptr),
      output_schema_proto_value_table_with_nested_int));
}

void SampleCatalog::AddProcedureWithArgumentType(std::string type_name,
                                                 const Type* arg_type) {
  auto procedure = absl::WrapUnique(
      new Procedure({absl::StrCat("proc_on_", type_name)},
                    {types_->get_bool(), {arg_type}, /*context_id=*/-1}));
  catalog_->AddOwnedProcedure(std::move(procedure));
}

void SampleCatalog::LoadProcedures() {
  Procedure* procedure = nullptr;

  // Procedure with no arguments.
  procedure = new Procedure({"proc_no_args"},
                            {types_->get_bool(), {}, /*context_id=*/-1});
  catalog_->AddOwnedProcedure(procedure);

  // Add a procedure that takes a specific enum as an argument.
  const EnumType* enum_TestEnum =
      GetEnumType(zetasql_test__::TestEnum_descriptor());
  procedure = new Procedure(
      {"proc_on_TestEnum"},
      {types_->get_bool(), {enum_TestEnum}, /*context_id=*/-1});
  catalog_->AddOwnedProcedure(procedure);

  // Add a procedure to illustrate how repeated/optional arguments are resolved.
  procedure = new Procedure(
      {"proc_on_req_opt_rep"},
      {types_->get_int64(),
          {{types_->get_int64(), FunctionArgumentType::REQUIRED},
           {types_->get_int64(), FunctionArgumentType::REPEATED},
           {types_->get_int64(), FunctionArgumentType::REPEATED},
           {types_->get_int64(), FunctionArgumentType::REQUIRED},
           {types_->get_int64(), FunctionArgumentType::OPTIONAL}},
           /*context_id=*/-1});
  catalog_->AddOwnedProcedure(procedure);

  // Add a procedure with templated arguments.
  procedure = new Procedure(
      {"proc_on_any_any"},
      {types_->get_int64(),
          {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, /*context_id=*/-1});
  catalog_->AddOwnedProcedure(procedure);

  // Add a procedure with templated arguments of arbitrary type.
  procedure = new Procedure({"proc_on_arbitrary_arbitrary"},
                            {types_->get_int64(),
                             {ARG_TYPE_ARBITRARY, ARG_TYPE_ARBITRARY},
                             /*context_id=*/-1});
  catalog_->AddOwnedProcedure(procedure);

  // Add a procedure with one repeated argument.
  procedure = new Procedure(
      {"proc_on_rep"},
      {types_->get_int64(),
          {{types_->get_int64(), FunctionArgumentType::REPEATED}},
          /*context_id=*/-1});
  catalog_->AddOwnedProcedure(procedure);

  // Add a procedure with one optional argument.
  procedure = new Procedure(
      {"proc_on_opt"},
      {types_->get_int64(),
          {{types_->get_int64(), FunctionArgumentType::OPTIONAL}},
          /*context_id=*/-1});
  catalog_->AddOwnedProcedure(procedure);

  // These sample procedures are named 'proc_on_<typename>' with one argument of
  // type <typename> that returns a bool.
  AddProcedureWithArgumentType("bool", types_->get_bool());
  AddProcedureWithArgumentType("int32", types_->get_int32());
  AddProcedureWithArgumentType("int64", types_->get_int64());
  AddProcedureWithArgumentType("uint32", types_->get_uint32());
  AddProcedureWithArgumentType("uint64", types_->get_uint64());
  AddProcedureWithArgumentType("float", types_->get_float());
  AddProcedureWithArgumentType("double", types_->get_double());
  AddProcedureWithArgumentType("date", types_->get_date());
  AddProcedureWithArgumentType("timestamp", types_->get_timestamp());
  AddProcedureWithArgumentType("string", types_->get_string());
}

void SampleCatalog::LoadConstants() {
  // Load constants that are owned by 'catalog_'.
  std::unique_ptr<SimpleConstant> int64_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"TestConstantInt64"},
                                  Value::Int64(1L), &int64_constant));
  catalog_->AddOwnedConstant(int64_constant.release());
  std::unique_ptr<SimpleConstant> string_constant;
  ZETASQL_CHECK_OK(
      SimpleConstant::Create(std::vector<std::string>{"TestConstantString"},
                             Value::String("foo"), &string_constant));
  catalog_->AddOwnedConstant(string_constant.release());

  std::unique_ptr<SimpleConstant> string_constant_nonstandard_name;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"Test Constant-String"},
      Value::String("foo bar"), &string_constant_nonstandard_name));
  catalog_->AddOwnedConstant(string_constant_nonstandard_name.release());

  // Load a constant that is not owned by 'catalog_'.
  const ProtoType* const proto_type =
      GetProtoType(zetasql_test__::KitchenSinkPB::descriptor());
  absl::Cord text_proto = absl::Cord("int64_key_1: 1, int64_key_2: -999");

  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"TestConstantProto"},
                                  Value::Proto(proto_type, text_proto),
                                  &owned_constant_));
  catalog_->AddConstant(owned_constant_.get());

  // Load a constant that conflicts with a table.
  const StructType* table_struct_type;
  ZETASQL_CHECK_OK(types_->MakeStructType({{"key", types_->get_int32()}},
                                  &table_struct_type));
  std::unique_ptr<SimpleConstant> table_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"NameConflictTable"},
      Value::Struct(table_struct_type, {Value::Int32(-3456)}),
      &table_constant));
  catalog_->AddOwnedConstant(table_constant.release());

  // Load a constant that conflicts with a value table.
  std::unique_ptr<SimpleConstant> value_table_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"Int32ValueTable"},
                                  Value::Int32(3), &value_table_constant));
  catalog_->AddOwnedConstant(value_table_constant.release());

  // Load a constant that conflicts with a type.
  std::unique_ptr<SimpleConstant> type_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"NameConflictType"},
                                  Value::Bool(false), &type_constant));
  catalog_->AddOwnedConstant(type_constant.release());

  // Load a constant that conflicts with zero-argument functions.
  std::unique_ptr<SimpleConstant> zero_argument_function_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"sort_count"},
                                  Value::Int64(4),
                                  &zero_argument_function_constant));
  catalog_->AddOwnedConstant(zero_argument_function_constant.release());

  std::unique_ptr<SimpleConstant>
      zero_argument_function_constant_with_optional_parentheses;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"CURRENT_DATE"}, Value::Int64(4),
      &zero_argument_function_constant_with_optional_parentheses));
  catalog_->AddOwnedConstant(
      zero_argument_function_constant_with_optional_parentheses.release());

  // Load a constant that conflicts with a multi-argument function.
  std::unique_ptr<SimpleConstant> multi_argument_function_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"concat"},
                                  Value::Int64(5),
                                  &multi_argument_function_constant));
  catalog_->AddOwnedConstant(multi_argument_function_constant.release());

  // Load a constant that conflicts with a zero-argument TVF.
  std::unique_ptr<SimpleConstant> zero_argument_tvf_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"tvf_no_args"},
                                  Value::Int64(6),
                                  &zero_argument_tvf_constant));
  catalog_->AddOwnedConstant(zero_argument_tvf_constant.release());

  // Load a constant that conflicts with a multi-argument TVF.
  std::unique_ptr<SimpleConstant> multi_argument_tvf_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"tvf_exactly_1_int64_arg"}, Value::Int64(7),
      &multi_argument_tvf_constant));
  catalog_->AddOwnedConstant(multi_argument_tvf_constant.release());

  // Load a constant that conflicts with a zero-argument procedure.
  // The multi-argument case is handled in the nested catalog.
  std::unique_ptr<SimpleConstant> constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create({"proc_no_args"}, Value::Bool(true),
                                  &constant));
  catalog_->AddOwnedConstant(constant.release());

  // Load a constant that conflicts with a catalog.
  const StructType* nested_struct_type;
  ZETASQL_CHECK_OK(types_->MakeStructType(
      {{"a", types_->get_int32()}, {"b", types_->get_int64()}},
      &nested_struct_type));
  const StructType* catalog_struct_type;
  ZETASQL_CHECK_OK(
      types_->MakeStructType({{"a", types_->get_int32()},
                              {"nested_catalog_catalog", nested_struct_type},
                              {"TestConstantBool", types_->get_bool()}},
                             &catalog_struct_type));
  std::unique_ptr<SimpleConstant> catalog_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"nested_catalog_with_catalog"},
      Value::Struct(catalog_struct_type,
                    {Value::Int32(-3456),
                     Value::Struct(nested_struct_type,
                                   {Value::Int32(-3434), Value::Int64(4333)}),
                     Value::Bool(false)}),
      &catalog_constant));
  catalog_->AddOwnedConstant(catalog_constant.release());

  // Load a constant that conflicts with an expression column in standalone
  // expression resolution.
  std::unique_ptr<SimpleConstant> standalone_expression_constant;
  ZETASQL_CHECK_OK(
      SimpleConstant::Create(std::vector<std::string>{"column_KitchenSink"},
                             Value::Int64(8), &standalone_expression_constant));
  catalog_->AddOwnedConstant(standalone_expression_constant.release());

  // Load a constant with a name that resembles a system variable.
  std::unique_ptr<SimpleConstant> sysvar1_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"@@sysvar1"},
                                  Value::Int64(8), &sysvar1_constant));
  catalog_->AddOwnedConstant(sysvar1_constant.release());

  std::unique_ptr<SimpleConstant> sysvar2_constant;
  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"@@sysvar2"},
                                  Value::Int64(8), &sysvar2_constant));
  catalog_->AddOwnedConstant(sysvar2_constant.release());

  // Script variables are managed by the ScriptExecutor. Eventually, they get
  // put into the catalog as constants. For testing, we'll add some "variables"
  // here.
  std::unique_ptr<SimpleConstant> string_variable_foo;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"string_variable_foo"},
      Value::String("string_variable_foo_value"), &string_variable_foo));
  catalog_->AddOwnedConstant(std::move(string_variable_foo));

  std::unique_ptr<SimpleConstant> string_variable_bar;
  ZETASQL_CHECK_OK(SimpleConstant::Create(
      std::vector<std::string>{"string_variable_bar"},
      Value::String("string_variable_bar_value"), &string_variable_bar));
  catalog_->AddOwnedConstant(std::move(string_variable_bar));

  std::unique_ptr<SimpleConstant> int_variable_foo;
  ZETASQL_CHECK_OK(SimpleConstant::Create(std::vector<std::string>{"int_variable_foo"},
                                  Value::Int32(4), &int_variable_foo));
  catalog_->AddOwnedConstant(std::move(int_variable_foo));
}

void SampleCatalog::LoadConnections() {
  auto connection1 = absl::make_unique<SimpleConnection>("connection1");
  auto connection2 = absl::make_unique<SimpleConnection>("connection2");
  owned_connections_[connection1->Name()] = std::move(connection1);
  owned_connections_[connection2->Name()] = std::move(connection2);
  for (auto it = owned_connections_.begin(); it != owned_connections_.end();
       ++it) {
    catalog_->AddConnection(it->second.get());
  }
}

void SampleCatalog::AddOwnedTable(SimpleTable* table) {
  catalog_->AddOwnedTable(absl::WrapUnique(table));
  zetasql_base::InsertOrDie(&tables_, table->Name(), table);
}

void SampleCatalog::LoadWellKnownLambdaArgFunctions() {
  const Type* int64_type = types_->get_int64();
  const Type* bool_type = types_->get_bool();

  // Models ARRAY_FILTER
  auto function = absl::make_unique<Function>(
      "fn_array_filter", "sample_functions", Function::SCALAR);
  function->AddSignature(
      {ARG_ARRAY_TYPE_ANY_1,
       {ARG_ARRAY_TYPE_ANY_1,
        FunctionArgumentType::Lambda({ARG_TYPE_ANY_1}, bool_type)},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<array<T1>>, LAMBDA(<T1>->BOOL)) -> <array<T1>>",
           function->GetSignature(0)->DebugString());
  function->AddSignature(
      {ARG_ARRAY_TYPE_ANY_1,
       {ARG_ARRAY_TYPE_ANY_1,
        FunctionArgumentType::Lambda({ARG_TYPE_ANY_1, int64_type}, bool_type)},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<array<T1>>, LAMBDA((<T1>, INT64)->BOOL)) -> <array<T1>>",
           function->GetSignature(1)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  // Models ARRAY_TRANSFORM
  function = absl::make_unique<Function>("fn_array_transform",
                                         "sample_functions", Function::SCALAR);
  function->AddSignature(
      {ARG_ARRAY_TYPE_ANY_2,
       {ARG_ARRAY_TYPE_ANY_1,
        FunctionArgumentType::Lambda({ARG_TYPE_ANY_1}, ARG_TYPE_ANY_2)},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<array<T1>>, LAMBDA(<T1>-><T2>)) -> <array<T2>>",
           function->GetSignature(0)->DebugString());
  function->AddSignature({ARG_ARRAY_TYPE_ANY_2,
                          {ARG_ARRAY_TYPE_ANY_1,
                           FunctionArgumentType::Lambda(
                               {ARG_TYPE_ANY_1, int64_type}, ARG_TYPE_ANY_2)},
                          /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<array<T1>>, LAMBDA((<T1>, INT64)-><T2>)) -> <array<T2>>",
           function->GetSignature(1)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  function = absl::make_unique<Function>("fn_fp_array_sort", "sample_functions",
                                         Function::SCALAR);
  function->AddSignature({ARG_TYPE_ANY_1,
                          {ARG_ARRAY_TYPE_ANY_1,
                           FunctionArgumentType::Lambda(
                               {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, int64_type)},
                          /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<array<T1>>, LAMBDA((<T1>, <T1>)->INT64)) -> <T1>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  // Models REDUCE function, which takes an input array, an initial state and a
  // function to run over each element with the current state to produce the
  // final state.
  function = absl::make_unique<Function>("fn_fp_array_reduce",
                                         "sample_functions", Function::SCALAR);
  function->AddSignature(
      {ARG_TYPE_ANY_2,
       {ARG_ARRAY_TYPE_ANY_1, ARG_TYPE_ANY_2,
        FunctionArgumentType::Lambda({ARG_TYPE_ANY_2, ARG_TYPE_ANY_1},
                                     ARG_TYPE_ANY_2)},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<array<T1>>, <T2>, LAMBDA((<T2>, <T1>)-><T2>)) -> <T2>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());
}

void SampleCatalog::LoadContrivedLambdaArgFunctions() {
  const Type* int64_type = types_->get_int64();
  const Type* bool_type = types_->get_bool();

  // Demonstrate having to get common super type for two different concrete type
  // for a single template type.
  auto function = absl::make_unique<Function>(
      "fn_fp_T_T_LAMBDA", "sample_functions", Function::SCALAR);
  function->AddSignature(
      {ARG_TYPE_ANY_1,
       {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1,
        FunctionArgumentType::Lambda({ARG_TYPE_ANY_1}, bool_type)},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<T1>, <T1>, LAMBDA(<T1>->BOOL)) -> <T1>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  // fn_fp_ArrayT_T is provided here to show current behavior to make it easier
  // for reader to understand fn_fp_ArrayT_T_LAMBDA.
  function = absl::make_unique<Function>("fn_fp_ArrayT_T", "sample_functions",
                                         Function::SCALAR);
  function->AddSignature({ARG_TYPE_ANY_1,
                          {ARG_ARRAY_TYPE_ANY_1, ARG_TYPE_ANY_1},
                          /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<array<T1>>, <T1>) -> <T1>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  // Demostrate case where we don't have common super type for T1, due to
  // ARRAY<T1>.
  function = absl::make_unique<Function>("fn_fp_ArrayT_T_LAMBDA",
                                         "sample_functions", Function::SCALAR);
  function->AddSignature(
      {ARG_TYPE_ANY_1,
       {ARG_ARRAY_TYPE_ANY_1, ARG_TYPE_ANY_1,
        FunctionArgumentType::Lambda({ARG_TYPE_ANY_1}, bool_type)},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<array<T1>>, <T1>, LAMBDA(<T1>->BOOL)) -> <T1>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  // Demonstrate that lambda argument type inference conflict with final
  // concrete type of templated type influenced by lambda body type.
  function = absl::make_unique<Function>("fn_fp_T_LAMBDA_RET_T",
                                         "sample_functions", Function::SCALAR);
  function->AddSignature(
      {ARG_TYPE_ANY_1,
       {ARG_TYPE_ANY_1,
        FunctionArgumentType::Lambda({ARG_TYPE_ANY_1}, ARG_TYPE_ANY_1)},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(<T1>, LAMBDA(<T1>-><T1>)) -> <T1>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  const auto named_required_format_arg = zetasql::FunctionArgumentType(
      types_->get_string(),
      zetasql::FunctionArgumentTypeOptions().set_argument_name(
          "format_string"));
  // Signature with lambda and named argument before lambda.
  function = absl::make_unique<Function>("fn_fp_named_then_lambda",
                                         "sample_functions", Function::SCALAR);
  function->AddSignature(
      {ARG_TYPE_ANY_1,
       {named_required_format_arg,
        FunctionArgumentType::Lambda({int64_type}, ARG_TYPE_ANY_1)},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(STRING format_string, LAMBDA(INT64-><T1>)) -> <T1>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  // Signature with lambda and named argument after lambda.
  function = absl::make_unique<Function>("fn_fp_lambda_then_named",
                                         "sample_functions", Function::SCALAR);
  function->AddSignature(
      {ARG_TYPE_ANY_1,
       {FunctionArgumentType::Lambda({int64_type}, ARG_TYPE_ANY_1),
        named_required_format_arg},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(LAMBDA(INT64-><T1>), STRING format_string) -> <T1>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  // Signature with lambda and repeated arguments after lambda.
  const auto repeated_arg = zetasql::FunctionArgumentType(
      types_->get_int64(), FunctionArgumentType::REPEATED);
  function = absl::make_unique<Function>("fn_fp_lambda_then_repeated",
                                         "sample_functions", Function::SCALAR);
  function->AddSignature(
      {ARG_TYPE_ANY_1,
       {FunctionArgumentType::Lambda({int64_type}, ARG_TYPE_ANY_1),
        repeated_arg},
       /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(LAMBDA(INT64-><T1>), repeated INT64) -> <T1>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());

  // Signature with lambda and repeated arguments before lambda.
  function = absl::make_unique<Function>("fn_fp_repeated_arg_then_lambda",
                                         "sample_functions", Function::SCALAR);
  function->AddSignature({ARG_TYPE_ANY_1,
                          {repeated_arg, FunctionArgumentType::Lambda(
                                             {int64_type}, ARG_TYPE_ANY_1)},
                          /*context_id=*/-1});
  ZETASQL_CHECK_EQ("(repeated INT64, LAMBDA(INT64-><T1>)) -> <T1>",
           function->GetSignature(0)->DebugString());
  catalog_->AddOwnedFunction(function.release());
}

void SampleCatalog::AddSqlDefinedFunction(
    absl::string_view name, FunctionSignature signature,
    const std::vector<std::string>& argument_names,
    absl::string_view function_body_sql,
    const LanguageOptions& language_options) {
  AnalyzerOptions analyzer_options;
  analyzer_options.set_language(language_options);
  ZETASQL_CHECK_EQ(argument_names.size(), signature.arguments().size());
  for (int i = 0; i < argument_names.size(); ++i) {
    ZETASQL_CHECK_NE(signature.argument(i).type(), nullptr);
    ZETASQL_CHECK_OK(analyzer_options.AddExpressionColumn(
        argument_names[i], signature.argument(i).type()));
  }
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_CHECK_OK(AnalyzeExpressionForAssignmentToType(
      function_body_sql, analyzer_options, catalog_.get(),
      catalog_->type_factory(), signature.result_type().type(),
      &analyzer_output));
  std::unique_ptr<SQLFunction> function;
  ZETASQL_CHECK_OK(SQLFunction::Create(
      std::string(name), FunctionEnums::SCALAR, {signature},
      /*function_options=*/{}, analyzer_output->resolved_expr(), argument_names,
      /*aggregate_expression_list=*/{}, /*parse_resume_location=*/{},
      &function));
  catalog_->AddOwnedFunction(function.release());
  sql_function_artifacts_.emplace_back(std::move(analyzer_output));
}

// Add a SQL function to catalog starting from a full create_function
// statement.
void SampleCatalog::AddSqlDefinedFunctionFromCreate(
    absl::string_view create_function, const LanguageOptions& language_options,
    bool inline_sql_functions) {
  // Ensure the language options used allow CREATE FUNCTION
  LanguageOptions language = language_options;
  language.AddSupportedStatementKind(RESOLVED_CREATE_FUNCTION_STMT);
  language.EnableLanguageFeature(FEATURE_V_1_3_INLINE_LAMBDA_ARGUMENT);
  language.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  language.EnableLanguageFeature(FEATURE_TEMPLATE_FUNCTIONS);
  AnalyzerOptions analyzer_options;
  analyzer_options.set_language(language);
  analyzer_options.set_enabled_rewrites(/*rewrites=*/{});
  analyzer_options.enable_rewrite(REWRITE_INLINE_SQL_FUNCTIONS,
                                  inline_sql_functions);
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_CHECK_OK(AnalyzeStatement(create_function, analyzer_options, catalog_.get(),
                            catalog_->type_factory(), &analyzer_output))
      << create_function;
  const ResolvedStatement* resolved = analyzer_output->resolved_statement();
  ZETASQL_CHECK(resolved->Is<ResolvedCreateFunctionStmt>());
  const ResolvedCreateFunctionStmt* resolved_create =
      resolved->GetAs<ResolvedCreateFunctionStmt>();
  if (resolved_create->function_expression() != nullptr) {
    std::unique_ptr<SQLFunction> function;
    ZETASQL_CHECK_OK(SQLFunction::Create(
        absl::StrJoin(resolved_create->name_path(), "."), FunctionEnums::SCALAR,
        {resolved_create->signature()}, /*function_options=*/{},
        resolved_create->function_expression(),
        resolved_create->argument_name_list(), /*aggregate_expression_list=*/{},
        /*parse_resume_location=*/{}, &function));
    catalog_->AddOwnedFunction(function.release());
  } else {
    auto template_function = absl::make_unique<TemplatedSQLFunction>(
        resolved_create->name_path(), resolved_create->signature(),
        resolved_create->argument_name_list(),
        ParseResumeLocation::FromStringView(resolved_create->code()));
    catalog_->AddOwnedFunction(template_function.release());
  }
  sql_function_artifacts_.emplace_back(std::move(analyzer_output));
}

void SampleCatalog::LoadSqlFunctions(const LanguageOptions& language_options) {
  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION NullaryPi() RETURNS FLOAT64 AS (3.141597); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION NullaryWithSubquery()
          AS (EXISTS (SELECT 1 FROM UNNEST([1,2,3]) AS e WHERE e = 2)); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION NullaryWithCommonTableExpression()
          AS (EXISTS (WITH t AS (SELECT [1,2,3] AS arr)
                      SELECT 1 FROM t, t.arr as e WHERE e = 2)); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION NullaryWithLambda()
          AS (ARRAY_INCLUDES([1, 2, 3], e -> e = 2)); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION NullaryWithSqlFunctionCallPreInlined()
          AS (NullaryPi()); )",
      language_options, /*inline_sql_functions=*/true);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION NullaryWithSqlFunctionCallNotPreInlined()
          AS (NullaryPi()); )",
      language_options, /*inline_sql_functions=*/false);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION UnaryIncrement(a INT64) AS ( a + 1 ); )",
      language_options);

  // Creating the function using AnalyzeExpressionForAssignmentToType results in
  // a function body expression with ResolvedExpressionColumn where arguments
  // are referenced instead of a ResolvedArgumentRef. That appears to be done
  // in several catalogs, so we want to test that case too.
  const Type* int64_type = types_->get_int64();
  FunctionSignature int_int = {int64_type,
                               {int64_type},
                               /*context_id=*/static_cast<int64_t>(0)};
  AddSqlDefinedFunction("UnaryIncrementRefArg", int_int, {"a"}, "a + 1",
                        language_options);

  // Function from sql/modules/math_utils/math_utils.sqlm
  // References each of its arguments more than once.
  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION FloorDiv(n INT64, d INT64) RETURNS INT64
          AS ( DIV(n, d) - IF(n < 0 AND MOD(n, d) != 0, 1, 0) ); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION IgnoresArg(a INT64) AS (1); )", language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION ReferencesArgsInsideSubquery(a INT64, b INT64)
          AS ((SELECT a + b)); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION ReferencesArgInsideCte(a INT64)
          AS ((WITH t AS (SELECT a AS c) SELECT c FROM t)); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION ReferencesArgOutsideCte(a INT64)
          AS ((WITH t AS (SELECT 1 AS c) SELECT c + a FROM t)); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"(  CREATE FUNCTION ReferencesArgInsideLambda(a INT64)
           AS (ARRAY_TRANSFORM([1, 2, 3], e->e = a)); )",
      language_options);

  // This function is logically equivalent to ARRAY_REVERSE
  AddSqlDefinedFunctionFromCreate(
      R"(  CREATE FUNCTION REVERSE_ARRAY(input_arr ANY TYPE)
           AS (IF (input_arr IS NULL,
                   NULL,
                   ARRAY(SELECT e
                         FROM UNNEST(input_arr) AS e WITH OFFSET
                         ORDER BY OFFSET desc))); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION TimesTwo(arg ANY TYPE) AS (arg + arg); )",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION SUM_DOUBLE_ARRAY(input_arr ANY TYPE)
          AS ((SELECT SUM(e)
               FROM UNNEST(ARRAY_TRANSFORM(input_arr, e->TimesTwo(e))) e));)",
      language_options);

  AddSqlDefinedFunctionFromCreate(
      R"( CREATE FUNCTION CallsPi0() AS (NullaryPi());)", language_options,
      /*inline_sql_functions=*/false);

  for (int i = 0; i < 25; ++i) {
    AddSqlDefinedFunctionFromCreate(
        absl::StrCat("CREATE FUNCTION CallsPi", i + 1, "() AS (CallsPi", i,
                     "());"),
        language_options, /*inline_sql_functions=*/false);
  }
}

}  // namespace zetasql
