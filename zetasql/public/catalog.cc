//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/public/catalog.h"

#include "google/protobuf/io/tokenizer.h"
#include "zetasql/public/strings.h"
#include "absl/strings/str_join.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// TODO We may want to change the interfaces to just return a bool.
// The resolver never uses the error message that gets returned from here.
zetasql_base::Status Catalog::FindTable(
    const absl::Span<const std::string>& path,
    const Table** table,
    const FindOptions& options) {
  *table = nullptr;
  if (path.empty()) {
    return EmptyNamePathInternalError("Table");
  }

  const std::string& name = path.front();
  if (path.size() > 1) {
    Catalog* catalog = nullptr;
    ZETASQL_RETURN_IF_ERROR(GetCatalog(name, &catalog, options));
    if (catalog == nullptr) {
      return TableNotFoundError(path);
    }
    const absl::Span<const std::string> path_suffix =
        path.subspan(1, path.size() - 1);
    return catalog->FindTable(path_suffix, table, options);
  } else {
    ZETASQL_RETURN_IF_ERROR(GetTable(name, table, options));
    if (*table == nullptr) {
      return TableNotFoundError(path);
    }
    return ::zetasql_base::OkStatus();
  }
}

zetasql_base::Status Catalog::FindModel(const absl::Span<const std::string>& path,
                                const Model** model,
                                const FindOptions& options) {
  *model = nullptr;
  if (path.empty()) {
    return EmptyNamePathInternalError("Model");
  }

  const std::string& name = path.front();
  if (path.size() > 1) {
    Catalog* catalog = nullptr;
    ZETASQL_RETURN_IF_ERROR(GetCatalog(name, &catalog, options));
    if (catalog == nullptr) {
      return ModelNotFoundError(path);
    }
    const absl::Span<const std::string> path_suffix =
        path.subspan(1, path.size() - 1);
    return catalog->FindModel(path_suffix, model, options);
  } else {
    ZETASQL_RETURN_IF_ERROR(GetModel(name, model, options));
    if (*model == nullptr) {
      return ModelNotFoundError(path);
    }
    return ::zetasql_base::OkStatus();
  }
}

zetasql_base::Status Catalog::FindFunction(
    const absl::Span<const std::string>& path,
    const Function** function,
    const FindOptions& options) {
  *function = nullptr;
  if (path.empty()) {
    return EmptyNamePathInternalError("Function");
  }

  const std::string& name = path.front();
  if (path.size() > 1) {
    Catalog* catalog = nullptr;
    ZETASQL_RETURN_IF_ERROR(GetCatalog(name, &catalog, options));
    if (catalog == nullptr) {
      return FunctionNotFoundError(path);
    }
    const absl::Span<const std::string> path_suffix
      = path.subspan(1, path.size() - 1);
    return catalog->FindFunction(path_suffix, function, options);
  } else {
    ZETASQL_RETURN_IF_ERROR(GetFunction(name, function, options));
    if (*function == nullptr) {
      return FunctionNotFoundError(path);
    }
    return ::zetasql_base::OkStatus();
  }
}

zetasql_base::Status Catalog::FindTableValuedFunction(
    const absl::Span<const std::string>& path,
    const TableValuedFunction** function,
    const FindOptions& options) {
  *function = nullptr;
  if (path.empty()) {
    return EmptyNamePathInternalError("TableValuedFunction");
  }

  const std::string& name = path.front();
  if (path.size() > 1) {
    Catalog* catalog = nullptr;
    ZETASQL_RETURN_IF_ERROR(GetCatalog(name, &catalog, options));
    if (catalog == nullptr) {
      return TableValuedFunctionNotFoundError(path);
    }
    const absl::Span<const std::string> path_suffix =
        path.subspan(1, path.size() - 1);
    return catalog->FindTableValuedFunction(path_suffix, function, options);
  } else {
    ZETASQL_RETURN_IF_ERROR(GetTableValuedFunction(name, function, options));
    if (*function == nullptr) {
      return TableValuedFunctionNotFoundError(path);
    }
    return ::zetasql_base::OkStatus();
  }
}

zetasql_base::Status Catalog::FindProcedure(
    const absl::Span<const std::string>& path,
    const Procedure** procedure,
    const FindOptions& options) {
  *procedure = nullptr;
  if (path.empty()) {
    return EmptyNamePathInternalError("Procedure");
  }
  const std::string& name = path.front();
  if (path.size() > 1) {
    Catalog* catalog = nullptr;
    ZETASQL_RETURN_IF_ERROR(GetCatalog(name, &catalog, options));
    if (catalog == nullptr) {
      return ProcedureNotFoundError(path);
    }
    const absl::Span<const std::string> path_suffix =
        path.subspan(1, path.size() - 1);
    return catalog->FindProcedure(path_suffix, procedure, options);
  } else {
    ZETASQL_RETURN_IF_ERROR(GetProcedure(name, procedure, options));
    if (*procedure == nullptr) {
      return ProcedureNotFoundError(path);
    }
    return ::zetasql_base::OkStatus();
  }
}

zetasql_base::Status Catalog::FindType(
    const absl::Span<const std::string>& path,
    const Type** type,
    const FindOptions& options) {
  *type = nullptr;
  if (path.empty()) {
    return EmptyNamePathInternalError("Type");
  }

  const std::string& name = path.front();
  if (path.size() > 1) {
    Catalog* catalog = nullptr;
    ZETASQL_RETURN_IF_ERROR(GetCatalog(name, &catalog, options));
    if (catalog == nullptr) {
      // FindType-specific behavior: If we have a path, and it looks like it
      // could be a valid unquoted multi-part proto name, try looking up that
      // proto name as a type.
      const std::string proto_name = ConvertPathToProtoName(path);
      if (!proto_name.empty()) {
        ZETASQL_RETURN_IF_ERROR(GetType(proto_name, type, options));
        if (*type != nullptr) {
          return ::zetasql_base::OkStatus();
        } else {
          return ::zetasql_base::NotFoundErrorBuilder(ZETASQL_LOC)
                 << "Type not found: " << ToIdentifierLiteral(proto_name)
                 << " is not a type and " << ToIdentifierLiteral(name)
                 << " is not a nested catalog in catalog " << FullName();
        }
      }
      return TypeNotFoundError(path);
    }
    const absl::Span<const std::string> path_suffix =
        path.subspan(1, path.size() - 1);
    return catalog->FindType(path_suffix, type, options);
  } else {
    ZETASQL_RETURN_IF_ERROR(GetType(name, type, options));
    if (*type == nullptr) {
      return TypeNotFoundError(path);
    }
    return ::zetasql_base::OkStatus();
  }
}

zetasql_base::Status Catalog::FindConstant(const absl::Span<const std::string> path,
                                   const Constant** constant,
                                   const FindOptions& options) {
  int num_names_consumed = 0;
  ZETASQL_RETURN_IF_ERROR(
      FindConstantWithPathPrefix(path, &num_names_consumed, constant, options));
  if (num_names_consumed < path.size()) {
    return ConstantNotFoundError(path);
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Catalog::FindConstantWithPathPrefix(
    const absl::Span<const std::string> path, int* num_names_consumed,
    const Constant** constant, const FindOptions& options) {
  // Return an error if <path> is empty.
  *constant = nullptr;
  *num_names_consumed = 0;
  if (path.empty()) {
    return EmptyNamePathInternalError("Constant");
  }

  // Find the longest prefix of <path> that can be resolved to either a constant
  // or a catalog.
  zetasql_base::Status find_constant_with_path_prefix_status =
      FindConstantWithPathPrefixImpl(path, num_names_consumed, constant,
                                     options);

  // If no prefix of <path> resolves to a constant or a catalog, return
  // NOT_FOUND for the whole <path> in this catalog. Otherwise return the
  // resolved constant or whatever other error occurred.
  return find_constant_with_path_prefix_status.code() ==
                 zetasql_base::StatusCode::kNotFound
             ? ConstantNotFoundError(path)
             : find_constant_with_path_prefix_status;
}

zetasql_base::Status Catalog::FindConstantWithPathPrefixImpl(
    const absl::Span<const std::string> path, int* num_names_consumed,
    const Constant** constant, const FindOptions& options) {
  // Get the first step in <path>.
  ZETASQL_RET_CHECK(!path.empty());
  const std::string& name = path.front();

  // If there are multiple steps in <path>, try to resolve the first step
  // against a sub-catalog. If that succeeds, move on to the next step.
  if (path.size() > 1) {
    Catalog* next_resolved_catalog = nullptr;
    ZETASQL_RETURN_IF_ERROR(GetCatalog(name, &next_resolved_catalog, options));
    if (next_resolved_catalog != nullptr) {
      // Continue resolving <path> assuming that <name> is a catalog reference.
      // If this fails, fall through to backtracking on <name> which could also
      // be a struct-typed constant (see below).
      const absl::Span<const std::string> path_suffix =
          path.subspan(1, path.size() - 1);
      const zetasql_base::Status find_constant_with_path_prefix_status =
          next_resolved_catalog->FindConstantWithPathPrefix(
              path_suffix, num_names_consumed, constant, options);
      if (find_constant_with_path_prefix_status.code() !=
          zetasql_base::StatusCode::kNotFound) {
        *num_names_consumed += 1;
        return find_constant_with_path_prefix_status;
      }
    }
  }

  // At any step in <path>, once <name> does not resolve to a catalog, it must
  // reference a constant.
  ZETASQL_RETURN_IF_ERROR(GetConstant(name, constant, options));
  if (*constant != nullptr) {
    *num_names_consumed += 1;
    return ::zetasql_base::OkStatus();
  }

  // If no constant is found, reset <num_names_consumed> and return an error.
  *num_names_consumed = 0;
  return ConstantNotFoundError(path);
}

zetasql_base::Status Catalog::FindObject(absl::Span<const std::string> path,
                                 const Function** object,
                                 const FindOptions& options) {
  return FindFunction(path, object, options);
}
zetasql_base::Status Catalog::FindObject(absl::Span<const std::string> path,
                                 const TableValuedFunction** object,
                                 const FindOptions& options) {
  return FindTableValuedFunction(path, object, options);
}
zetasql_base::Status Catalog::FindObject(absl::Span<const std::string> path,
                                 const Table** object,
                                 const FindOptions& options) {
  return FindTable(path, object, options);
}
zetasql_base::Status Catalog::FindObject(const absl::Span<const std::string> path,
                                 const Model** object,
                                 const FindOptions& options) {
  return FindModel(path, object, options);
}
zetasql_base::Status Catalog::FindObject(absl::Span<const std::string> path,
                                 const Procedure** object,
                                 const FindOptions& options) {
  return FindProcedure(path, object, options);
}
zetasql_base::Status Catalog::FindObject(absl::Span<const std::string> path,
                                 const Type** object,
                                 const FindOptions& options) {
  return FindType(path, object, options);
}
zetasql_base::Status Catalog::FindObject(absl::Span<const std::string> path,
                                 const Constant** object,
                                 const FindOptions& options) {
  return FindConstant(path, object, options);
}

std::string Catalog::ConvertPathToProtoName(absl::Span<const std::string> path) {
  // Return "" if path contains an invalid proto identifier name.
  for (const std::string& identifier : path) {
    if (!google::protobuf::io::Tokenizer::IsIdentifier(identifier)) {
      return "";
    }
  }
  // This also returns "" for empty paths.
  return absl::StrJoin(path, ".");
}

std::string Catalog::SuggestTable(const absl::Span<const std::string>& mistyped_path) {
  return "";
}

std::string Catalog::SuggestModel(const absl::Span<const std::string>& mistyped_path) {
  return "";
}

std::string Catalog::SuggestFunction(const absl::Span<const std::string>& mistyped_path) {
  return "";
}

std::string Catalog::SuggestTableValuedFunction(
    const absl::Span<const std::string>& mistyped_path) {
  return "";
}

std::string Catalog::SuggestConstant(const absl::Span<const std::string>& mistyped_path) {
  return "";
}

zetasql_base::Status Catalog::GetTable(
    const std::string& name,
    const Table** table,
    const FindOptions& options) {
  *table = nullptr;
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Catalog::GetModel(const std::string& name, const Model** model,
                               const FindOptions& options) {
  *model = nullptr;
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Catalog::GetFunction(
    const std::string& name,
    const Function** function,
    const FindOptions& options) {
  *function = nullptr;
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Catalog::GetTableValuedFunction(
    const std::string& name,
    const TableValuedFunction** function,
    const FindOptions& options) {
  *function = nullptr;
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Catalog::GetProcedure(
    const std::string& name,
    const Procedure** procedure,
    const FindOptions& options) {
  *procedure = nullptr;
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Catalog::GetType(
    const std::string& name,
    const Type** type,
    const FindOptions& options) {
  *type = nullptr;
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Catalog::GetCatalog(
    const std::string& name,
    Catalog** catalog,
    const FindOptions& options) {
  *catalog = nullptr;
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Catalog::GetConstant(
    const std::string& name,
    const Constant** constant,
    const FindOptions& options) {
  *constant = nullptr;
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Catalog::GenericNotFoundError(
    const std::string& object_type, absl::Span<const std::string> path) const {
  const std::string& name = path.front();
  if (path.size() > 1) {
    return ::zetasql_base::NotFoundErrorBuilder(ZETASQL_LOC)
        << object_type << " not found: catalog " << ToIdentifierLiteral(name)
        << " not found in catalog " << FullName();
  }
  return ::zetasql_base::NotFoundErrorBuilder(ZETASQL_LOC)
      << object_type << " not found: " << ToIdentifierLiteral(name)
      << " not found in catalog " << FullName();
}

zetasql_base::Status Catalog::TableNotFoundError(absl::Span<const std::string> path)
    const {
  return GenericNotFoundError("Table", path);
}

zetasql_base::Status Catalog::ModelNotFoundError(absl::Span<const std::string> path) const {
  return GenericNotFoundError("Model", path);
}

zetasql_base::Status Catalog::FunctionNotFoundError(absl::Span<const std::string> path)
    const {
  return GenericNotFoundError("Function", path);
}

zetasql_base::Status Catalog::TableValuedFunctionNotFoundError(
    const absl::Span<const std::string> path) const {
  return GenericNotFoundError("Table function", path);
}

zetasql_base::Status Catalog::ProcedureNotFoundError(
    absl::Span<const std::string> path) const {
  return GenericNotFoundError("Procedure", path);
}

zetasql_base::Status Catalog::TypeNotFoundError(absl::Span<const std::string> path) const {
  return GenericNotFoundError("Type", path);
}

zetasql_base::Status Catalog::ConstantNotFoundError(
    const absl::Span<const std::string> path) const {
  return GenericNotFoundError("Constant", path);
}

zetasql_base::Status Catalog::EmptyNamePathInternalError(
    const std::string& object_type) const {
  return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
      << "Invalid empty " << object_type << " name path";
}

}  // namespace zetasql
