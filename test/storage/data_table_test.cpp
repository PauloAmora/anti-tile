//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_test.cpp
//
// Identification: test/storage/data_table_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/data_table.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "storage/tile_group.h"
#include "storage/database.h"


//===--------------------------------------------------------------------===//
// Data Table Tests
//===--------------------------------------------------------------------===//


peloton::catalog::Column GetColumnInfo(int index) {
  using namespace peloton;
  const bool is_inlined = true;
  std::string not_null_constraint_name = "not_null";
  catalog::Column dummy_column;

  switch (index) {
    case 0: {
      auto column = catalog::Column(
          type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
          "COL_A", is_inlined);

      column.AddConstraint(catalog::Constraint(ConstraintType::NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 1: {
      auto column = catalog::Column(
          type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
          "COL_B", is_inlined);

      column.AddConstraint(catalog::Constraint(ConstraintType::NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 2: {
      auto column = catalog::Column(
          type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL),
          "COL_C", is_inlined);

      column.AddConstraint(catalog::Constraint(ConstraintType::NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 3: {
      auto column = catalog::Column(type::Type::VARCHAR, 25,  // Column length.
                                    "COL_D", !is_inlined);    // inlined.

      column.AddConstraint(catalog::Constraint(ConstraintType::NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    default: {
      throw ExecutorException("Invalid column index : " +
                              std::to_string(index));
    }
  }

  return dummy_column;
}

int main() {
  using namespace peloton;
  // Create a table and wrap it in logical tiles

  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase("default_database", nullptr);
  auto database = catalog->GetDatabaseWithName("default_database");
  catalog::Schema *table_schema = new catalog::Schema(
       {GetColumnInfo(0), GetColumnInfo(1), GetColumnInfo(2), GetColumnInfo(3)});
   std::string table_name("test_table");

   // Create table.
   bool own_schema = true;
   bool adapt_table = false;
   storage::DataTable *table = storage::TableFactory::GetDataTable(
       INVALID_OID, 0, table_schema, table_name,
       5, own_schema, adapt_table);


  // Create the new column map
  storage::column_map_type column_map;
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(1, 0);
  column_map[3] = std::make_pair(1, 1);

  auto theta = 0.0;

  // Transform the tile group
  table->TransformTileGroup(0, theta);

  // Create the another column map
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(0, 2);
  column_map[3] = std::make_pair(1, 0);

  // Transform the tile group
  table->TransformTileGroup(0, theta);

  // Create the another column map
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(1, 0);
  column_map[2] = std::make_pair(1, 1);
  column_map[3] = std::make_pair(1, 2);

  // Transform the tile group
  table->TransformTileGroup(0, theta);
  return 0;
}


