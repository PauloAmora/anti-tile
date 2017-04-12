//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database.cpp
//
// Identification: src/storage/database.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "catalog/foreign_key.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/database.h"
#include "storage/table_factory.h"

namespace peloton {
namespace storage {

Database::Database(const oid_t &database_oid) : database_oid(database_oid) {}

Database::~Database() {
  // Clean up all the tables
  LOG_TRACE("Deleting tables from database");
  for (auto table : tables) delete table;

  LOG_TRACE("Finish deleting tables from database");
}

//===--------------------------------------------------------------------===//
// TABLE
//===--------------------------------------------------------------------===//

void Database::AddTable(storage::DataTable *table, bool is_catalog) {
  {
    std::lock_guard<std::mutex> lock(database_mutex);
    tables.push_back(table);
  }
}

storage::DataTable *Database::GetTableWithOid(const oid_t table_oid) const {
  for (auto table : tables)
    if (table->GetOid() == table_oid) return table;
  throw CatalogException("Table with oid = " + std::to_string(table_oid) + " is not found");
  return nullptr;
}

storage::DataTable *Database::GetTableWithName(const std::string table_name) const {
  for (auto table : tables)
    if (table->GetName() == table_name) return table;
  throw CatalogException("Table '" + table_name + "' does not exist");
  return nullptr;
}

void Database::DropTableWithOid(const oid_t table_oid) {

  {
    std::lock_guard<std::mutex> lock(database_mutex);

    oid_t table_offset = 0;
    for (auto table : tables) {
      if (table->GetOid() == table_oid) {
        delete table;
        break;
      }
      table_offset++;
    }
    PL_ASSERT(table_offset < tables.size());

    // Drop the table
    tables.erase(tables.begin() + table_offset);
  }
}

storage::DataTable *Database::GetTable(const oid_t table_offset) const {
  PL_ASSERT(table_offset < tables.size());
  auto table = tables.at(table_offset);
  return table;
}

oid_t Database::GetTableCount() const { return tables.size(); }

//===--------------------------------------------------------------------===//
// UTILITIES
//===--------------------------------------------------------------------===//

// Get a string representation for debugging
const std::string Database::GetInfo() const {
  std::ostringstream os;

  os << "=====================================================\n";
  os << "DATABASE(" << GetOid() << ") : \n";

  oid_t table_count = GetTableCount();
  os << "Table Count : " << table_count << "\n";

  oid_t table_itr = 0;
  for (auto table : tables) {
    if (table != nullptr) {
      os << "(" << ++table_itr << "/" << table_count << ") "
         << "Table Name(" << table->GetOid() << ") : " << table->GetName() << std::endl;

      if (table->HasForeignKeys()) {
        os << "foreign tables \n";

        }
      }
    }

  os << "=====================================================\n";

  return os.str();
}

std::string Database::GetDBName() { return database_name; }

void Database::setDBName(const std::string &database_name) {
  Database::database_name = database_name;
}

}  // End storage namespace
}  // End peloton namespace
