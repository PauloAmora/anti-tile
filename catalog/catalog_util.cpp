//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_util.cpp
//
// Identification: src/catalog/catalog_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog_util.h"

namespace peloton {

namespace catalog {
/**
 * Inserts a tuple in a table
 */
void InsertTuple(storage::DataTable *table,
                 std::unique_ptr<storage::Tuple> tuple);

void DeleteTuple(storage::DataTable *table, oid_t id);
/**
 * Generate a database catalog tuple
 * Input: The table schema, the database id, the database name
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetDatabaseCatalogTuple(
    const catalog::Schema *schema, oid_t database_id, std::string database_name,
    type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetVarcharValue(database_name, nullptr);
  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  return std::move(tuple);
}

/**
 * Generate a database metric tuple
 * Input: The table schema, the database id, number of txn committed,
 * number of txn aborted, the timestamp
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetDatabaseMetricsCatalogTuple(
    const catalog::Schema *schema, oid_t database_id, int64_t commit,
    int64_t abort, int64_t time_stamp) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetIntegerValue(commit);
  auto val3 = type::ValueFactory::GetIntegerValue(abort);
  auto val4 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);
  tuple->SetValue(3, val4, nullptr);
  return std::move(tuple);
}

/**
 * Generate a table metric tuple
 * Input: The table schema, the database id, the table id, number of tuples
 * read, updated, deleted inserted, the timestamp
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetTableMetricsCatalogTuple(
    const catalog::Schema *schema, oid_t database_id, oid_t table_id,
    int64_t reads, int64_t updates, int64_t deletes, int64_t inserts,
    int64_t time_stamp) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetIntegerValue(table_id);
  auto val3 = type::ValueFactory::GetIntegerValue(reads);
  auto val4 = type::ValueFactory::GetIntegerValue(updates);
  auto val5 = type::ValueFactory::GetIntegerValue(deletes);
  auto val6 = type::ValueFactory::GetIntegerValue(inserts);
  auto val7 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);
  tuple->SetValue(3, val4, nullptr);
  tuple->SetValue(4, val5, nullptr);
  tuple->SetValue(5, val6, nullptr);
  tuple->SetValue(6, val7, nullptr);
  return std::move(tuple);
}

/**
 * Generate a index metric tuple
 * Input: The table schema, the database id, the table id, the index id,
 * number of tuples read, deleted inserted, the timestamp
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetIndexMetricsCatalogTuple(
    const catalog::Schema *schema, oid_t database_id, oid_t table_id,
    oid_t index_id, int64_t reads, int64_t deletes, int64_t inserts,
    int64_t time_stamp) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetIntegerValue(table_id);
  auto val3 = type::ValueFactory::GetIntegerValue(index_id);
  auto val4 = type::ValueFactory::GetIntegerValue(reads);
  auto val5 = type::ValueFactory::GetIntegerValue(deletes);
  auto val6 = type::ValueFactory::GetIntegerValue(inserts);
  auto val7 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);
  tuple->SetValue(3, val4, nullptr);
  tuple->SetValue(4, val5, nullptr);
  tuple->SetValue(5, val6, nullptr);
  tuple->SetValue(6, val7, nullptr);
  return std::move(tuple);
}

/**
 * Generate a query metric tuple
 * Input: The table schema, the query string, database id, number of
 * tuples read, updated, deleted inserted, the timestamp
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetQueryMetricsCatalogTuple(
    const catalog::Schema *schema, std::string query_name, oid_t database_id,
    int64_t num_params, stats::QueryMetric::QueryParamBuf type_buf,
    stats::QueryMetric::QueryParamBuf format_buf,
    stats::QueryMetric::QueryParamBuf val_buf, int64_t reads, int64_t updates,
    int64_t deletes, int64_t inserts, int64_t cpu_time,
    int64_t time_stamp, type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));

  auto val1 = type::ValueFactory::GetVarcharValue(query_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(database_id);
  auto val3 = type::ValueFactory::GetIntegerValue(num_params);

  type::Value param_type =
      type::ValueFactory::GetNullValueByType(type::Type::VARBINARY);
  type::Value param_format =
      type::ValueFactory::GetNullValueByType(type::Type::VARBINARY);
  type::Value param_value =
      type::ValueFactory::GetNullValueByType(type::Type::VARBINARY);

  if (num_params != 0) {
    param_type =
        type::ValueFactory::GetVarbinaryValue(type_buf.buf, type_buf.len, false);
    param_format =
        type::ValueFactory::GetVarbinaryValue(format_buf.buf, format_buf.len, false);
    param_value =
        type::ValueFactory::GetVarbinaryValue(val_buf.buf, val_buf.len, false);
  }

  auto val7 = type::ValueFactory::GetIntegerValue(reads);
  auto val8 = type::ValueFactory::GetIntegerValue(updates);
  auto val9 = type::ValueFactory::GetIntegerValue(deletes);
  auto val10 = type::ValueFactory::GetIntegerValue(inserts);
  auto val12 = type::ValueFactory::GetIntegerValue(cpu_time);
  auto val13 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);

  tuple->SetValue(3, param_type, pool);
  tuple->SetValue(4, param_format, pool);
  tuple->SetValue(5, param_value, pool);

  tuple->SetValue(6, val7, nullptr);
  tuple->SetValue(7, val8, nullptr);
  tuple->SetValue(8, val9, nullptr);
  tuple->SetValue(9, val10, nullptr);
  tuple->SetValue(11, val12, nullptr);
  tuple->SetValue(12, val13, nullptr);

  return std::move(tuple);
}

/**
 * Generate a table catalog tuple
 * Input: The table schema, the table id, the table name, the database id, and
 * the database name
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetTableCatalogTuple(
    const catalog::Schema *schema, oid_t table_id, std::string table_name,
    oid_t database_id, std::string database_name, type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(table_id);
  auto val2 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(database_id);
  auto val4 = type::ValueFactory::GetVarcharValue(database_name, nullptr);
  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);
  return std::move(tuple);
}
}
}
