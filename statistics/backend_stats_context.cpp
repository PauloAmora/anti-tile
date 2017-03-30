//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_stats_context.cpp
//
// Identification: src/statistics/backend_stats_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <map>

#include "type/types.h"
#include "common/statement.h"
#include "catalog/catalog.h"
#include "statistics/backend_stats_context.h"
#include "statistics/stats_aggregator.h"
#include "statistics/counter_metric.h"
#include "storage/database.h"
#include "storage/tile_group.h"

namespace peloton {
namespace stats {

CuckooMap<std::thread::id, std::shared_ptr<BackendStatsContext>> &
  BackendStatsContext::GetBackendContextMap() {
  static CuckooMap<std::thread::id, std::shared_ptr<BackendStatsContext>>
      stats_context_map;
  return stats_context_map;
}

BackendStatsContext* BackendStatsContext::GetInstance() {

  // Each thread gets a backend stats context
  std::thread::id this_id = std::this_thread::get_id();
  std::shared_ptr<BackendStatsContext> result(nullptr);
  auto &stats_context_map = GetBackendContextMap();
  if (stats_context_map.Find(this_id, result) == false) {
    result.reset(new BackendStatsContext(true));
    stats_context_map.Insert(this_id, result);
  }
  return result.get();
}

BackendStatsContext::BackendStatsContext(bool regiser_to_aggregator)
     {
  std::thread::id this_id = std::this_thread::get_id();
  thread_id_ = this_id;

  is_registered_to_aggregator_ = regiser_to_aggregator;

  // Register to the global aggregator
  if (regiser_to_aggregator == true)
    StatsAggregator::GetInstance().RegisterContext(thread_id_, this);
}

BackendStatsContext::~BackendStatsContext() {}

//===--------------------------------------------------------------------===//
// ACCESSORS
//===--------------------------------------------------------------------===//

// Returns the table metric with the given database ID and table ID
TableMetric* BackendStatsContext::GetTableMetric(oid_t database_id,
                                                 oid_t table_id) {
  if (table_metrics_.find(table_id) == table_metrics_.end()) {
    table_metrics_[table_id] = std::unique_ptr<TableMetric>(
        new TableMetric{TABLE_METRIC, database_id, table_id});
  }
  return table_metrics_[table_id].get();
}

// Returns the database metric with the given database ID
DatabaseMetric* BackendStatsContext::GetDatabaseMetric(oid_t database_id) {
  if (database_metrics_.find(database_id) == database_metrics_.end()) {
    database_metrics_[database_id] = std::unique_ptr<DatabaseMetric>(
        new DatabaseMetric{DATABASE_METRIC, database_id});
  }
  return database_metrics_[database_id].get();
}

void BackendStatsContext::IncrementTableInserts(oid_t tile_group_id) {
  oid_t table_id =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetTableId();
  oid_t database_id = catalog::Manager::GetInstance()
                          .GetTileGroup(tile_group_id)
                          ->GetDatabaseId();
  auto table_metric = GetTableMetric(database_id, table_id);
  PL_ASSERT(table_metric != nullptr);
  table_metric->GetTableAccess().IncrementInserts();
  if (ongoing_query_metric_ != nullptr) {
    ongoing_query_metric_->GetQueryAccess().IncrementInserts();
  }
}

void BackendStatsContext::IncrementTableUpdates(oid_t tile_group_id) {
  oid_t table_id =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetTableId();
  oid_t database_id = catalog::Manager::GetInstance()
                          .GetTileGroup(tile_group_id)
                          ->GetDatabaseId();
  auto table_metric = GetTableMetric(database_id, table_id);
  PL_ASSERT(table_metric != nullptr);
  table_metric->GetTableAccess().IncrementUpdates();
  if (ongoing_query_metric_ != nullptr) {
    ongoing_query_metric_->GetQueryAccess().IncrementUpdates();
  }
}

void BackendStatsContext::IncrementTableDeletes(oid_t tile_group_id) {
  oid_t table_id =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetTableId();
  oid_t database_id = catalog::Manager::GetInstance()
                          .GetTileGroup(tile_group_id)
                          ->GetDatabaseId();
  auto table_metric = GetTableMetric(database_id, table_id);
  PL_ASSERT(table_metric != nullptr);
  table_metric->GetTableAccess().IncrementDeletes();
  if (ongoing_query_metric_ != nullptr) {
    ongoing_query_metric_->GetQueryAccess().IncrementDeletes();
  }
}


void BackendStatsContext::InitQueryMetric(
    const std::shared_ptr<Statement> statement,
    const std::shared_ptr<QueryMetric::QueryParams> params) {
  // TODO currently all queries belong to DEFAULT_DB
  ongoing_query_metric_.reset(new QueryMetric(
      QUERY_METRIC, statement->GetQueryString(), params, DEFAULT_DB_ID));
}

//===--------------------------------------------------------------------===//
// HELPER FUNCTIONS
//===--------------------------------------------------------------------===//

void BackendStatsContext::Aggregate(BackendStatsContext& source) {

  // Aggregate all per-database metrics
  for (auto& database_item : source.database_metrics_) {
    GetDatabaseMetric(database_item.first)->Aggregate(*database_item.second);
  }

  // Aggregate all per-table metrics
  for (auto& table_item : source.table_metrics_) {
    GetTableMetric(table_item.second->GetDatabaseId(),
                   table_item.second->GetTableId())
        ->Aggregate(*table_item.second);
  }


  // Aggregate all per-query metrics
  std::shared_ptr<QueryMetric> query_metric;
  while (source.completed_query_metrics_.Dequeue(query_metric)) {
    completed_query_metrics_.Enqueue(query_metric);
    LOG_TRACE("Found a query metric to aggregate");
    aggregated_query_count_++;
  }
}

void BackendStatsContext::Reset() {

  for (auto& database_item : database_metrics_) {
    database_item.second->Reset();
  }
  for (auto& table_item : table_metrics_) {
    table_item.second->Reset();
  }

  oid_t num_databases = catalog::Catalog::GetInstance()->GetDatabaseCount();
  for (oid_t i = 0; i < num_databases; ++i) {
    auto database = catalog::Catalog::GetInstance()->GetDatabaseWithOffset(i);
    oid_t database_id = database->GetOid();

    // Reset database metrics
    if (database_metrics_.find(database_id) == database_metrics_.end()) {
      database_metrics_[database_id] = std::unique_ptr<DatabaseMetric>(
          new DatabaseMetric{DATABASE_METRIC, database_id});
    }

    // Reset table metrics
    oid_t num_tables = database->GetTableCount();
    for (oid_t j = 0; j < num_tables; ++j) {
      auto table = database->GetTable(j);
      oid_t table_id = table->GetOid();

      if (table_metrics_.find(table_id) == table_metrics_.end()) {
        table_metrics_[table_id] = std::unique_ptr<TableMetric>(
            new TableMetric{TABLE_METRIC, database_id, table_id});
      }


    }
  }
}

std::string BackendStatsContext::ToString() const {
  std::stringstream ss;


  for (auto& database_item : database_metrics_) {
    oid_t database_id = database_item.second->GetDatabaseId();
    ss << database_item.second->GetInfo();

    for (auto& table_item : table_metrics_) {
      if (table_item.second->GetDatabaseId() == database_id) {
        ss << table_item.second->GetInfo();
      if (!table_metrics_.empty()) {
        ss << std::endl;
      }
    }
    if (!database_metrics_.empty()) {
      ss << std::endl;
    }
  }

  return ss.str();
}
}

void BackendStatsContext::CompleteQueryMetric() {
  if (ongoing_query_metric_ != nullptr) {
    ongoing_query_metric_->GetProcessorMetric().RecordTime();
    completed_query_metrics_.Enqueue(ongoing_query_metric_);
    ongoing_query_metric_.reset();
    LOG_TRACE("Ongoing query completed");
  }
}

}  // namespace stats
}  // namespace peloton
