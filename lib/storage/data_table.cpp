//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.cpp
//
// Identification: src/storage/data_table.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <mutex>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/foreign_key.h"
#include "common/exception.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/platform.h"
#include "storage/abstract_table.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"

//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//

std::vector<peloton::oid_t> sdbench_column_ids;

double peloton_projectivity;

int peloton_num_groups;

namespace peloton {
namespace storage {

oid_t DataTable::invalid_tile_group_id = -1;

size_t DataTable::default_active_tilegroup_count_ = 1;
size_t DataTable::default_active_indirection_array_count_ = 1;

DataTable::DataTable(catalog::Schema *schema, const std::string &table_name,
                     const oid_t &database_oid, const oid_t &table_oid,
                     const size_t &tuples_per_tilegroup, const bool own_schema,
                     const bool adapt_table, const bool is_catalog)
    : AbstractTable(table_oid, schema, own_schema),
      database_oid(database_oid),
      table_name(table_name),
      tuples_per_tilegroup_(tuples_per_tilegroup),
      adapt_table_(adapt_table) {
  // Init default partition
  auto col_count = schema->GetColumnCount();
  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    default_partition_[col_itr] = std::make_pair(0, col_itr);
  }

  if (is_catalog == true) {
    active_tilegroup_count_ = 1;
    active_indirection_array_count_ = 1;
  } else {
    active_tilegroup_count_ = default_active_tilegroup_count_;
    active_indirection_array_count_ = default_active_indirection_array_count_;
  }

  active_tile_groups_.resize(active_tilegroup_count_);

  active_indirection_arrays_.resize(active_indirection_array_count_);
  // Create tile groups.
  for (size_t i = 0; i < active_tilegroup_count_; ++i) {
    AddDefaultTileGroup(i);
  }

  // Create indirection layers.
  for (size_t i = 0; i < active_indirection_array_count_; ++i) {
    AddDefaultIndirectionArray(i);
  }
}

DataTable::~DataTable() {
  // clean up tile groups by dropping the references in the catalog
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto tile_groups_size = tile_groups_.GetSize();
  std::size_t tile_groups_itr;

  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
       tile_groups_itr++) {
    auto tile_group_id = tile_groups_.Find(tile_groups_itr);

    if (tile_group_id != invalid_tile_group_id) {
      LOG_TRACE("Dropping tile group : %u ", tile_group_id);
      // drop tile group in catalog
      catalog_manager.DropTileGroup(tile_group_id);
    }
  }


  // drop all indirection arrays
  for (auto indirection_array : active_indirection_arrays_) {
    auto oid = indirection_array->GetOid();
    catalog_manager.DropIndirectionArray(oid);
  }

  // AbstractTable cleans up the schema
}

//===--------------------------------------------------------------------===//
// TUPLE HELPER OPERATIONS
//===--------------------------------------------------------------------===//

bool DataTable::CheckNulls(const storage::Tuple *tuple) const {
  PL_ASSERT(schema->GetColumnCount() == tuple->GetColumnCount());

  oid_t column_count = schema->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    if (tuple->IsNull(column_itr) && schema->AllowNull(column_itr) == false) {
      LOG_TRACE(
          "%u th attribute in the tuple was NULL. It is non-nullable "
          "attribute.",
          column_itr);
      return false;
    }
  }

  return true;
}

bool DataTable::CheckConstraints(const storage::Tuple *tuple) const {
  // First, check NULL constraints
  if (CheckNulls(tuple) == false) {
    LOG_TRACE("Not NULL constraint violated");
    throw ConstraintException("Not NULL constraint violated : " +
                              std::string(tuple->GetInfo()));
    return false;
  }
  return true;
}

// this function is called when update/delete/insert is performed.
// this function first checks whether there's available slot.
// if yes, then directly return the available slot.
// in particular, if this is the last slot, a new tile group is created.
// if there's no available slot, then some other threads must be allocating a
// new tile group.
// we just wait until a new tuple slot in the newly allocated tile group is
// available.
// when updating a tuple, we will invoke this function with the argument set to
// nullptr.
// this is because we want to minimize data copy overhead by performing
// in-place update at executor level.
// however, when performing insert, we have to copy data immediately,
// and the argument cannot be set to nullptr.
ItemPointer DataTable::GetEmptyTupleSlot(const storage::Tuple *tuple) {
 /* //=============== garbage collection==================
  // check if there are recycled tuple slots
  auto free_item_pointer = gc_manager.ReturnFreeSlot(this->table_oid);
  if (free_item_pointer.IsNull() == false) {
    // when inserting a tuple
    if (tuple != nullptr) {
      auto tile_group =
          catalog::Manager::GetInstance().GetTileGroup(free_item_pointer.block);
      tile_group->CopyTuple(tuple, free_item_pointer.offset);
    }
    return free_item_pointer;
  }
  //====================================================
*/
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  std::shared_ptr<storage::TileGroup> tile_group;
  oid_t tuple_slot = INVALID_OID;
  oid_t tile_group_id = INVALID_OID;

  // get valid tuple.
  while (true) {
    // get the last tile group.
    tile_group = active_tile_groups_[active_tile_group_id];

    tuple_slot = tile_group->InsertTuple(tuple);

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      tile_group_id = tile_group->GetTileGroupId();
      break;
    }
  }

  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    AddDefaultTileGroup(active_tile_group_id);
  }

  LOG_TRACE("tile group count: %lu, tile group id: %u, address: %p",
            tile_group_count_.load(), tile_group->GetTileGroupId(),
            tile_group.get());

  // Set tuple location
  ItemPointer location(tile_group_id, tuple_slot);

  return location;
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//
ItemPointer DataTable::InsertEmptyVersion() {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

ItemPointer DataTable::AcquireVersion() {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

bool DataTable::InstallVersion(const AbstractTuple *tuple,
                               const TargetList *targets_ptr,
                               concurrency::Transaction *transaction,
                               ItemPointer *index_entry_ptr) {

  return true;
}

ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple,
                                   concurrency::Transaction *transaction,
                                   ItemPointer **index_entry_ptr) {
  // the upper layer may not pass a index_entry_ptr (default value: nullptr)
  // into the function.
  // in this case, we have to create a temp_ptr to hold the content.
  ItemPointer *temp_ptr = nullptr;

  if (index_entry_ptr == nullptr) {
    index_entry_ptr = &temp_ptr;
  }

  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

    IncreaseTupleCount(1);
    return location;
  }

// insert tuple into a table that is without index.
ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple) {
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
  return location;
}

//===--------------------------------------------------------------------===//
// STATS
//===--------------------------------------------------------------------===//

/**
 * @brief Increase the number of tuples in this table
 * @param amount amount to increase
 */
void DataTable::IncreaseTupleCount(const size_t &amount) {
  number_of_tuples_ += amount;
  dirty_ = true;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void DataTable::DecreaseTupleCount(const size_t &amount) {
  number_of_tuples_ -= amount;
  dirty_ = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void DataTable::SetTupleCount(const size_t &num_tuples) {
  number_of_tuples_ = num_tuples;
  dirty_ = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
size_t DataTable::GetTupleCount() const { return number_of_tuples_; }

/**
 * @brief return dirty flag
 * @return dirty flag
 */
bool DataTable::IsDirty() const { return dirty_; }

/**
 * @brief Reset dirty flag
 */
void DataTable::ResetDirty() { dirty_ = false; }

//===--------------------------------------------------------------------===//
// TILE GROUP
//===--------------------------------------------------------------------===//

TileGroup *DataTable::GetTileGroupWithLayout(
    const column_map_type &partitioning) {
  oid_t tile_group_id = catalog::Manager::GetInstance().GetNextTileGroupId();
  return (AbstractTable::GetTileGroupWithLayout(
      database_oid, tile_group_id, partitioning, tuples_per_tilegroup_));
}

oid_t DataTable::AddDefaultIndirectionArray(
    const size_t &active_indirection_array_id) {
  auto &manager = catalog::Manager::GetInstance();
  oid_t indirection_array_id = manager.GetNextIndirectionArrayId();

  std::shared_ptr<IndirectionArray> indirection_array(
      new IndirectionArray(indirection_array_id));
  manager.AddIndirectionArray(indirection_array_id, indirection_array);

  COMPILER_MEMORY_FENCE;

  active_indirection_arrays_[active_indirection_array_id] = indirection_array;

  return indirection_array_id;
}

oid_t DataTable::AddDefaultTileGroup() {
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  return AddDefaultTileGroup(active_tile_group_id);
}

oid_t DataTable::AddDefaultTileGroup(const size_t &active_tile_group_id) {
  column_map_type column_map;
  oid_t tile_group_id = INVALID_OID;

  // Figure out the partitioning for given tilegroup layout
  column_map = GetTileGroupLayout(LayoutType::LAYOUT_TYPE_ROW);

  // Create a tile group with that partitioning
  std::shared_ptr<TileGroup> tile_group(GetTileGroupWithLayout(column_map));
  PL_ASSERT(tile_group.get());

  tile_group_id = tile_group->GetTileGroupId();

  LOG_TRACE("Added a tile group ");
  tile_groups_.Append(tile_group_id);

  // add tile group metadata in locator
  catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

  COMPILER_MEMORY_FENCE;

  active_tile_groups_[active_tile_group_id] = tile_group;

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);

  return tile_group_id;
}

void DataTable::AddTileGroupWithOidForRecovery(const oid_t &tile_group_id) {
  PL_ASSERT(tile_group_id);

  std::vector<catalog::Schema> schemas;
  schemas.push_back(*schema);

  column_map_type column_map;
  // default column map
  auto col_count = schema->GetColumnCount();
  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    column_map[col_itr] = std::make_pair(0, col_itr);
  }

  std::shared_ptr<TileGroup> tile_group(TileGroupFactory::GetTileGroup(
      database_oid, table_oid, tile_group_id, this, schemas, column_map,
      tuples_per_tilegroup_));

  auto tile_groups_exists = tile_groups_.Contains(tile_group_id);

  if (tile_groups_exists == false) {
    tile_groups_.Append(tile_group_id);

    LOG_TRACE("Added a tile group ");

    // add tile group metadata in locator
    catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

    // we must guarantee that the compiler always add tile group before adding
    // tile_group_count_.
    COMPILER_MEMORY_FENCE;

    tile_group_count_++;

    LOG_TRACE("Recording tile group : %u ", tile_group_id);
  }
}

// NOTE: This function is only used in test cases.
void DataTable::AddTileGroup(const std::shared_ptr<TileGroup> &tile_group) {
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;

  active_tile_groups_[active_tile_group_id] = tile_group;

  oid_t tile_group_id = tile_group->GetTileGroupId();

  tile_groups_.Append(tile_group_id);

  // add tile group in catalog
  catalog::Manager::GetInstance().AddTileGroup(tile_group_id, tile_group);

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);
}

size_t DataTable::GetTileGroupCount() const { return tile_group_count_; }

std::shared_ptr<storage::TileGroup> DataTable::GetTileGroup(
    const std::size_t &tile_group_offset) const {
  PL_ASSERT(tile_group_offset < GetTileGroupCount());

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);

  return GetTileGroupById(tile_group_id);
}

std::shared_ptr<storage::TileGroup> DataTable::GetTileGroupById(
    const oid_t &tile_group_id) const {
  auto &manager = catalog::Manager::GetInstance();
  return manager.GetTileGroup(tile_group_id);
}

void DataTable::DropTileGroups() {
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto tile_groups_size = tile_groups_.GetSize();
  std::size_t tile_groups_itr;

  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
       tile_groups_itr++) {
    auto tile_group_id = tile_groups_.Find(tile_groups_itr);

    if (tile_group_id != invalid_tile_group_id) {
      // drop tile group in catalog
      catalog_manager.DropTileGroup(tile_group_id);
    }
  }

  // Clear array
  tile_groups_.Clear(invalid_tile_group_id);

  tile_group_count_ = 0;
}


// Get the schema for the new transformed tile group
std::vector<catalog::Schema> TransformTileGroupSchema(
    storage::TileGroup *tile_group, const column_map_type &column_map) {
  std::vector<catalog::Schema> new_schema;
  oid_t orig_tile_offset, orig_tile_column_offset;
  oid_t new_tile_offset, new_tile_column_offset;

  // First, get info from the original tile group's schema
  std::map<oid_t, std::map<oid_t, catalog::Column>> schemas;
  auto orig_schemas = tile_group->GetTileSchemas();
  for (auto column_map_entry : column_map) {
    new_tile_offset = column_map_entry.second.first;
    new_tile_column_offset = column_map_entry.second.second;
    oid_t column_offset = column_map_entry.first;

    tile_group->LocateTileAndColumn(column_offset, orig_tile_offset,
                                    orig_tile_column_offset);

    // Get the column info from original schema
    auto orig_schema = orig_schemas[orig_tile_offset];
    auto column_info = orig_schema.GetColumn(orig_tile_column_offset);
    schemas[new_tile_offset][new_tile_column_offset] = column_info;
  }

  // Then, build the new schema
  for (auto schemas_tile_entry : schemas) {
    std::vector<catalog::Column> columns;
    for (auto schemas_column_entry : schemas_tile_entry.second)
      columns.push_back(schemas_column_entry.second);

    catalog::Schema tile_schema(columns);
    new_schema.push_back(tile_schema);
  }

  return new_schema;
}

// Set the transformed tile group column-at-a-time
void SetTransformedTileGroup(storage::TileGroup *orig_tile_group,
                             storage::TileGroup *new_tile_group) {
  // Check the schema of the two tile groups
  auto new_column_map = new_tile_group->GetColumnMap();
  auto orig_column_map = orig_tile_group->GetColumnMap();
  PL_ASSERT(new_column_map.size() == orig_column_map.size());

  oid_t orig_tile_offset, orig_tile_column_offset;
  oid_t new_tile_offset, new_tile_column_offset;

  auto column_count = new_column_map.size();
  auto tuple_count = orig_tile_group->GetAllocatedTupleCount();
  // Go over each column copying onto the new tile group
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    // Locate the original base tile and tile column offset
    orig_tile_group->LocateTileAndColumn(column_itr, orig_tile_offset,
                                         orig_tile_column_offset);

    new_tile_group->LocateTileAndColumn(column_itr, new_tile_offset,
                                        new_tile_column_offset);

    auto orig_tile = orig_tile_group->GetTile(orig_tile_offset);
    auto new_tile = new_tile_group->GetTile(new_tile_offset);

    // Copy the column over to the new tile group
    for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
      type::Value val =
          (orig_tile->GetValue(tuple_itr, orig_tile_column_offset));
      new_tile->SetValue(val, tuple_itr, new_tile_column_offset);
    }
  }

  // Finally, copy over the tile header
  auto header = orig_tile_group->GetHeader();
  auto new_header = new_tile_group->GetHeader();
  *new_header = *header;
}

storage::TileGroup *DataTable::TransformTileGroup(
    const oid_t &tile_group_offset, const double &theta) {
  // First, check if the tile group is in this table
  if (tile_group_offset >= tile_groups_.GetSize()) {
    LOG_ERROR("Tile group offset not found in table : %u ", tile_group_offset);
    return nullptr;
  }

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);

  // Get orig tile group from catalog
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto tile_group = catalog_manager.GetTileGroup(tile_group_id);
  auto diff = tile_group->GetSchemaDifference(default_partition_);

  // Check threshold for transformation
  if (diff < theta) {
    return nullptr;
  }

  LOG_TRACE("Transforming tile group : %u", tile_group_offset);

  // Get the schema for the new transformed tile group
  auto new_schema =
      TransformTileGroupSchema(tile_group.get(), default_partition_);

  // Allocate space for the transformed tile group
  std::shared_ptr<storage::TileGroup> new_tile_group(
      TileGroupFactory::GetTileGroup(
          tile_group->GetDatabaseId(), tile_group->GetTableId(),
          tile_group->GetTileGroupId(), tile_group->GetAbstractTable(),
          new_schema, default_partition_,
          tile_group->GetAllocatedTupleCount()));

  // Set the transformed tile group column-at-a-time
  SetTransformedTileGroup(tile_group.get(), new_tile_group.get());

  // Set the location of the new tile group
  // and clean up the orig tile group
  catalog_manager.AddTileGroup(tile_group_id, new_tile_group);

  return new_tile_group.get();
}

std::map<oid_t, oid_t> DataTable::GetColumnMapStats() {
  std::map<oid_t, oid_t> column_map_stats;

  // Cluster per-tile column count
  for (auto entry : default_partition_) {
    auto tile_id = entry.second.first;
    auto column_map_itr = column_map_stats.find(tile_id);
    if (column_map_itr == column_map_stats.end())
      column_map_stats[tile_id] = 1;
    else
      column_map_stats[tile_id]++;
  }

  return std::move(column_map_stats);
}

void DataTable::SetDefaultLayout(const column_map_type &layout) {
  default_partition_ = layout;
}

column_map_type DataTable::GetDefaultLayout() const{
  return default_partition_;
}


}  // End storage namespace
}  // End peloton namespace
