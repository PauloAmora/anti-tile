//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_ordering_transaction_manager.cpp
//
// Identification: src/concurrency/timestamp_ordering_transaction_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/timestamp_ordering_transaction_manager.h"

#include "catalog/manager.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/platform.h"
#include "logging/log_manager.h"
#include "logging/records/transaction_record.h"

namespace peloton {
namespace concurrency {

TimestampOrderingTransactionManager &
TimestampOrderingTransactionManager::GetInstance() {
  static TimestampOrderingTransactionManager txn_manager;
  return txn_manager;
}


// Initiate reserved area of a tuple
void TimestampOrderingTransactionManager::InitTupleReserved(
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t tuple_id) {
  auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

  new ((reserved_area + LOCK_OFFSET)) Spinlock();
  *(cid_t *)(reserved_area + LAST_READER_OFFSET) = 0;
}

bool TimestampOrderingTransactionManager::PerformRead(
    const ItemPointer &location,
    bool acquire_ownership) {


 /* oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  LOG_TRACE("PerformRead (%u, %u)\n", location.block, location.offset);
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();

  // Check if it's select for update before we check the ownership and modify
  // the
  // last reader tid
  if (acquire_ownership == true &&
      IsOwner(current_txn, tile_group_header, tuple_id) == false) {
    // Acquire ownership if we haven't
    if (IsOwnable(current_txn, tile_group_header, tuple_id) == false) {
      // Can not own
      return false;
    }
    if (AcquireOwnership(current_txn, tile_group_header, tuple_id) == false) {
      // Can not acquire ownership
      return false;
    }
    // Promote to RWType::READ_OWN
    current_txn->RecordReadOwn(location);
  }

  // if the current transaction has already owned this tuple, then perform read
  // directly.
  if (IsOwner(current_txn, tile_group_header, tuple_id) == true) {
    PL_ASSERT(GetLastReaderCommitId(tile_group_header, tuple_id) <=
              current_txn->GetBeginCommitId());
    // Increment table read op stats
    if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
      stats::BackendStatsContext::GetInstance()->IncrementTableReads(
          location.block);
    }
    return true;
  }
  // if the current transaction does not own this tuple, then attemp to set last
  // reader cid.
  if (SetLastReaderCommitId(tile_group_header, tuple_id,
                            current_txn->GetBeginCommitId()) == true) {
    current_txn->RecordRead(location);
    // Increment table read op stats
    if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
      stats::BackendStatsContext::GetInstance()->IncrementTableReads(
          location.block);
    }
    return true;
  } else {
    // if the tuple has been owned by some concurrent transactions, then read
    // fails.
    LOG_TRACE("Transaction read failed");*/
    return false;

}

void TimestampOrderingTransactionManager::PerformInsert(
     const ItemPointer &location,
    ItemPointer *index_entry_ptr) {
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();


  //tile_group_header->SetTransactionId(tuple_id, transaction_id);

  // no need to set next item pointer.

  // Add the new tuple into the insert set
  //current_txn->RecordInsert(location);

  InitTupleReserved(tile_group_header, tuple_id);

  // Write down the head pointer's address in tile group header
  tile_group_header->SetIndirection(tuple_id, index_entry_ptr);

  // Increment table insert op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableInserts(
        location.block);
  }
}

void TimestampOrderingTransactionManager::PerformUpdate(
    const ItemPointer &old_location,
    const ItemPointer &new_location) {/*
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  LOG_TRACE("Performing Write old tuple %u %u", old_location.block,
            old_location.offset);
  LOG_TRACE("Performing Write new tuple %u %u", new_location.block,
            new_location.offset);

  auto tile_group_header = catalog::Manager::GetInstance()
                               .GetTileGroup(old_location.block)
                               ->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();
  // if we can perform update, then we must have already locked the older
  // version.
  PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
            transaction_id);
  PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
            INVALID_TXN_ID);
  PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
            MAX_CID);
  PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
            MAX_CID);

  // if the executor doesn't call PerformUpdate after AcquireOwnership,
  // no one will possibly release the write lock acquired by this txn.
  // Set double linked list
  // old_prev is the version next (newer) to the old version.

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_prev);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

  if (old_prev.IsNull() == false) {
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                          .GetTileGroup(old_prev.block)
                                          ->GetHeader();

    // once everything is set, we can allow traversing the new version.
    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }

  InitTupleReserved(new_tile_group_header, new_location.offset);

  // if the transaction is not updating the latest version,
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are updating the latest version.
    // Set the header information for the new version
    ItemPointer *index_entry_ptr =
        tile_group_header->GetIndirection(old_location.offset);

    if (index_entry_ptr != nullptr) {

      new_tile_group_header->SetIndirection(new_location.offset,
                                            index_entry_ptr);

      // Set the index header in an atomic way.
      // We do it atomically because we don't want any one to see a half-done
      // pointer.
      // In case of contention, no one can update this pointer when we are
      // updating it
      // because we are holding the write lock. This update should success in
      // its first trial.
      UNUSED_ATTRIBUTE auto res =
          AtomicUpdateItemPointer(index_entry_ptr, new_location);
      PL_ASSERT(res == true);
    }
  }

  // Add the old tuple into the update set
  current_txn->RecordUpdate(old_location);

  // Increment table update op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
        new_location.block);
  }*/

    return false;
}

void TimestampOrderingTransactionManager::PerformUpdate(
     const ItemPointer &location) {
    /*
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
            current_txn->GetTransactionId());
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  PL_ASSERT(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetNextItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // update an inserted version
    current_txn->RecordUpdate(old_location);
  }

  // Increment table update op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
        location.block);
  }
}

void TimestampOrderingTransactionManager::PerformDelete(
    Transaction *const current_txn, const ItemPointer &old_location,
    const ItemPointer &new_location) {
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  LOG_TRACE("Performing Delete");

  auto tile_group_header = catalog::Manager::GetInstance()
                               .GetTileGroup(old_location.block)
                               ->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  auto transaction_id = current_txn->GetTransactionId();

  PL_ASSERT(GetLastReaderCommitId(tile_group_header, old_location.offset) <=
            current_txn->GetBeginCommitId());

  PL_ASSERT(tile_group_header->GetTransactionId(old_location.offset) ==
            transaction_id);
  PL_ASSERT(new_tile_group_header->GetTransactionId(new_location.offset) ==
            INVALID_TXN_ID);
  PL_ASSERT(new_tile_group_header->GetBeginCommitId(new_location.offset) ==
            MAX_CID);
  PL_ASSERT(new_tile_group_header->GetEndCommitId(new_location.offset) ==
            MAX_CID);

  // Set up double linked list

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  tile_group_header->SetPrevItemPointer(old_location.offset, new_location);

  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_prev);

  new_tile_group_header->SetNextItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);

  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  // we should guarantee that the newer version is all set before linking the
  // newer version to older version.
  COMPILER_MEMORY_FENCE;

  if (old_prev.IsNull() == false) {
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                          .GetTileGroup(old_prev.block)
                                          ->GetHeader();

    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }

  InitTupleReserved(new_tile_group_header, new_location.offset);

  // if the transaction is not deleting the latest version,
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are deleting the latest version.
    // Set the header information for the new version
    ItemPointer *index_entry_ptr =
        tile_group_header->GetIndirection(old_location.offset);

    // if there's no primary index on a table, then index_entry_ptry == nullptr.
    if (index_entry_ptr != nullptr) {
      new_tile_group_header->SetIndirection(new_location.offset,
                                            index_entry_ptr);

      // Set the index header in an atomic way.
      // We do it atomically because we don't want any one to see a half-down
      // pointer
      // In case of contention, no one can update this pointer when we are
      // updating it
      // because we are holding the write lock. This update should success in
      // its first trial.
      UNUSED_ATTRIBUTE auto res =
          AtomicUpdateItemPointer(index_entry_ptr, new_location);
      PL_ASSERT(res == true);
    }
  }

  current_txn->RecordDelete(old_location);

  // Increment table delete op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
        old_location.block);
  } */
    return false;
}

void TimestampOrderingTransactionManager::PerformDelete(
    const ItemPointer &location) {/*
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

  PL_ASSERT(tile_group_header->GetTransactionId(tuple_id) ==
            current_txn->GetTransactionId());
  PL_ASSERT(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // Add the old tuple into the delete set
  auto old_location = tile_group_header->GetNextItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // if this version is not newly inserted.
    current_txn->RecordDelete(old_location);
  } else {
    // if this version is newly inserted.
    current_txn->RecordDelete(location);
  }

  // Increment table delete op stats
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
        location.block);
  }*/
}

ResultType TimestampOrderingTransactionManager::CommitTransaction(
    Transaction *const current_txn) {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  if (current_txn->IsDeclaredReadOnly() == true) {
    EndReadonlyTransaction(current_txn);
    return ResultType::SUCCESS;
  }

  auto &manager = catalog::Manager::GetInstance();
  auto &log_manager = logging::LogManager::GetInstance();

  // generate transaction id.
  cid_t end_commit_id = current_txn->GetBeginCommitId();
  log_manager.LogBeginTransaction(end_commit_id);

  auto &rw_set = current_txn->GetReadWriteSet();

  auto gc_set = current_txn->GetGCSetPtr();

  oid_t database_id = 0;
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    if (!rw_set.empty()) {
      database_id =
          manager.GetTileGroup(rw_set.begin()->first)->GetDatabaseId();
    }
  }

  // install everything.
  // 1. install a new version for update operations;
  // 2. install an empty version for delete operations;
  // 3. install a new tuple for insert operations.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RWType::READ_OWN) {
        // A read operation has acquired ownership but hasn't done any further
        // update/delete yet
        // Yield the ownership
        YieldOwnership(current_txn, tile_group_id, tuple_slot);
      } else if (tuple_entry.second == RWType::UPDATE) {
        // we must guarantee that, at any time point, only one version is
        // visible.
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        PL_ASSERT(new_version.IsNull() == false);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        PL_ASSERT(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = false;

        // add to log manager
        log_manager.LogUpdate(
            end_commit_id, ItemPointer(tile_group_id, tuple_slot), new_version);

      } else if (tuple_entry.second == RWType::DELETE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        PL_ASSERT(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        // we need to recycle both old and new versions.
        // we require the GC to delete tuple from index only once.
        // recycle old version, delete from index
        gc_set->operator[](tile_group_id)[tuple_slot] = true;
        // recycle new version (which is an empty version), do not delete from index
        gc_set->operator[](new_version.block)[new_version.offset] = false;

        // add to log manager
        log_manager.LogDelete(end_commit_id,
                              ItemPointer(tile_group_id, tuple_slot));

      } else if (tuple_entry.second == RWType::INSERT) {
        PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
                  current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // nothing to be added to gc set.

        // add to log manager
        log_manager.LogInsert(end_commit_id,
                              ItemPointer(tile_group_id, tuple_slot));

      } else if (tuple_entry.second == RWType::INS_DEL) {
        PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
                  current_txn->GetTransactionId());

        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        // set the begin commit id to persist insert
        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = true;

        // no log is needed for this case
      }
    }
  }

  ResultType result = current_txn->GetResult();

  EndTransaction(current_txn);

  // Increment # txns committed metric
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTxnCommitted(
        database_id);
  }

  return result;
}

ResultType TimestampOrderingTransactionManager::AbortTransaction(
    Transaction *const current_txn) {
  // It's impossible that a pre-declared readonly transaction aborts
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == false);

  LOG_TRACE("Aborting peloton txn : %lu ", current_txn->GetTransactionId());
  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetReadWriteSet();

  auto gc_set = current_txn->GetGCSetPtr();

  oid_t database_id = 0;
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    if (!rw_set.empty()) {
      database_id =
          manager.GetTileGroup(rw_set.begin()->first)->GetDatabaseId();
    }
  }

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RWType::READ_OWN) {
        // A read operation has acquired ownership but hasn't done any further
        // update/delete yet
        // Yield the ownership
        YieldOwnership(current_txn, tile_group_id, tuple_slot);
      } else if (tuple_entry.second == RWType::UPDATE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        // these two fields can be set at any time.
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // as the aborted version has already been placed in the version chain,
        // we need to unlink it by resetting the item pointers.
        auto old_prev =
            new_tile_group_header->GetPrevItemPointer(new_version.offset);

        // check whether the previous version exists.
        if (old_prev.IsNull() == true) {
          PL_ASSERT(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          ItemPointer *index_entry_ptr =
              tile_group_header->GetIndirection(tuple_slot);
          UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
              index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
          PL_ASSERT(res == true);
        }
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        if (old_prev.IsNull() == false) {
          auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                                .GetTileGroup(old_prev.block)
                                                ->GetHeader();
          old_prev_tile_group_header->SetNextItemPointer(
              old_prev.offset, ItemPointer(tile_group_id, tuple_slot));
          tile_group_header->SetPrevItemPointer(tuple_slot, old_prev);
        } else {
          tile_group_header->SetPrevItemPointer(tuple_slot,
                                                INVALID_ITEMPOINTER);
        }

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](new_version.block)[new_version.offset] = false;

      } else if (tuple_entry.second == RWType::DELETE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();

        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // as the aborted version has already been placed in the version chain,
        // we need to unlink it by resetting the item pointers.
        auto old_prev =
            new_tile_group_header->GetPrevItemPointer(new_version.offset);

        // check whether the previous version exists.
        if (old_prev.IsNull() == true) {
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          ItemPointer *index_entry_ptr =
              tile_group_header->GetIndirection(tuple_slot);
          UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
              index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
          PL_ASSERT(res == true);
        }
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);

        if (old_prev.IsNull() == false) {
          auto old_prev_tile_group_header = catalog::Manager::GetInstance()
                                                .GetTileGroup(old_prev.block)
                                                ->GetHeader();
          old_prev_tile_group_header->SetNextItemPointer(
              old_prev.offset, ItemPointer(tile_group_id, tuple_slot));
        }

        tile_group_header->SetPrevItemPointer(tuple_slot, old_prev);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](new_version.block)[new_version.offset] = false;

      } else if (tuple_entry.second == RWType::INSERT) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        // delete from index
        gc_set->operator[](tile_group_id)[tuple_slot] = true;

      } else if (tuple_entry.second == RWType::INS_DEL) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = true;
      }
    }
  }

  current_txn->SetResult(ResultType::ABORTED);
  EndTransaction(current_txn);

  // Increment # txns aborted metric
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementTxnAborted(database_id);
  }

  return ResultType::ABORTED;
}

}  // End storage namespace
}  // End peloton namespace
