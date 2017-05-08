//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/include/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <atomic>
#include <unordered_map>
#include <list>
#include <utility>

#include "storage/tile_group_header.h"
#include "common/logger.h"

namespace peloton {

class ItemPointer;

namespace storage {
class DataTable;
class TileGroupHeader;
}

namespace catalog {
class Manager;
}

namespace concurrency {

class Transaction;

class TransactionManager {
 public:
  TransactionManager() {
    next_cid_ = ATOMIC_VAR_INIT(START_CID);
    maximum_grant_cid_ = ATOMIC_VAR_INIT(MAX_CID);
  }

  virtual ~TransactionManager() {}


  // The index_entry_ptr is the address of the head node of the version chain, 
  // which is directly pointed by the primary index.
  virtual void PerformInsert(
                             const ItemPointer &location, 
                             ItemPointer *index_entry_ptr = nullptr) = 0;

  virtual bool PerformRead(
                           const ItemPointer &location,
                           bool acquire_ownership = false) = 0;

  virtual void PerformUpdate(
                             const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformDelete(
                             const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformUpdate(const ItemPointer &location) = 0;

  virtual void PerformDelete(const ItemPointer &location) = 0;

  void ResetStates() {
    next_cid_ = START_CID;
  }


 private:
  std::atomic<cid_t> next_cid_;
  std::atomic<cid_t> maximum_grant_cid_;

};
}  // End storage namespace
}  // End peloton namespace
