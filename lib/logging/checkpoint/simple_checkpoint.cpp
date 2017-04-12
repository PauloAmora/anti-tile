//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_checkpoint.cpp
//
// Identification: src/logging/checkpoint/simple_checkpoint.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <cstdio>
#include <numeric>

#include "logging/checkpoint/simple_checkpoint.h"
#include "logging/loggers/wal_frontend_logger.h"
#include "logging/records/tuple_record.h"
#include "logging/records/transaction_record.h"
#include "logging/log_record.h"
#include "logging/checkpoint_tile_scanner.h"
#include "logging/logging_util.h"

#include "catalog/manager.h"
#include "catalog/catalog.h"

#include "common/logger.h"
#include "type/types.h"

#include "logging/log_record.h"
#include "logging/log_manager.h"
#include "logging/checkpoint_manager.h"
#include "storage/database.h"

namespace peloton {
namespace logging {
//===--------------------------------------------------------------------===//
// Simple Checkpoint
//===--------------------------------------------------------------------===//

SimpleCheckpoint::SimpleCheckpoint(bool disable_file_access)
    : Checkpoint(disable_file_access), logger_(nullptr) {
  InitDirectory();
  InitVersionNumber();
}

SimpleCheckpoint::~SimpleCheckpoint() {
  for (auto &record : records_) {
    record.reset();
  }
  records_.clear();
}

void SimpleCheckpoint::DoCheckpoint() {

}

cid_t SimpleCheckpoint::DoRecovery() {

}

void SimpleCheckpoint::InsertTuple(cid_t commit_id) {
  TupleRecord tuple_record(LOGRECORD_TYPE_WAL_TUPLE_INSERT);

  // Check for torn log write
  if (LoggingUtil::ReadTupleRecordHeader(tuple_record, file_handle_) == false) {
    LOG_ERROR("Could not read tuple record header.");
    return;
  }

  auto table = LoggingUtil::GetTable(tuple_record);
  if (!table) {
    // the table was deleted
    LoggingUtil::SkipTupleRecordBody(file_handle_);
    return;
  }

  // Read off the tuple record body from the log
  std::unique_ptr<storage::Tuple> tuple(LoggingUtil::ReadTupleRecordBody(
      table->GetSchema(), pool.get(), file_handle_));
  // Check for torn log write
  if (tuple == nullptr) {
    LOG_ERROR("Torn checkpoint write.");
    return;
  }
  auto target_location = tuple_record.GetInsertLocation();
  auto tile_group_id = target_location.block;
  RecoverTuple(tuple.get(), table, target_location, commit_id);
  if (max_oid_ < target_location.block) {
    max_oid_ = tile_group_id;
  }
  LOG_TRACE("Inserted a tuple from checkpoint: (%u, %u)", target_location.block,
            target_location.offset);
}

void SimpleCheckpoint::Scan(storage::DataTable *target_table,
                            oid_t database_oid) {

}

void SimpleCheckpoint::SetLogger(BackendLogger *logger) {
  logger_.reset(logger);
}

std::vector<std::shared_ptr<LogRecord>> SimpleCheckpoint::GetRecords() {
  return records_;
}

// Private Functions
void SimpleCheckpoint::CreateFile() {
  if (disable_file_access) return;
  // open checkpoint file and file descriptor
  std::string file_name = ConcatFileName(checkpoint_dir, ++checkpoint_version);
  bool success =
      LoggingUtil::InitFileHandle(file_name.c_str(), file_handle_, "ab");
  if (!success) {
    PL_ASSERT(false);
    return;
  }
  LOG_TRACE("Created a new checkpoint file: %s", file_name.c_str());
}

// Only called when checkpoint has actual contents
void SimpleCheckpoint::Persist() {
  if (disable_file_access) return;
  PL_ASSERT(file_handle_.file);
  PL_ASSERT(file_handle_.fd != INVALID_FILE_DESCRIPTOR);

  LOG_TRACE("Persisting %lu checkpoint entries", records_.size());
  // write all the record in the queue and free them
  for (auto record : records_) {
    PL_ASSERT(record);
    PL_ASSERT(record->GetMessageLength() > 0);
    fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
           file_handle_.file);
    record.reset();
  }
  records_.clear();
}

void SimpleCheckpoint::Cleanup() {
  // Clean up the record queue
  for (auto record : records_) {
    record.reset();
  }
  records_.clear();

  if (!disable_file_access) {
    // Close and sync the current one
    fclose(file_handle_.file);

    // Remove previous version
    if (checkpoint_version > 0 && !disable_file_access) {
      auto previous_version =
          ConcatFileName(checkpoint_dir, checkpoint_version - 1).c_str();
      if (remove(previous_version) != 0) {
        LOG_TRACE("Failed to remove file %s", previous_version);
      }
    }
  }
  // Truncate logs
  LogManager::GetInstance().TruncateLogs(start_commit_id_);
}

void SimpleCheckpoint::InitVersionNumber() {
  // Get checkpoint version
  LOG_TRACE("Trying to read checkpoint directory");
  struct dirent *file;
  auto dirp = opendir(checkpoint_dir.c_str());
  if (dirp == nullptr) {
    LOG_TRACE("Opendir failed: Errno: %d, error: %s", errno, strerror(errno));
    return;
  }

  while ((file = readdir(dirp)) != NULL) {
    if (strncmp(file->d_name, FILE_PREFIX.c_str(), FILE_PREFIX.length()) == 0) {
      // found a checkpoint file!
      LOG_TRACE("Found a checkpoint file with name %s", file->d_name);
      int version = LoggingUtil::ExtractNumberFromFileName(file->d_name);
      if (version > checkpoint_version) {
        checkpoint_version = version;
      }
    }
  }
  closedir(dirp);
  LOG_TRACE("set checkpoint version to: %d", checkpoint_version);
}

}  // namespace logging
}  // namespace peloton
