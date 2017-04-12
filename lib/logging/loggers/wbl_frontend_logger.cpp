//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wbl_frontend_logger.cpp
//
// Identification: src/logging/loggers/wbl_frontend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <sys/mman.h>

#include "common/exception.h"
#include "catalog/manager.h"
#include "catalog/schema.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "logging/loggers/wbl_frontend_logger.h"
#include "logging/loggers/wbl_backend_logger.h"
#include "logging/logging_util.h"
#include "logging/log_manager.h"

#define POSSIBLY_DIRTY_GRANT_SIZE 10000000;  // ten million seems reasonable

namespace peloton {
namespace logging {

struct WriteBehindLogRecord {
  cid_t persistent_commit_id;
  cid_t max_possible_dirty_commit_id;
};

// TODO for now, these helper routines are defined here, and also use
// some routines from the LoggingUtil class. Make sure that all places where
// these helper routines are called in this file use the LoggingUtil class
size_t GetLogFileSize(int fd) {
  struct stat log_stats;
  fstat(fd, &log_stats);
  return log_stats.st_size;
}

/**
 * @brief create NVM backed log pool
 */
WriteBehindFrontendLogger::WriteBehindFrontendLogger() {
  logging_type = LoggingType::NVM_WBL;

  // open log file and file descriptor
  // we open it in append + binary mode
  log_file = fopen(GetLogFileName().c_str(), "ab+");
  if (log_file == NULL) {
    LOG_ERROR("LogFile is NULL");
  }

  // also, get the descriptor
  log_file_fd = fileno(log_file);
  if (log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }
}

/**
 * @brief clean NVM space
 */
WriteBehindFrontendLogger::~WriteBehindFrontendLogger() {
  // Clean up the frontend logger's queue
  global_queue.clear();
}

//===--------------------------------------------------------------------===//
// Active Processing
//===--------------------------------------------------------------------===//

/**
 * @brief flush all log records to the file
 */
void WriteBehindFrontendLogger::FlushLogRecords(void) {

}

//===--------------------------------------------------------------------===//
// Recovery
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void WriteBehindFrontendLogger::DoRecovery() {

}

std::string WriteBehindFrontendLogger::GetLogFileName(void) {
  auto &log_manager = logging::LogManager::GetInstance();
  return log_manager.GetLogFileName();
}

void WriteBehindFrontendLogger::SetLoggerID(UNUSED_ATTRIBUTE int id) {
  // do nothing
}

}  // namespace logging
}  // namespace peloton
