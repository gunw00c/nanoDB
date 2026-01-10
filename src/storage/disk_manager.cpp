#include "nanodb/storage/disk_manager.hpp"

namespace nanodb {

void DiskManager::writePage(int /*pageId*/, const void* /*data*/, size_t /*size*/) {
    // Stub: In-memory only for now
    // Future: Write page to disk file
}

void DiskManager::readPage(int /*pageId*/, void* /*data*/, size_t /*size*/) {
    // Stub: In-memory only for now
    // Future: Read page from disk file
}

} // namespace nanodb
