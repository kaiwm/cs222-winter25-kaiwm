#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        struct stat buffer;
        if (stat(fileName.c_str(), &buffer) == 0) {
            return -1;  // File already exists
        }

        FILE *file = fopen(fileName.c_str(), "wb");
        if (file == nullptr) {
            return -1;
        }

        // Initialize metadata page
        MetadataPage metadata = {0, 0, 0, 0};
        char page[PAGE_SIZE] = {0};  // Initialize full page to zeros
        memcpy(page, &metadata, sizeof(MetadataPage));

        if (fwrite(page, 1, PAGE_SIZE, file) != PAGE_SIZE) {
            fclose(file);
            return -1;
        }

        fclose(file);
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if (remove(fileName.c_str()) != 0) {
            return -1;
        }
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (fileHandle.file != nullptr) {
            return -1;
        }

        FILE *file = fopen(fileName.c_str(), "rb+");
        if (file == nullptr) {
            return -1;
        }

        fileHandle.file = file;

        // Read metadata page
        char page[PAGE_SIZE];
        if (fread(page, 1, PAGE_SIZE, file) != PAGE_SIZE) {
            fclose(file);
            fileHandle.file = nullptr;
            return -1;
        }

        MetadataPage metadata;
        memcpy(&metadata, page, sizeof(MetadataPage));

        // Initialize counters from metadata
        fileHandle.readPageCounter = metadata.readPageCounter;
        fileHandle.writePageCounter = metadata.writePageCounter;
        fileHandle.appendPageCounter = metadata.appendPageCounter;

        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.file == nullptr) {
            return -1;
        }

        // Update metadata page before closing
        if (fileHandle.updateMetadata() != 0) {
            return -1;
        }

        if (fclose(fileHandle.file) != 0) {
            return -1;
        }

        fileHandle.file = nullptr;
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        file = nullptr;
    }

    FileHandle::~FileHandle() {
        if (file != nullptr) {
            updateMetadata();  // Try to update metadata before closing
            fclose(file);
            file = nullptr;
        }
    }

    RC FileHandle::updateMetadata() {
        if (file == nullptr) {
            return -1;
        }

        // Seek to beginning of file
        if (fseek(file, 0, SEEK_SET) != 0) {
            return -1;
        }

        // Prepare metadata page
        char page[PAGE_SIZE] = {0};
        MetadataPage metadata = {
                getNumberOfPages(),
                readPageCounter,
                writePageCounter,
                appendPageCounter
        };
        memcpy(page, &metadata, sizeof(MetadataPage));

        // Write metadata page
        if (fwrite(page, 1, PAGE_SIZE, file) != PAGE_SIZE) {
            return -1;
        }

        fflush(file);
        return 0;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (file == nullptr) {
            return -1;
        }

        // Add 1 to pageNum to account for metadata page
        unsigned actualPageNum = pageNum + 1;
        unsigned totalPages = getNumberOfPages()+1;

        if (actualPageNum >= totalPages) {
            return -1;
        }

        if (fseek(file, actualPageNum * PAGE_SIZE, SEEK_SET) != 0) {
            return -1;
        }

        if (fread(data, 1, PAGE_SIZE, file) != PAGE_SIZE) {
            return -1;
        }

        readPageCounter++;
        updateMetadata();  // Update metadata after successful read
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (file == nullptr) {
            return -1;
        }

        // Add 1 to pageNum to account for metadata page
        unsigned actualPageNum = pageNum + 1;
        unsigned totalPages = getNumberOfPages() + 1;

        if (actualPageNum >= totalPages) {
            return -1;
        }

        if (fseek(file, actualPageNum * PAGE_SIZE, SEEK_SET) != 0) {
            return -1;
        }

        if (fwrite(data, 1, PAGE_SIZE, file) != PAGE_SIZE) {
            return -1;
        }

        writePageCounter++;
        fflush(file);
        updateMetadata();  // Update metadata after successful write
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        if (file == nullptr) {
            return -1;
        }

        if (fseek(file, 0, SEEK_END) != 0) {
            return -1;
        }

        if (fwrite(data, 1, PAGE_SIZE, file) != PAGE_SIZE) {
            return -1;
        }

        appendPageCounter++;
        fflush(file);
        updateMetadata();  // Update metadata after successful append
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        if (file == nullptr) {
            return 0;
        }

        long currentPos = ftell(file);
        if (currentPos == -1) {
            return 0;
        }

        if (fseek(file, 0, SEEK_END) != 0) {
            return 0;
        }

        long fileSize = ftell(file);
        if (fileSize == -1) {
            return 0;
        }

        // Restore original position
        if (fseek(file, currentPos, SEEK_SET) != 0) {
            return 0;
        }

        // Include metadata page in total count
        return static_cast<unsigned>(fileSize / PAGE_SIZE) - 1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

} // namespace PeterDB