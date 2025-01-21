#include "src/include/pfm.h"

int main(){
    PeterDB::PagedFileManager* pfm = &PeterDB::PagedFileManager::instance();
    PeterDB::FileHandle fileHandle1;
    std::string fileName = "testdata";

    if(pfm->openFile(fileName, fileHandle1) != 0){
        std::cout << "Error: file open failed" << std::endl;
        return 0;
    }

    unsigned rc = 0;
    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;

    fileHandle1.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    std::cout << "Before AppendPage - R:" << readPageCount << " W:" << writePageCount << " A:" << appendPageCount << std::endl;

    rc = fileHandle1.appendPage(data);
    fileHandle1.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    std::cout << "After AppendPage R:" << readPageCount << " W:" << writePageCount << " A:" << appendPageCount << std::endl;
}