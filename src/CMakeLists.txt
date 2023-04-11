project(BUFFER_POOL_MANAGER)

add_library(bpm_src
            L2Space.cpp
            Query.cpp
            Utils.cpp
            storage-node/StorageIndex.cpp
            storage-node/StorageLists.cpp
            storage-node/StorageNode.cpp
            root-node/RootIndex.cpp
            root-node/RootNode.cpp
            buffer_management/BufferPoolManager.cpp
            buffer_management/ClockReplacer.cpp)

include_directories("/mnt/scratch/yuxsun/boost/include")

target_include_directories(bpm_src PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include)
