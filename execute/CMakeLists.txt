project(BUFFER_POOL_MANAGER)

# set(CMAKE_CXX_FLAGS "-std=c++17 -Wall -Wextra -v -O3 -mavx -fopenmp -lboost_serialization")
set(CMAKE_CXX_FLAGS "-std=c++17 -Wall -Wextra -v -O3 -msse -fopenmp -lboost_serialization")
add_definitions(-DPMODE=2)
add_definitions(-DDYNAMIC_INSERTION=0)

# add boost
set(Boost_INCLUDE_DIR /mnt/scratch/yuxsun/boost/include)
set(Boost_LIBRARY_DIR /mnt/scratch/yuxsun/boost/lib)
find_package(Boost 1.61.0 COMPONENTS system filesystem serialization REQUIRED)

include_directories(${Boost_INCLUDE_DIR})
link_directories(${Boost_LIBRARY_DIR})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../bin")

foreach(_target
    main main_bpm)
    add_executable(${_target} "${_target}.cpp")
    target_link_libraries(${_target}
        bpm_src
        ${Boost_LIBRARIES}
    )
endforeach()
