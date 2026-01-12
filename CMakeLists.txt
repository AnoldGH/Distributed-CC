cmake_minimum_required(VERSION 3.23)
#[[ In-source build check ]]
if("${CMAKE_BINARY_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
    message(FATAL_ERROR "In-source builds are not allowed.")
    message(WARNING "Remove CMakeFiles/ and CMakeCache.txt in the\
            source root before building again.")
else()
    #[[ Set project version ]]
    project(distributed_connectivity_modifier)

    # Set C++ standard
    set(CMAKE_CXX_STANDARD 20)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)

    # Find MPI
    find_package(MPI REQUIRED)

    # Point CMake to the correct igraph/libleidenalg built in the submodule
    list(APPEND CMAKE_PREFIX_PATH
        ${CMAKE_CURRENT_SOURCE_DIR}/external/constrained-clustering/external_libs/lib/cmake/igraph
        ${CMAKE_CURRENT_SOURCE_DIR}/external/constrained-clustering/external_libs/lib/cmake/libleidenalg
        ${CMAKE_CURRENT_SOURCE_DIR}/external/constrained-clustering/external_libs/lib64/cmake/igraph
        ${CMAKE_CURRENT_SOURCE_DIR}/external/constrained-clustering/external_libs/lib64/cmake/libleidenalg
    )

    # Add the constrained-clustering project
    add_subdirectory(external/constrained-clustering)

    # MPI executables
    add_executable(distributed_connectivity_modifier
        src/main.cpp
        src/load_balancer.cpp
        src/worker.cpp
    )

    target_include_directories(distributed_connectivity_modifier PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes)

    # Link
    target_link_libraries(distributed_connectivity_modifier PRIVATE
        MPI::MPI_CXX
        internal_libs   # constrained-clustering
        external_libs   # constrained-clustering external libs
    )

endif("${CMAKE_BINARY_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
