cmake_minimum_required(VERSION 3.23)
if(POLICY CMP0144) # https://cmake.org/cmake/help/git-stage/policy/CMP0144.html
  cmake_policy(SET CMP0144 NEW)
endif()

project(LibComm
  VERSION 0.2.0
  DESCRIPTION "Header-only MPI communication helpers used by LibRI and ABACUS"
  LANGUAGES CXX)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)
include(CTest)

list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(LIBCOMM_INSTALL_EXAMPLE_HEADERS "Install example helper headers under include/Comm/example." ON)
option(BUILD_TESTING "Enable unittest: check public headers for self-contained compilation." OFF)

find_package(Threads REQUIRED)
find_package(MPI REQUIRED COMPONENTS CXX)
find_package(OpenMP REQUIRED COMPONENTS CXX)
find_package(cereal REQUIRED)

file(GLOB_RECURSE LIBCOMM_PUBLIC_HEADERS CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/include/Comm/*.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/Comm/*.hpp")

if(NOT LIBCOMM_INSTALL_EXAMPLE_HEADERS)
  list(FILTER LIBCOMM_PUBLIC_HEADERS EXCLUDE REGEX "/include/Comm/example/")
endif()

add_library(LibComm INTERFACE)

set_target_properties(LibComm PROPERTIES
  EXPORT_NAME LibComm)

target_sources(LibComm
  INTERFACE
    FILE_SET public_headers
    TYPE HEADERS
    BASE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include"
    FILES ${LIBCOMM_PUBLIC_HEADERS})

target_compile_features(LibComm INTERFACE cxx_std_17)

target_include_directories(LibComm
  INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")

target_link_libraries(LibComm
  INTERFACE
    MPI::MPI_CXX
    OpenMP::OpenMP_CXX
    Threads::Threads
    cereal::cereal)

install(TARGETS LibComm
  EXPORT LibCommTargets
  FILE_SET public_headers DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

install(EXPORT LibCommTargets
  NAMESPACE LibComm::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/LibComm")

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/LibCommConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/LibCommConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/LibComm")

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/LibCommConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/LibCommConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/LibCommConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/LibComm")

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/libcomm.pc.in"
  "${CMAKE_CURRENT_BINARY_DIR}/libcomm.pc"
  @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/libcomm.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE"
  DESTINATION "${CMAKE_INSTALL_DOCDIR}")

if(BUILD_TESTING)
  add_library(LibComm::LibComm ALIAS LibComm)
  set(_libcomm_header_check_index 0)
  foreach(_libcomm_header IN LISTS LIBCOMM_PUBLIC_HEADERS)
    math(EXPR _libcomm_header_check_index "${_libcomm_header_check_index} + 1")
    file(RELATIVE_PATH _libcomm_header_rel
      "${CMAKE_CURRENT_SOURCE_DIR}/include"
      "${_libcomm_header}")
    set(_libcomm_header_check_target
      "libcomm_public_header_check_${_libcomm_header_check_index}")
    set(_libcomm_header_check_source
      "${CMAKE_CURRENT_BINARY_DIR}/${_libcomm_header_check_target}.cpp")
    file(WRITE "${_libcomm_header_check_source}"
      "#include <${_libcomm_header_rel}>\n"
      "int main() { return 0; }\n")
    add_executable(${_libcomm_header_check_target}
      "${_libcomm_header_check_source}")
    target_link_libraries(${_libcomm_header_check_target}
      PRIVATE LibComm::LibComm)
    add_test(
      NAME "${_libcomm_header_check_target}"
      COMMAND ${_libcomm_header_check_target})
  endforeach()
endif()

set(CPACK_PACKAGE_NAME "LibComm")
set(CPACK_PACKAGE_VENDOR "ABACUS")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Header-only MPI communication helpers")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_GENERATOR "TGZ")
include(CPack)
