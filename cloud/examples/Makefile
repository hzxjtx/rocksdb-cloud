include ../../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += "-DROCKSDB_JEMALLOC"
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

.PHONY: clean librocksdb

all: huzx_rocksdb_cloud huzx_rocksdb_cloud_read huzx_example_with_cf
#all: huzx_rocksdb_cloud cloud_durable_example clone_example cloud_dump

huzx_rocksdb_cloud: librocksdb huzx_rocksdb_cloud.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -luuid -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

huzx_example_with_cf: librocksdb huzx_example_with_cf.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -luuid -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

huzx_rocksdb_cloud_read: librocksdb huzx_rocksdb_cloud_read.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -luuid -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

cloud_durable_example: librocksdb cloud_durable_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clone_example: librocksdb clone_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

cloud_dump: librocksdb cloud_dump.cc
	$(CXX) $(CXXFLAGS) $@.cc -o $@ ../../librocksdb.a -I../../include $(OPT) -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./cloud_durable_example  ./clone_example ./cloud_dump

librocksdb:
	cd ../.. && $(MAKE) static_lib
