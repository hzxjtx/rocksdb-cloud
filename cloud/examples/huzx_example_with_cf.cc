// Copyright (c) 2017-present, Rockset, Inc.  All rights reserved.
#include <cstdio>
#include <iostream>
#include <string>
#include <uuid/uuid.h>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/options.h"

void generateRandomBucket(char buf[]) {
    uuid_t uu;
    struct timeval tv;
    uuid_generate(uu);
    uuid_unparse(uu,buf);
}

using namespace ROCKSDB_NAMESPACE;

// This is the local directory where the db is stored.
std::string kDBPath = "/app/rockset/rocksdb_cloud_cf";

// This is the name of the cloud storage bucket where the db
// is made durable. if you are using AWS, you have to manually
// ensure that this bucket name is unique to you and does not
// conflict with any other S3 users who might have already created
// this bucket name.
std::string kBucketSuffix = "cloud.durable.cf.";
std::string kRegion = "us-west-2";

static const bool flushAtEnd = true;
static const bool disableWAL = false;

int main(int argc, char** argv) {

  /**
   * argv:
   * 0: Execute file name
   * 1: KeyLen
   */
  int dftKeyLen = 3;
/*
  if (argc >= 2) {
    dftKeyLen = atoi(argv[1]);
  }
*/
  printf("Current input key length is:%d\n", dftKeyLen);

  // cloud environment config options here
  CloudEnvOptions cloud_env_options;
  cloud_env_options.endpointOverride = "http://127.0.0.1:9000";
  cloud_env_options.credentials.InitializeSimple("admin", "admin123");

  // Store a reference to a cloud env. A new cloud env object should be
  // associated
  // with every new cloud-db.
  std::unique_ptr<CloudEnv> cloud_env;

  if (!cloud_env_options.credentials.HasValid().ok()) {
    fprintf(
        stderr,
        "Please set env variables "
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return -1;
  }

  // Append the user name to the bucket name in an attempt to make it
  // globally unique. S3 bucket-names need to be globally unique.
  // If you want to rerun this example, then unique user-name suffix here.
  // char* user = getenv("USER");
  char* user = "xxx002";
  kBucketSuffix.append(user);

  // "rockset." is the default bucket prefix
  const std::string bucketPrefix = "rockset.";
  cloud_env_options.src_bucket.SetBucketName(kBucketSuffix, bucketPrefix);
  cloud_env_options.dest_bucket.SetBucketName(kBucketSuffix, bucketPrefix);
  // todo huzx   
  cloud_env_options.keep_local_log_files = false;
  cloud_env_options.log_type = LogType::kLogKafka;
  cloud_env_options.kafka_log_options.client_config_params["metadata.broker.list"] = "172.20.3.83:9092";


  // create a bucket name for debugging purposes
  const std::string bucketName = bucketPrefix + kBucketSuffix;
  printf("Huzx: now the bucketName is: %s\n", bucketName.c_str());

  // Create a new AWS cloud env Status
  CloudEnv* cenv;
  Status s = CloudEnv::NewAwsEnv(Env::Default(), kBucketSuffix, kDBPath,
                                 kRegion, kBucketSuffix, kDBPath, kRegion,
                                 cloud_env_options, nullptr, &cenv);
  fprintf(stdout, "%s at line %d\n", __FUNCTION__, __LINE__);
  if (!s.ok()) {
    fprintf(stderr, "Unable to create cloud env in bucket %s. %s\n",
            bucketName.c_str(), s.ToString().c_str());
    return -1;
  }
  cloud_env.reset(cenv);

  // Create options and use the AWS env that we created earlier
  Options options;
  options.env = cloud_env.get();
  options.create_if_missing = true;
  options.create_missing_column_families = true;

  // No persistent read-cache
  std::string persistent_cache = "";

  // options for each write
  WriteOptions wopt;
  wopt.disableWAL = disableWAL;

  // open DB with two column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new one, too
  column_families.push_back(ColumnFamilyDescriptor("huzxcol", ColumnFamilyOptions()));

  std::vector<ColumnFamilyHandle*> handles;
  DBCloud* db;
  s = DBCloud::Open(options, kDBPath, column_families, persistent_cache, 0, &handles, &db, false);
  if (!s.ok()) {
   fprintf(stderr, "Unable to open db at path %s with bucket %s. %s\n",
           kDBPath.c_str(), bucketName.c_str(), s.ToString().c_str());
   return -1;
  }

  // open DB
  // s = DBCloud::Open(options, kDBPath, persistent_cache, 0, &db);

  // Put key-value
  printf("Start Write Key: %s\n", "key1");
  s = db->Put(wopt, handles[1], "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  printf("Start Get Key: %s\n", "key1");
  s = db->Get(ReadOptions(), handles[1], "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  /*
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(wopt, &batch);
  }
  */

  long totalWCnt = 0;
  char key[1024];
  char rocksV[1024];
  for (long i = 0; i < 100; i++) {
    WriteBatch batch;
      for (long j = 0; j < 1000; j++) {
         memset(key, 0, sizeof(key));
         memset(rocksV, 0, sizeof(rocksV));
          if (dftKeyLen <= 10) {
              sprintf(key, "%010ld%010ld", i, j);
          } else {
              sprintf(key, "%020ld%010ld", i, j);
          }
         generateRandomBucket(rocksV);
         batch.Put(handles[1], key, rocksV);
         totalWCnt++;
         printf("Write Key:%s, Value:%s\n", key, rocksV);
      } 
    db->Write(wopt, &batch);
    db->Flush(FlushOptions());
  }

  // print all values in the database
  printf("----------------Start Scan the database ----------\n");
  long totalRCnt = 0;
  ROCKSDB_NAMESPACE::Iterator* it =
      db->NewIterator(ROCKSDB_NAMESPACE::ReadOptions(), handles[1]);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    totalRCnt++;
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  delete it;

  // Flush all data from main db to sst files. Release db.
  if (flushAtEnd) {
    db->Flush(FlushOptions());
  }

  for (auto handle : handles) {
    s = db->DestroyColumnFamilyHandle(handle);
    assert(s.ok());
  }

  delete db;

  fprintf(stdout, "Successfully used db at path %s in bucket %s. totalWCnt:%ld, totalRCnt:%ld\n",
          kDBPath.c_str(), bucketName.c_str(), totalWCnt, totalRCnt);
  return 0;
}


