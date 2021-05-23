// Copyright (c) 2017-present, Rockset, Inc.  All rights reserved.
#include <cstdio>
#include <iostream>
#include <string>
#include <algorithm>
#include <uuid/uuid.h>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

void generateRandomBucket(char buf[]) {
    uuid_t uu;
    struct timeval tv;
    uuid_generate(uu);
    uuid_unparse(uu,buf);
}

int case_insensitive_match(std::string s1, std::string s2) {
   //convert s1 and s2 into lower case strings
   transform(s1.begin(), s1.end(), s1.begin(), ::tolower);
   transform(s2.begin(), s2.end(), s2.begin(), ::tolower);
   if(s1.compare(s2) == 0)
      return 0; //The strings are same
   return 1; //not matched
}


// This is the local directory where the db is stored.
std::string kDBPath = "/app/rockset/";

// This is the name of the cloud storage bucket where the db
// is made durable. if you are using AWS, you have to manually
// ensure that this bucket name is unique to you and does not
// conflict with any other S3 users who might have already created
// this bucket name.
// std::string kBucketSuffix = "cloud.durable.complex000.";
std::string kBucketSuffix = "com.zetyun.rt.";
std::string kRegion = "us-west-2";

static const bool flushAtEnd = true;
static const bool disableWAL = false;

int main(int argc, char** argv) {

  /**
   * argv:
   * 1: UserName of Subffix
   * 2: ColumnFamilyDescriptor
   */
  std::string userName = "";
  std::string columnF = "";
  if (argc >= 3) {
      std::string lUserName(argv[1]);
      std::string lColumnF(argv[2]);
      userName = lUserName;
      columnF = lColumnF;
  } else {
      userName = "huzx";
      columnF = kDefaultColumnFamilyName;
  }
  printf("Input column family is:[%s]\n",  columnF.c_str());

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

  // "rockset." is the default bucket prefix
  const std::string bucketPrefix = "rockset.";
  kBucketSuffix +=  userName;
  kDBPath += userName;
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

  DBCloud* db;
  std::vector<ColumnFamilyHandle*> handles;
  std::vector<ColumnFamilyDescriptor> column_families;
  bool isDftColumnF = (case_insensitive_match(columnF, kDefaultColumnFamilyName)) == 0;
  printf("Is Same ColumnF: input:%s, default:%s\n", columnF.c_str(), kDefaultColumnFamilyName.c_str());
  if (!isDftColumnF) {
      // open DB with two column families
      // have to open default column family
      column_families.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
      // open the new one, too
      column_families.push_back(ColumnFamilyDescriptor(columnF, ColumnFamilyOptions()));
      s = DBCloud::Open(options, kDBPath, column_families, persistent_cache, 0, &handles, &db, false);
  } else {
      s = DBCloud::Open(options, kDBPath, persistent_cache, 0, &db);
  }
  s = DBCloud::Open(options, kDBPath, column_families, persistent_cache, 0, &handles, &db, false);
  if (!s.ok()) {
   fprintf(stderr, "Unable to open db at path %s with bucket %s. %s\n",
           kDBPath.c_str(), bucketName.c_str(), s.ToString().c_str());
   return -1;
  }
  
  long totalWCnt = 0;
  char key[1024];
  char rocksV[1024];
  for (long i = 0; i < 100; i++) {
    WriteBatch batch;
      for (long j = 0; j < 1000; j++) {
         memset(key, 0, sizeof(key));
         memset(rocksV, 0, sizeof(rocksV));
         sprintf(key, "%020ld%010ld", i, j);
         generateRandomBucket(rocksV);
         if (!isDftColumnF) {
             batch.Put(handles[1], key, rocksV);
         } else {
             batch.Put(key, rocksV);
         }
         totalWCnt++;
         printf("Write Key:%s, Value:%s\n", key, rocksV);
      } 
    db->Write(wopt, &batch);

     if (isDftColumnF) {
        db->Flush(FlushOptions());
     } else {
        db->Flush(FlushOptions(), handles[1]);
     }
  }

  // print all values in the database
  printf("----------------Start Scan the database ----------\n");
  long totalRCnt = 0;
  ROCKSDB_NAMESPACE::Iterator* it = NULL;
  if (isDftColumnF) {
    it = db->NewIterator(ROCKSDB_NAMESPACE::ReadOptions());
  } else {
    it = db->NewIterator(ROCKSDB_NAMESPACE::ReadOptions(), handles[1]);
  }
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    totalRCnt++;
    std::cout << it->key().ToString() << ": " << it->value().ToString() << std::endl;
  }
  delete it;

  if (!isDftColumnF) {
      for (auto handle : handles) {
          s = db->DestroyColumnFamilyHandle(handle);
          assert(s.ok());
      }
  }
  delete db;

  fprintf(stdout, "Successfully used db at path %s in bucket %s. totalWCnt:%ld, totalRCnt:%ld\n",
          kDBPath.c_str(), bucketName.c_str(), totalWCnt, totalRCnt);
  return 0;
}


