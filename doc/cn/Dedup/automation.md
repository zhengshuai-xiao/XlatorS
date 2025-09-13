Automation test

Use run_e2e_test.sh to test upload/download object 

1. start test env
2. start xlator dedup service
3. generate data
4. upload object
5. check dedup rate
6. upload object again
7. check dedup rate(should greater than 99%)
8. download object
9. delete all objects
10. garbage collection
11. delete bucket
12. cleanup

```
zxiao@localhost:/workspace/X/XlatorS$ make auto-test
--> Running auto tests...
./automation/run_e2e_test.sh
INFO: --- Starting Test Environment ---
make[1]: Entering directory '/workspace/X/XlatorS'
make[1]: Nothing to be done for 'build'.
make[1]: Leaving directory '/workspace/X/XlatorS'
INFO: Waiting for services to start...
INFO: --- Generating Test Data ---
50+0 records in
50+0 records out
52428800 bytes (52 MB, 50 MiB) copied, 0.0917539 s, 571 MB/s
50+0 records in
50+0 records out
52428800 bytes (52 MB, 50 MiB) copied, 0.122141 s, 429 MB/s
INFO: Generated test file /tmp/test_100M.data with MD5: d091074fd1a5bda5e5bb50caf661fa1d
INFO: --- Creating Bucket ---
2025/09/13 14:50:19 Successfully created bucket 'test.bucket'.
INFO: Bucket test.bucket created or already exists.
INFO: --- Running Upload Test ---
File uploaded successfully:
  Bucket:     test.bucket
  Object:     test_100M.data
  Size:       104857600 bytes
  ETag:       d41d8cd98f00b204e9800998ecf8427e
  Time taken: 620.291263ms
  Throughput: 161.21 MB/s
INFO: Upload completed.
INFO: Waiting for logs to be flushed...
INFO: --- Parsing Performance Metrics from Log (First Upload) ---
INFO: Deduplication Rate (First Upload): 49.98%,
INFO: Compression Rate (First Upload): 0.23%,
INFO: --- Running Second Upload Test (for dedup) ---
File uploaded successfully:
  Bucket:     test.bucket
  Object:     test_100M.data.2
  Size:       104857600 bytes
  ETag:       d41d8cd98f00b204e9800998ecf8427e
  Time taken: 675.093618ms
  Throughput: 148.13 MB/s
INFO: Second upload completed.
INFO: Waiting for logs to be flushed...
INFO: --- Parsing Performance Metrics from Log (Second Upload) ---
INFO: Deduplication Rate (Second Upload): 100.00%,
INFO: --- Running Download Test ---
Object downloaded successfully:
  From:       s3://test.bucket/test_100M.data
  To:         /tmp/test_100M.data.downloaded
  Size:       104857600 bytes
  Time taken: 141.612193ms
  Throughput: 706.15 MB/s
INFO: Download completed.
INFO: --- Verifying Data Integrity ---
INFO: Downloaded file MD5: d091074fd1a5bda5e5bb50caf661fa1d
INFO: MD5 checksums match. Data integrity verified.
INFO: --- Running Deletion and GC Test ---
INFO: Deleting object test_100M.data from bucket test.bucket...
Successfully deleted object 'test_100M.data' from bucket 'test.bucket'.
INFO: Object deleted.
INFO: Deleting the second object test_100M.data from bucket test.bucket...
Successfully deleted object 'test_100M.data.2' from bucket 'test.bucket'.
INFO: Second object 'test_100M.data.2' deleted.
INFO: Triggering Garbage Collection...
2025/09/13 14:50:25 Successfully published GC trigger command.
INFO: GC triggered. Waiting for it to run...
INFO: Delete Bucket
2025/09/13 14:50:40 Successfully removed bucket 'test.bucket'.
INFO: Verifying data cache cleanup...
INFO: Data cache is empty. GC test successful.
INFO: --- End-to-End Test Successful ---
INFO: --- Cleaning up ---
INFO: Stopping xlators process (PID: 52148)...
INFO: Cleanup complete.

```
