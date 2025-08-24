# Dedup TODO

1. architecture design
2. Write(MakeBucketWithLocation/PutObject/NewMultipartUpload）
3. Read（ListBuckets/GetBucketInfo/GetObject/GetObjectInfo/ListObjects）
4. Delete
5. GC
6. relationship: logic bucket<-->backend bucket
7. chunking algorithm: FLC(fixed length chunk, rabinCDC, fastCDC, GearCDC...)
8. performance
   1. FP cache
   2. local cache
   3. structure Serialize/Deserialize
   4.
