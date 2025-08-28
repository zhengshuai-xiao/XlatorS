# 指纹查询与多层缓存优化

在重删系统中，指纹查询（Fingerprint Lookup）是决定写入性能的关键路径。频繁地与后端元数据服务（如 Redis）进行网络通信会引入显著的延迟。为了最大限度地减少这种开销，`Dedup Xlator` 采用了一套精巧的多层缓存策略。

这套策略的核心思想是：**尽可能在离计算节点最近、最快的地方命中缓存**。

整个查询流程遵循以下三级缓存层次结构：

1. **对象内缓存 (Intra-Object Cache)**：内存中的 `map`，生命周期与单个对象（或分块上传会话）绑定。
2. **批次内缓存 (Intra-Batch Cache)**：内存中的临时 `map`，生命周期仅限于单次数据块打包写入（`writeDObj`）的过程。
3. **全局命名空间缓存 (Global Namespace Cache)**：持久化在 Redis 中的哈希表，为整个命名空间提供全局去重。

## 查询流程详解

当一个写请求（`PutObject` 或 `PutObjectPart`）到达时，数据流被切分成一个个数据块（Chunk）。对于每一批（例如 16MB）数据块，系统会执行以下查询流程：

```mermaid
graph TD
    A[开始处理一批 Chunks] --> B{在 '对象内缓存' (localFPCache) 中查找?};
    B -- 命中 --> C[标记为已去重, 复用 DOid];
    B -- 未命中 --> D{在 '全局缓存' (Redis FPCache) 中查找?};
    C --> E[处理下一批 Chunks];
    D -- 命中 --> F[标记为已去重, 复用 DOid];
    F --> G[将(FP, DOid)存入 '对象内缓存'];
    G --> E;
    D -- 未命中 --> H[标记为新数据块];
    H --> I[调用 writeDObj 写入新数据块];
    I --> J[将新分配的(FP, DOid)存入 '对象内缓存'];
    J --> E;
```

### 1. 对象内缓存 (`localFPCache`)

这是最高优先级的缓存，也是性能提升最显著的一环。

* **目的**：实现单个对象内部的自我去重。对于一个内部含有大量重复数据块的文件（如虚拟机镜像、拷贝粘贴的文档），这个缓存能确保重复块只被处理一次。
* **实现**：一个存在于内存中的 `map[string]uint64`。
* **生命周期**：
    *   对于普通上传 (`PutObject`)，它在 `PutObject` 函数开始时创建，与该次上传的 `manifestID` 关联，并存入一个全局缓存池中。在对象写完后，该缓存会从池中被清理。
    *   对于分块上传 (`NewMultipartUpload`)，它在会话开始时创建，与 `uploadID` (其本身也是一个 `manifestID`) 关联，并存入同一个全局缓存池中。所有属于该 `uploadID` 的 `PutObjectPart` 请求都会共享这一个缓存。在 `CompleteMultipartUpload` 或 `AbortMultipartUpload` 后，该缓存会从池中被清理。
* **工作机制**：
  1. 在查询 Redis 之前，首先检查 `localFPCache`。
  2. 如果命中，直接使用缓存的 `DOid`，避免了任何网络请求。
  3. 当一个数据块被确认（无论是从 Redis 查到还是新写入）后，它的指纹和 `DOid` 会被立即更新到 `localFPCache` 中，供后续的数据块使用。

**代码示例 (`dobj_ops.go` - `writeObj` 函数):**
```go
// `localFPCache` is passed in as a parameter to enable intra-object deduplication.
// func (x *XlatorDedup) writeObj(..., localFPCache map[string]uint64) (...) {
// ...
for i := range chunks {
    if doid, ok := localFPCache[chunks[i].FP]; ok {
        chunks[i].Deduped = true
        chunks[i].DOid = doid
        // ...
    } else {
        // ... Prepare to query Redis
    }
}

// ... 查询和写入后 ...

// Update local cache with newly written chunks for intra-object dedup.
for i := range chunks {
    if !chunks[i].Deduped {
        localFPCache[chunks[i].FP] = chunks[i].DOid
    }
}
// }
```

### 2. 批次内缓存 (`batchFPCache`)

这是一个非常短命但高效的缓存，用于优化数据打包（Chunk Packing）的过程。

* **目的**：防止在同一个数据对象（DObj，例如 16MB）中写入内容完全相同的多个数据块。
* **实现**：一个临时的 `map[string]uint64`。
* **生命周期**：在 `writeDObj` 函数开始时创建，函数结束时销毁。
* **工作机制**：
  1. `writeDObj` 函数接收一批需要写入的新数据块。
  2. 在将数据块的二进制内容写入本地 DObj 文件之前，先在 `batchFPCache` 中查找。
  3. 如果命中，说明本批次内已经写入过一个一模一样的块了。此时只需复用 `DOid`，并**跳过文件写入操作**。
  4. 如果未命中，则正常写入文件，并将新分配的 `DOid` 存入 `batchFPCache`。

**代码示例 (`dobj_ops.go` - `writeDObj` 函数):**

```go
// A short-lived cache to handle duplicates within this single batch of chunks.
batchFPCache := make(map[string]uint64)

for i, _ := range chunks {
    if chunks[i].Deduped {
        continue
    }

    // Check for duplicates within this batch.
    if doid, ok := batchFPCache[chunks[i].FP]; ok {
        chunks[i].DOid = doid
        continue // Skip writing this chunk
    }

    // ... 写入数据到 DObj 文件 ...

    // Cache the FP and its newly assigned DOid for this batch.
    batchFPCache[chunks[i].FP] = chunks[i].DOid
}
```

### 3. 全局命名空间缓存 (Global Namespace Cache)

这是最后一层缓存，也是去重的基石，提供了跨对象的全局去重能力。

* **目的**：在整个命名空间（Namespace）范围内实现数据去重。
* **实现**：Redis 中的一个 Hash 数据结构，Key 为 `namespace.FPCache`。
* **工作机制**：
  1. 当一个数据块在 `localFPCache` 中未命中时，系统会通过 `DedupFPsBatch` 批量查询 Redis。
  2. 如果命中，说明该数据块在全局已存在，复用其 `DOid`。
  3. 当一个新对象成功写入后，其包含的所有新数据块的指纹会通过 `InsertFPsBatch` 批量写入 Redis。

## 总结

通过这套 **对象内 -> 批次内 -> 全局** 的多层缓存查询机制，`Dedup Xlator` 实现了：

* **极致的性能优化**：大量查询在内存中完成，极大地减少了与 Redis 的网络交互。
* **高效的存储利用**：不仅实现了全局去重，还避免了在打包文件中写入冗余数据。
* **清晰的逻辑分层**：每一层缓存都有明确的职责和生命周期，使得代码易于理解和维护。
