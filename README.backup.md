# Backup and Restore Utility

基于XlatorS Dedup提供的功能，我们可以在这个基础之上实现一个文件目录的备份、恢复功能，

这里使用 `xcli` 的命令行工具，除了它提供了与 S3 兼容存储进行交互的功能，还增加了文件目录的备份和恢复。

## 通用参数

所有命令都需要 S3 连接信息，可以通过命令行参数提供：

- `--endpoint`: S3 服务地址 (例如: `s3.example.com`)。
- `--access-key`: S3 Access Key ID。
- `--secret-key`: S3 Secret Access Key。
- `--no-ssl`: 如果 S3 服务不使用 SSL，请添加此选项。

---

## 1. 备份 (`backup`)

`backup` 命令将一个或多个本地文件或目录打包，并上传到 S3 存储桶中。备份由两个对象组成：

- 一个 `.HDR` (header) 文件，包含所有文件的元数据清单。
- 一个 `.DATA` 文件，包含所有文件的内容拼接。

这种设计使得在恢复时，即使是恢复单个小文件，也无需下载整个备份集。

### 命令语法

```shell
bin/xcli backup --bucket <bucket-name> --src-dir <directory-to-backup>
```

- `--bucket`: 用于存储备份的目标 S3 存储桶。
- `--object-base-name`: 备份对象的唯一基本名称。命令将创建 `<backup-name>.HDR` 和 `<backup-name>.DATA`。建议使用时间戳以保证唯一性 (例如: `backup-$(date +%Y%m%d%H%M%S)`)。
- `[要备份的路径...]`: 一个或多个要包含在备份中的本地文件或目录。

### 示例

将 `/etc/nginx` 目录和 `/var/log/app.log` 文件备份到 `my-app-backups` 存储桶中。

```shell
bin/xcli backup \
    --bucket "my-app-backups" \
    --object-base-name "backup-$(date +%Y%m%d%H%M%S)" \
    /etc/nginx \
    /var/log/app.log
```

---

## 2. 恢复 (`restore`)

`restore` 命令从 S3 存储桶中获取备份，并将其解压到本地目录。您可以选择恢复整个备份，也可以指定恢复其中的部分文件或子目录。

### 命令语法

```shell
bin/xcli restore --bucket <bucket-name> --object-base-name <backup-name> --dest-dir <local-path> [要恢复的路径1] [要恢复的路径2] ...
```

- `--bucket`: 包含备份的源 S3 存储桶。
- `--object-base-name`: 要恢复的备份的基本名称 (不带 `.HDR` 或 `.DATA` 后缀)。
- `--dest-dir`: 用于存放恢复后文件的本地目录。
- `[要恢复的路径...]`: (可选) 备份中要恢复的特定文件或目录。如果省略，将恢复整个备份。

### 示例

#### a. 完整恢复

将 `backup-20250913003839` 的全部内容恢复到 `/tmp/full-restore` 目录。

```shell
bin/xcli restore --bucket "my-app-backups" --object-base-name "backup-20250913003839" --dest-dir "/tmp/full-restore"
```

#### b. 部分恢复

仅从 `backup-20250913003839` 备份中恢复 `nginx/nginx.conf` 文件到 `/tmp/partial-restore` 目录。

```shell
bin/xcli restore --bucket "my-app-backups" --object-base-name "backup-20250913003839" --dest-dir "/tmp/partial-restore" nginx/nginx.conf
```

---

## 3. 列出备份 (`image-list`)

`image-list` 命令用于列出指定 S3 存储桶中所有可用的备份。它通过查找 `.HDR` 文件来识别备份集，并显示它们的 `object-base-name`，方便您了解有哪些备份可以恢复。

### 命令语法

```shell
bin/xcli list-backup --bucket "my-app-backups" --object-base-name backup-20250913003839
```

- `--bucket`: 要查询备份列表的 S3 存储桶。

### 示例

列出 `my-app-backups` 存储桶中的所有备份。

```shell
zxiao@localhost:/workspace/X/XlatorS$ bin/xcli list-backup --bucket backup.xzs --object-base-name backup-20250913003839
2025/09/13 15:02:13 Downloading manifest: backup-20250913003839.HDR
2025/09/13 15:02:13 Manifest parsed successfully, found 4 files/directories.
Mode         Size      Modified              Name
-rwxr-xr-x   0         2025-09-13 00:37:39   backup
-rw-r--r--   3019      2025-09-13 00:37:33   backup/1.data
-rw-r--r--   2097152   2025-09-13 00:37:39   backup/2M.data
-rw-r--r--   1048576   2025-09-13 00:37:26   backup/xzs1M.data

```
