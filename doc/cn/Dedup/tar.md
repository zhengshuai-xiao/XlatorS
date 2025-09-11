Tar Stream

我需要增加处理输入为tar stream的功能，

1.需要在userdefined里判断是否为tar format stream/object，如果是按下面步骤处理

2.tar stream 过来的数据需要解析为两部分一部分为object_key.DATA, 另一部分为object_key.HDR，他们分别存储tar流里的数据内容和文件metadata内容，这两个分别为独立的两个object

object_key.DATA会正常参与去重落盘，去重的时候需要以文件末尾为边界

object_key.HDR应该增加一个字段记录当前文件在DATA里的offset，这样为了读的时候能找到具体位置，HDR不需要做任何切块行为，直接序列化后写入object文件即可

3.读的时候需要支持list文件、恢复整个文件目录、恢复单独某个文件或目录的功能
