# Race Condition Research

这个系统中有一些并发时的竞争问题需要仔细推敲

## Object deletion

当系统有持续不断的写时，用户执行了删除对象命令，有两个方案：

1.立即删除对象的元数据，以及在refDB中的引用

这样就需要在GC的时候保证不会被正在写入的数据引用的data container被误删，如果误删会造成data loss

2.延时删除，那什么时候删呢？

为了防止竞争，就需要在没有写任务时删，这样就得需要一个异步任务去做清理。

或者是检查一下这个object引用了哪些data container, 这些data container有没有被fp cache加载进去，没有加载进去的可以删。

## Object Protection

是否应该对那些被加载fp的Object进行保护呢，做延时删除？

## Garbage Collection

当系统有持续不断的写时，GC触发了

1.选择哪些data container来删除呢？

* dc1在refdb里已经没有引用了，也没有任何fp cache加载它，可以删除
* dc2在refdb里没有引用了，但有fp cache 加载了它,那如何将它安全删除呢？
  * 如何判断这个dc2有没有被当前的任何正在写的object引用了？
    * 去manifest里的unique dcid里去找一下，这个过程需要加锁
    * 如果是分布式多节点的情况，需要去每一个节点都要查一下
  * 如果判断了没有任何正在写的object引用，需要在fp cache前增加一个deleted_dc,这样所有从fp cache 出去的检查都再经过deleted_dc，来决定是否去重
* dc3在refdb里有引用，不能被删

## Server Global FP Cache and Client FP Cache

同样的问题，要保证不出现data loss，FP cache里加载的data container 不能被删除
