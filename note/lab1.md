# Lab 1

## 2.5 Heap File access
访问方法提供了一种访问磁盘特殊逻辑结构文件的入口。常见的访问方式包括堆文件和B树文件。在这里只需要实现堆文件访问。

`HeapFile`对象被存储在多个页面中，每个页面大小相同（大小在`BufferPool.DEFAULT_PAGE_SIZE`中被定义）。在SimpleDB中每个表都有一个`HeapFile`。