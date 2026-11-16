# 新增的流式接入契约

原abi.py、producer.py、checks.py及其布局约定保留。stream_abi.py声明真实ArrowArrayStream C ABI，stream_fixture.Source是来源生产器，只导出回调和物理缓冲区，不包含消费、解码、视图引用或并发适配答案。只处理本项目公开的int64、UTF-8、list、struct、int32字典索引类型及合法C指针；不要求任意Arrow扩展、畸形指针或网络。

新增stream_import.open_stream(ptr)，ptr为POINTER(ArrowArrayStream)，调用者持有Source对象直到全部根已释放。成功或get_schema失败均接管流根；get_schema只调用一次，复制错误诊断后在抛出异常前释放仍归本方的根。来源错误抛SourceError(code,message)，公开code、message属性；不能把错误误认作正常结束。调用open_stream后，原指针仅供生产器测试观测，调用者不再自行消费或释放。

Reader.next()返回下一个Batch；成功且根array.release为空是正常流结束，返回None且关闭流。零行但release非空仍是一个需释放的批次，不能当结束。Reader.close()幂等。Reader和Batch都支持with，异常退出同样close。显式关闭、错误或结束之后next()均返回None，不再调用来源。open_stream失败不得返回Reader。只在回调成功时接管其输出；本项目错误生产器将输出置空，不要求消费错误回调的未定义输出。

多个线程可同时调用同一Reader.next/close，同一Batch的row/slice/close及兄弟视图操作；不要求支持从来源回调重入Importer。get_schema/get_next/get_last_error/stream.release不并发重入来源，get_last_error在复制原诊断之前不能被另一个来源请求覆盖。close等待已经开始的来源取批完成后释放流；已经开始的取批可以先返回自己的Batch，Batch不能被Reader关闭破坏。close返回后没有来源调用再开始。正常结束、错误与关闭竞争时流根恰好释放一次。

Batch.length为非负整数，row(i)返回当前视图第i行的独立Python值，slice(start,length)返回共享底层所有权的新视图。切片仍按原物理offset、父子偏移、空值位和字典规则解释，只转当前请求行。只接受整数且拒绝bool，非整数抛TypeError；负索引、越界以及超出右端的切片抛IndexError，不支持Python负下标。关闭视图之后length/row/slice抛ValueError；close幂等。slice(0,0)可以持有零行视图，直到它关闭仍拥有根。多个线程之间的操作须可解释为合法顺序：在读/切片开始前已关闭的视图应失败；已开始读取可先完成，但其数组根不能在读取期间释放。返回的Python行值不借用C存储。

各批次及其任意深切片共享该批次的数组所有权，读者关闭不撤销已交付视图。仅在该批次最后开放视图和最后在途读取结束后释放数组根。模式根要覆盖所有仍依赖模式的解码/视图；可以复制必要类型描述后提前释放模式，也可以持有共享模式根直至依赖者释放，但都须证明模式/字典及子类型解释仍可用。每个已接管数组、模式和流根恰好调用一次release；不得自行遍历释放子节点，根回调负责其子树；不依赖垃圾回收。不强制特定锁、计数器或类型计划表示。

不得对整批预先转成Python行，也不通过完整数据复制把生命周期问题隐藏。原large(200000)生产器的缓冲区构建不计测量；构建后通过来源导出一批，open_stream、next、创建100个共享切片、读取三行、关闭Reader及全部视图的tracemalloc峰值增量须≤4MiB。测量前可构建large(1)模式束和large(200000)数据束，Source(count=1,factory=lambda:data_bundle,schema_factory=lambda:schema_bundle)在调用时只移动预建根。导出C根的必要小对象复制与必要类型描述复制允许；完整值缓冲复制不允许。资源指标用于支持大型采集批次按需接入；不增加全批转换或网络目标。

下文固定布局继续适用于row，不自动增加其他Arrow类型。abi.py、producer.py、checks.py必须保持原字节。补充目标的独立值、线程交错、错误后释放与结束核验；至少用真实C回调和真实线程验证，不以伪造成功事件或只验证自带verify退出码代替。更新.pointwise-runtime.json的verify/demo，demo真实消费流并保留切片跨Reader关闭读取，再展示释放记录。只交付这一项流式接入，无持久化仓库、网络下载或跨进程共享内存。


# 物理布局
The root is a struct (+s), whose uniquely named fields are signed int64 (l),
UTF-8 (u), list (+l), or an int32 dictionary index (i) with a UTF-8 dictionary.
A list has exactly one child of any supported field type, including a nested
struct/list. Maximum schema depth is four; fields are at most eight per struct.
Dictionary values can be null. No other extension, union or decimal type is in
scope. Input schemas, offsets, lengths, UTF-8, dictionary indices and buffer
addresses are legal and stable until release; the receiver need not validate
malformed producer pointers. The new stream contract above defines concurrency.

Array.length is the logical length; Array.offset may be nonzero at every level.
Logical element i uses physical position Array.offset+i. Validity bits are LSB
first and use that physical position. A null validity pointer means all valid;
null_count may be -1 and must not be used as a substitute for the bitmap.
An invalid element is None, with no need to inspect its value or nested offsets.
For a struct at physical position p, each child is read at logical index p,
then that child's own offset applies. For a list at physical position p, int32
offsets[p:p+2] select the child logical half-open range; the child's own offset
also applies. For UTF-8, int32 offsets index the byte buffer. A dictionary index
is read as signed int32 then addresses a logical dictionary element, including
its own offset and validity. Null dictionary elements become None.
A struct value is a dict by schema field name, a list value is a Python list,
and scalar values are int/str/None. Only a requested row is materialized.


原small()三行值为 {"id":10,"tag":"蓝","samples":[5,6]}, None, {"id":30,"tag":None,"samples":[7]}。
