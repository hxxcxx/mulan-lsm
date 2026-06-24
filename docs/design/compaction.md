# Compaction — 触发、选文件、执行

**Compaction score**：
- L0：文件数 / `L0_COMPACTION_TRIGGER`（4）
- L1+：层总字节数 / `max_bytes_for_level(L)`（base=10MB，每层 x10）
- score > 1.0 触发，选 score 最大的层

**pick_compaction**：
- L0：全选所有 L0 文件（区间重叠，必须一起归并）
- L1+：从 `compact_pointer` 之后选第一个文件，加上 level+1 的重叠文件

**do_compaction**：
```
MergingIterator([输入文件](iterator.md) 多路归并) → 扫描全部条目
  对每个条目：
    Put → 写入输出
    Delete → 如果 is_base_level_for_user_key（[level+2+ 都没有该 key](version.md#get_overlaps)）→ 丢弃
           → 否则保留（丢 delete 会让更高层的旧版本"复活"）
  输出文件超出祖父层重叠阈值时切分
```

## 关键设计点

### 祖父重叠累加陷阱（两次修复）

**第一次**（重复累加）：原代码每条 entry 都遍历全部祖父文件，把覆盖该 user_key 的祖父 file_size 加进去。一个覆盖 N 条 entry 的祖父被加 N 次 → overlap 虚高 → 输出文件被过早切碎成大量小文件。修法：单调游标，跳过完全在 user_key 左侧的祖父。

**第二次**（游标归 0）：单调游标全局不归 0 时，切分后新文件 overlap 恒为 0（覆盖它的祖父已被前一个文件计入，游标已过）→ 新文件永不因祖父重叠切分 → 输出文件可能巨大。修法：**切分时 `grandparent_idx` 归 0 + `current_grandparent_overlap` 归 0**，新文件重新从 gp[0] 扫描计算自己的祖父重叠。

**关键语义辨析**：切分归 0 后，**同一祖父文件会被多个输出文件各计一次**——这是**对的**（每个输出文件确实与该祖父重叠，各自独立计算）。这与第一次 bug（**同一文件内**重复计入）是两回事。测试 `grandparent_overlap_recounted_after_split` 锁定了这一点。

### 层间重叠的来源

不是初始就有的，而是 LSM 逐层积压的自然结果。L0→L1→L2→L3 一路下推，每层独立 compaction，层与层之间从不协调边界，所以 L3 一个 `[a-z]` 大文件完全可能与 L1 的 `[a-c]` 共存。这恰恰是 LSM 的设计——每层内部有序就够了。

### 祖父层控制实战场景

`L1→L2` compaction 时发现 L3 有个 500MB 的 `[a-z]`。没有控制的话输出 `[a-c]` 到 L2 才 12MB，下次 `L2→L3` 就得 512MB。祖父层触发切分后，L2 被切成多个小文件，每轮只有小范围跟 L3 的大文件归并，单次 I/O 量可控。

### 精确推演：防下次 compaction 读太多

本次 compact `L1 → L2`，输入区间 `[k000, k300]`。祖父层 L3 有 6 个 2MB 文件恰好覆盖该区间。

**无控制**：输出一个文件到 L2。下次 `L2→L3` 捞全部 6 个 L3 文件，读 6 个文件归并。

**有控制**（`MAX_GRANDPARENT_OVERLAP_BYTES = 20MB`）：祖父重叠超阈值时切分，L2 被切成多个小文件。下次 `L2→L3` 选其中 1 个，只读 ≤ 少数 L3 文件。`10 × TARGET_FILE_SIZE` 是 LevelDB 经验值。

### inputs 文件的双重身份

`inputs[0]`（本层）+ `inputs[1]`（下一层）既是要**读入归并**的源，也是提交时**要从 Version 删除**的目标。`do_compaction` 末尾把所有输入文件加入 `deleted_files`，提交后新版 Version 不再引用它们，`remove_obsolete_ssts` 随后物理删除。

### level+1 文件选取的"并集外包络"权衡

L0 compaction 时 inputs[0] 是多个区间不连续的文件，选 inputs[1]（level+1 重叠文件）时用 inputs[0] 的**外包络** [min smallest, max largest] 调 [get_overlaps](version.md#get_overlaps)，而非逐个文件查。这会**多捞**一些 level+1 文件（被外包络覆盖但与具体 L0 文件不直接重叠的），但**绝不漏**——多捞的会被一起归并掉，正确性无损。宁可多捞不漏，是 LSM 选文件的安全原则。

### 锁粒度与 flush 并发

当前 mulan-lsm 的 `run_one_compaction` **全程持有** `Mutex<DbInner>`（pick → 读文件 → 归并 → 写 SST → 提交），期间前台写和 flush 完全阻塞。LevelDB 的做法是 pick 时拿锁、do_compaction I/O 时释放锁、install 时再拿锁提交，从而实现 compaction I/O 与 flush 的管道化——代价是 Version 可能变化需校验重试。

### current_file_number 的三时刻生命周期

`do_compaction` 是个状态机，`Option<FileNumber>` 表示"当前是否开着输出文件"：
1. **开文件**（首条 entry 或切分后）：`id_gen.new_file_number()` 分配编号 → `File::create(sst_path(dir, num))` 在磁盘建文件 → `current_file_number = Some(num)`
2. **写 entry**：用 `current_builder`（持有那个 File）写，current_file_number 不变
3. **切分/收尾**（finish_current）：`builder.finish()` 刷盘 → 用 current_file_number 构造 `FileMetaData`（编号+大小+smallest+largest）→ push 进 new_files

这条状态机和"何时切分"（祖父重叠）联动：切分分支里先 finish 旧文件（用旧 number）、再分配新 number 开新文件。
