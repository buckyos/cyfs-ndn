# fs_daemon 文件系统级测试用例设计

## 0. 统一约定与测试骨架建议

**目录与变量**

* 挂载点：`$MNT`（例如 `/mnt/ndm`）
* 每条用例在独立目录下跑，避免互相污染：`$T=$MNT/.ndm_test/<case_id>`
* 每条用例结束清理：`rm -rf "$T"`（失败时可选择保留现场）

**通用断言方式（bash）**

* 内容一致性：`sha256sum`/`cmp`
* 元数据：`stat -c '%s %Y %i %f %a' file`
* 错误码：`cmd; echo $?` 或 python 捕获 `OSError.errno`

**容错（建议）**

* 对“不支持”的能力，用例不要写成“必须成功”，而写成：

  * **成功** 或 **返回预期的“明确不支持”错误码集合**（例如 `EOPNOTSUPP/ENOTSUP/ENOSYS/EPERM`）
* 对 FUSE/缓存导致的“短暂不可见”，并发用例里可加小范围重试，但要有超时上限（例如 3s 内一致即可）。

---

## A. 基础 POSIX 语义（不依赖物化，最适合先落地）

### A01 挂载点可用性与最小读写

**目的/坑**：挂载异常、只读挂载、基础操作崩溃
**步骤**

1. `mkdir -p "$T"`
2. `echo hello > "$T/a"`
3. `cat "$T/a" | grep hello`
4. `rm "$T/a"`
   **断言**

* 写入成功、读出一致、删除成功

---

### A02 O_CREAT|O_EXCL 语义（并发创建前置）

**目的/坑**：dentry 去重/冲突检查缺陷
**步骤**

* python：`os.open(path, os.O_CREAT|os.O_EXCL|os.O_WRONLY, 0o644)` 连续执行两次
  **断言**
* 第一次成功，第二次失败且 errno 为 `EEXIST`

---

### A03 rename 原子替换（rename over existing file）

**目的/坑**：`rename()` 不原子 / 目标内容短暂不可读 / 覆盖语义错
**步骤**

1. `echo OLD > "$T/dst"`
2. `echo NEW > "$T/src"`
3. `mv -f "$T/src" "$T/dst"`
4. `cat "$T/dst"`
   **断言**

* `$T/dst` 内容为 `NEW`
* 过程中不应出现 `$T/dst` “不存在”的窗口（可并发起一个读线程循环读，见 A11）

---

### A04 open-unlink 语义（打开后删除）

**目的/坑**：inode/dentry 生命周期不符合 POSIX，导致编译器/临时文件逻辑异常
**步骤（python 更好）**

1. 打开 `$T/f` 写入随机数据，`fsync`
2. `os.unlink("$T/f")`
3. 继续通过已打开 fd 读取/写入
4. 关闭 fd
   **断言**

* unlink 后路径不可见（`stat` 失败）
* fd 仍可读写；关闭后数据最终不可再访问（路径不存在）

---

### A05 O_APPEND 与 seek 的交互

**目的/坑**：追加语义错误（很多工具依赖 O_APPEND，尤其日志/构建工具）
**步骤**

1. `printf 'AAAA' > "$T/f"`
2. 用 python `os.open` 带 `O_APPEND`，先 `os.lseek(fd, 0, SEEK_SET)` 再写入 `BBBB`
   **断言**

* 最终内容应为 `AAAABBBB`（而不是覆盖开头）

---

### A06 pwrite/随机写（DiffChunkList 覆盖路径的外部表现）

**目的/坑**：随机写是 Diff/Overlay 最容易出错的地方之一
**步骤（建议 python）**

1. 创建 1MB 文件全 0
2. 在多个随机 offset 做 `pwrite` 写入 pattern（例如每次写 4096 bytes 的固定字节）
3. 读回整文件，与内存中的期望镜像比对 hash
   **断言**

* 内容逐字节一致
* `stat size` 正确

---

### A07 truncate（变小/变大）

**目的/坑**：构建工具/下载器会用 truncate；变大时是否补 0
**步骤**

1. 写入 `0123456789`
2. `truncate -s 5 "$T/f"`；读出应为 `01234`
3. `truncate -s 12 "$T/f"`；读出长度 12，后 7 字节应为 `\0`（或若不支持，则明确报错）
   **断言**

* 支持时符合 POSIX；不支持时返回明确错误码且不破坏原文件

---

### A08 fsync / close 持久性（至少对“同进程重开”可见）

**目的/坑**：FileBuffer flush/close 链路不可靠导致读到旧数据
**步骤**

1. 写入随机数据
2. `fsync(fd)` 或 `sync;`（注意 `sync` 是系统级，fsync 更精确）
3. 关闭
4. 新进程打开读出，校验 hash
   **断言**

* hash 一致

---

### A09 mkdir/rmdir、非空 rmdir

**目的/坑**：目录项管理与错误码一致性
**步骤**

1. `mkdir "$T/dir"`
2. `touch "$T/dir/a"`
3. `rmdir "$T/dir"`
   **断言**

* 第 3 步失败，errno 为 `ENOTEMPTY`（或等价）

---

### A10 readdir 完整性（基础）

**目的/坑**：readdir 漏项/重复项（list/cache/rev 相关问题常外显为 ls/find 异常）
**步骤**

1. 创建 N=2000 文件：`printf '%s' {1..2000} | xargs -I{} touch "$T/dir/f_{}"`
2. `ls "$T/dir" | wc -l`
   **断言**

* 数量 == 2000（允许极短重试窗口，但最终一致）

---

### A11 readdir 与 rename 原子性并发观察

**目的/坑**：目录缓存/失效不正确时，容易出现 “ls 看不到/看到旧名很久”
**步骤**

* 进程 1：循环 `mv "$T/dir/a" "$T/dir/b"` 与 `mv "$T/dir/b" "$T/dir/a"`
* 进程 2：循环 `ls "$T/dir" | grep -E '^(a|b)$'` 统计是否出现“两者都不存在”或“两者都同时存在”
  **断言**
* 理想：任意时刻最多只有一个名字存在；不能长期出现都不存在/都存在
* 如果允许短暂抖动，也应在极短时间内收敛（否则就是 cache/rev 失效坑）

---

### A12 权限/模式位最小兼容（chmod/chown 可选）

**目的/坑**：工具链会 chmod（比如 git checkout、tar 解包）
**步骤**

1. `umask 077; touch "$T/p"`
2. `stat -c '%a' "$T/p"`
3. `chmod 600 "$T/p"`
   **断言**

* 若支持：模式位变化符合预期
* 若不支持：明确 `EOPNOTSUPP/EPERM`，且不影响读写基本功能

---

### A13 xattr 基础（可选但强烈建议测）

**目的/坑**：一些程序会探测 xattr（安全模块、桌面环境、tar）
**步骤**

* `setfattr -n user.test -v 123 "$T/f"` / `getfattr -d "$T/f"`
  **断言**
* 支持则可读回；不支持则错误码明确且不 crash

---

### A14 mmap（读写映射）

**目的/坑**：如果 FUSE/实现对 mmap 支持不稳，容易出现读到旧页/写丢失
**步骤（python）**

1. 创建文件写入已知内容
2. `mmap` 读校验
3. `mmap` 写入后 `flush`，关闭后再读校验
   **断言**

* 一致

---

## B. 并发与 lease（覆盖“写租约/会话”、常见构建并发）

> 设计文档里明确提到写入会申请 `file_write_lease` 并绑定 `lease_client_session`，并且讨论了物化 cacl_name 与再次 open 写入的 CAS 竞态。
> 所以下面这些用例非常关键：它们通常是“最容易出线上偶发 bug”的坑。

### B01 同一文件双写者（应该互斥或有明确冲突策略）

**目的/坑**：lease 失效、并发写导致 silent corruption
**步骤**

* 进程1：打开 `$T/f` 写入并 sleep 不关闭
* 进程2：尝试以写方式打开同一文件并写入
  **断言（两种可接受策略二选一）**

1. **互斥策略**：进程2 open 失败（`EACCES/EBUSY/ETXTBSY` 等），文件内容只来自进程1
2. **允许并发但必须有确定语义**：最终内容可预测且不损坏（一般不推荐；大多数实现会选择互斥）

> 建议把“期望错误码集合”放宽到实现实际返回的那一个，但要“稳定一致”。

---

### B02 多进程并发创建同名文件（同目录高竞争）

**目的/坑**：dentry upsert + rev 乐观锁处理不当会出现重复/幽灵项
**步骤**

* 10 个进程同时执行 `open(O_CREAT|O_EXCL)` 创建 `$T/dir/x`
  **断言**
* 只有 1 个成功；其余都是 `EEXIST`
* 最终 readdir 只有一个 `x`

---

### B03 写者崩溃后的 lease 回收（kill -9）

**目的/坑**：写者异常退出导致 lease 永久占用，后续无法写
**步骤**

1. 进程1 打开写 `$T/f`，写入一半后 `kill -9`
2. 等待一个“你们 lease 过期时间 + buffer”
3. 进程2 再次打开写入并关闭
   **断言**

* 最终可以重新写（不会永久 `EBUSY`）
* 文件内容要么是“旧版本”、要么是“新版本”，但不能是结构性损坏（例如读报错、size/内容乱）

---

### B04 读者在写入期间读取（可见性与一致性）

**目的/坑**：FileBuffer 分层读取（base_reader + fb_reader）组合逻辑容易出现“读到拼接错位/越界”
**步骤**

* 写者分块写入：每 4KB 写一个递增序号块（0,1,2,...），每写一块 sleep 10ms
* 读者持续从头读到当前 size，校验每个块要么是全旧（未写到），要么是全新（已写到），不要出现“块内半旧半新”的撕裂（除非你们语义允许）
  **断言**
* 不崩溃、不越界、内容满足定义的一致性模型（至少块级一致）

---

## C. 物化/状态机相关（Working/Cooling/Linked/Finalized 的坑）

这些用例如果 **没有“手工物化/查询状态”的工具** 会变成“等待型、非确定性”
如果有类似 `ndmctl finalize <path>`、`ndmctl inode_state <path>` 的调试接口，建议直接用它让测试确定性。

### C01 close 后很快 reopen 并写入（覆盖设计文档里提到的 CAS 竞态）

**目的/坑**：cacl_name 正在进行时再次 open 写导致状态回退/数据错
**步骤**

1. 写入 `$T/f` -> close
2. 立刻 reopen 写入新内容（多轮循环）
3. 并发（可选）触发/等待后台物化线程
   **断言**

* 最终文件内容必须是“最后一次写入”的内容
* 不能出现“写入成功但之后读回旧内容”

---

### C02 物化搬运期间读（move_to_store 过程）

**目的/坑**：从 FileBuffer 搬到 named_store 的过程中，读路径可能切换（fb_reader vs store_reader），容易出现短暂不可读
**步骤**

1. 写一个较大的文件（例如 128MB）
2. close 后触发物化（等待/手工）
3. 在物化过程中持续 `sha256sum "$T/f"`（或分段读）
   **断言**

* 读取要么一直成功，要么失败也必须是“可重试的短暂错误”且很快恢复（更理想是完全无感）
* 最终 hash 固定不变

---

### C03 Finalized 后再次写（实例化：FileObject -> FileNode）

**目的/坑**：反物化/实例化路径出错会导致写失败或内容丢
**步骤**

1. 生成文件并确保进入 Finalized（手工/等待）
2. 重新打开写，在中间 offset 改 4KB
3. close 后全文件校验
   **断言**

* 修改点生效，其他部分保持原样

---

## D. DirObject/BaseDirObject/Overlay/Tombstone（目录“坑”集中地）

这些用例的关键前提是：**让某个目录具有 BaseDirObject（或通过 snapshot 得到只读 DirObject）**。

### D01 BaseDirObject 下删除子项（Tombstone 行为）

**目的/坑**：rm 后仍在 ls 中出现（因为 base children 没被 tombstone 掩盖）
**前置**

* 通过 snapshot 或 finalize 让 `$T/base` 变成“带 BaseDirObject 的目录”
* 确保 base 里有 `a.txt`
  **步骤**

1. `rm "$T/base/a.txt"`
2. `ls "$T/base" | grep a.txt`
   **断言**

* `a.txt` 不再出现（tombstone 生效）

---

### D02 Tombstone 后同名重建

**目的/坑**：tombstone 永久遮蔽导致无法重建同名文件
**步骤**

1. 在 D01 后 `echo new > "$T/base/a.txt"`
2. 读回并校验
   **断言**

* 能创建成功，且内容为 `new`

---

### D03 rename/move 更新两个 parent rev（跨目录移动）

**目的/坑**：设计文档提到 rename(move) 需要更新两个 inode rev；做不好会导致 src/dst 目录缓存不一致
**步骤**

1. `mkdir "$T/d1" "$T/d2"; echo x > "$T/d1/f"`
2. 并发线程不断 `ls "$T/d1"`、`ls "$T/d2"`
3. 执行 `mv "$T/d1/f" "$T/d2/f"`
   **断言**

* 最终 `d1` 不含 `f`，`d2` 含 `f`
* 不应长期出现两边都看不到或两边都看到（允许极短暂抖动则要收敛）

---

### D04 snapshot 只读目录写入应失败（若支持快照）

**目的/坑**：只读快照被意外写入导致一致性破坏
**步骤**

1. 创建快照目录 `$T/snap`（只读）
2. 尝试 `touch "$T/snap/x"` 或修改已有文件
   **断言**

* 返回 `EROFS/EPERM` 等明确拒绝
* 原内容不变

---

## E. 符号链接（支持项）与路径解析“坑”

你文档里强调 `resolve_path_ex` 需要展开符号链接并有 `sym_count` 限制；这类问题经常外显成：某些工具进入死循环、或相对链接解析错误。

### E01 symlink 基础（文件/目录）

**步骤**

1. `echo hi > "$T/real"`
2. `ln -s real "$T/link"`
3. `cat "$T/link"`
   **断言**

* 输出 `hi`

### E02 相对 symlink + rename target

**步骤**

1. `mkdir "$T/dir"; echo a > "$T/dir/t"`
2. `ln -s t "$T/dir/l"`
3. `mv "$T/dir/t" "$T/dir/t2"`
4. `cat "$T/dir/l"`
   **断言**

* 应失败且 errno 为 `ENOENT`（相对链接不应自动跟随 rename）

### E03 symlink loop 检测

**步骤**

1. `ln -s b "$T/a"; ln -s a "$T/b"`
2. `cat "$T/a"` 或 `stat "$T/a"`
   **断言**

* 返回 `ELOOP`（或等价）

### E04 深层 symlink 链

**步骤**

* 构造 50 层链接（a1->a2->...->a50->real）
  **断言**
* 在你们设定的最大展开深度内成功，超过则 `ELOOP` 或明确失败

---

## F. 明确“不支持”的能力（要“失败得体”，否则上层会踩坑）

你文档里写了“只支持 Symlink，不支持 hardlink”。
这类用例的目标不是让它成功，而是要确保：**错误码稳定、不会导致目录/文件损坏、不会卡死**。

### F01 hardlink（ln）

**步骤**

1. `echo x > "$T/f"`
2. `ln "$T/f" "$T/f2"`
   **断言**

* 失败，errno 属于 `{EOPNOTSUPP, EPERM, ENOSYS}`（你们选一个稳定的）
* `$T/f` 仍可读，目录不出现半成品 `f2`

### F02 特殊文件（fifo / socket / mknod）

**步骤**

* `mkfifo "$T/p"`、`mknod`（可能需要 root）
  **断言**
* 不支持则明确失败，不应 crash 或卡住 mount

### F03 flock/posix lock（若未实现）

**步骤**

* python `fcntl.flock(fd, LOCK_EX|LOCK_NB)`
  **断言**
* 支持则正确互斥；不支持则明确错误码

---

## G. “build 场景”与“照片库场景”的回归/性能用例（很容易暴露 list/cache/合并逻辑问题）

文档把 build 作为核心场景：大量创建小文件、批量删除、目录 list 是最慢点并且是优化重点。
所以建议至少做两类：**正确性压力** + **性能基线**。

### G01 小文件风暴：创建→读取→删除

**步骤**

1. `mkdir "$T/build"`
2. 创建 N=100000 个小文件（1KB），文件名可散列到 256 个子目录降低单目录压力，也可以刻意全放一个目录专门测最坏情况
3. 随机抽样 1000 个文件校验 hash
4. `rm -rf "$T/build"`
   **断言**

* 创建/读取/删除全成功，无丢文件、无读错
* `rm -rf` 不出现“删不掉/残留幽灵项”

**建议指标（非硬断言，可做基线）**

* 创建吞吐（files/s）
* readdir 耗时（`time ls` 或 python os.scandir）
* 删除耗时

---

### G02 高并发 build：多进程创建同目录（最容易触发 rev/cache/冲突坑）

**步骤**

* 8~32 个进程同时在同一目录创建文件（名字避免冲突），同时另一个进程持续 `ls`/`find`
  **断言**
* 无 crash、无死锁
* 最终文件数正确（等于总创建数）

---

### G03 大目录 list 稳定性 + seekdir/telldir（若你用 python scandir 也能覆盖）

**目的/坑**：FUSE readdir 分页/游标实现不当会漏项或重复项
**步骤**

* 创建 50k 文件后，用 python `os.scandir` 迭代两遍，并对两次结果排序比对
  **断言**
* 两次结果集合一致（不漏不重）

---

### G04 “照片库式”深目录 + Unicode 文件名

**目的/坑**：路径解析、UTF-8、长路径、缓存命中
**步骤**

1. 创建多级目录：`$T/photos/2016/香港旅游/...`
2. 文件名包含中文、空格、emoji（可选）、超长名（接近 NAME_MAX）
3. 随机访问旧目录文件（模拟“多年前照片只读”）
   **断言**

* readdir 正确、open/read 正常、不会出现编码乱码/找不到文件

---

## H. 故障注入/稳定性（强烈建议 nightly）

### H01 写入中 kill -9（数据结构不应损坏）

**步骤**

1. 子进程循环写 `$T/f`（随机写/追加都测）
2. 主进程随机 `kill -9`
3. 之后重复打开/读取/覆盖写
   **断言**

* 文件系统仍可用（目录可 list、可创建新文件）
* `$T/f` 要么是旧内容、要么是部分新内容（取决于语义），但不能出现“读报错/无限阻塞/挂载异常”

### H02 重挂载一致性（如果你能方便 unmount/mount）

**步骤**

* 跑一轮 G01/G02 后，`umount $MNT` 再 mount
  **断言**
* 重挂载后数据一致、目录结构可遍历，无“孤儿条目”

---

## 可以直接照这个方式组织用例（推荐）

* **smoke（每次提交必跑，<2min）**：A01/A02/A03/A04/A05/A08/A10 + F01 + E01
* **regression（每天/每晚）**：A06/A07/A11/A14 + B01/B02/B03/B04 + E03/E04
* **stress（每周或大版本前）**：G01/G02/G03 + H01/H02 + C01/C02/C03 + D01/D02/D03/D04（如果有快照/物化工具）

---

## 最后一份“坑 → 用例”对照表（方便验收覆盖）

* **目录 list 慢/缓存不一致** → A10/A11/G03/G02/D03
* **rev 乐观锁/并发冲突** → A02/B02/G02
* **rename 需要更新两个 parent** → D03/A11
* **FileBuffer 分层读取撕裂/越界** → A06/B04/C02
* **物化与 reopen 写竞态（CAS）** → C01（重点）
* **Tombstone 掩盖 base children** → D01/D02
* **只支持 symlink 不支持 hardlink** → F01 + E 组
* **构建场景（小文件创建-删除）** → G01/G02
* **深目录/Unicode（照片库）** → G04


