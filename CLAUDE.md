# onyx-chunklet

独立的用户态 RAID / 块编排引擎（crate），借鉴 HPE 3PAR / Primera 的 chunklet 抽象，
为 onyx-storage 提供"灵活的混合 RAID 后端"。自己一个 git 仓库，独立构建、独立测试。

**当前定位**：与 metadb 同期，**完全独立于 onyx 主线**。Phase 8 之前不接 onyx 的写
路径，所有验收都靠自己的 `chunkletctl` / soak harness 在 sparse file + nvme-box
上完成。

## 构建与测试

```bash
cargo build
cargo test                    # unit + integration
cargo build --release
cargo test -- --ignored       # fault injection / 长跑，发布前必跑
```

测试覆盖率目标 90%+。`--ignored` 用例不是可选，是 phase gate。

## 架构脉络

抄 HPE 三层抽象，砍掉对 onyx 无用的部分（fast/slow chunklet、双控 owner/backup
owner、cage/magazine 物理拓扑）：

```
PhysicalDisk (PD)
  ↓ admit + 切片
Chunklet (固定 1 GiB, 落在某 PD 的某偏移)
  ↓ Allocator: 按 HA 域选 PD, 选 chunklet
LogicalDisk (LD): RAID 0/1/10/5/6 over chunklet set
  ↓ CPG: 声明式池, lazy 增长 LD
Volume / 上层 client (Phase 8: onyx BlockBackend)
```

LD 暴露给上层的形状 = **线性虚拟设备**（`read_at` / `write_at` / `block_size` /
`strip_size` / `capacity`）。RAID 编码、stripe 切分、degraded 处理全部在 LD 内部
完成，对调用者透明。

## 模块地图

| 模块 | 路径 | 职责 |
|------|------|------|
| types | `src/types.rs` | `PoolId` / `PdId` / `ChunkletId` / `LdId` / `CpgId` / `Lba` 等 typed newtype |
| error | `src/error.rs` | `ChunkletError` 统一错误 + `Result` 别名 |
| io | `src/io/` | `AlignedBuf`（sector-aligned）+ `RawDevice`（O_DIRECT 完整传输循环）|
| pd | `src/pd.rs` | `PhysicalDisk`：打开 raw block dev、属性探测、IO 分发 |
| superblock | `src/superblock.rs` | 每盘 superblock COW pair + crc + manifest 编解码 |
| bitmap | `src/bitmap.rs` | per-PD chunklet 状态表（free / used / spare / bad / migrating）|
| pool | `src/pool.rs` | `Pool`：注册 PD、init/scan/admit、跨盘 superblock 多数派裁决 |
| allocator | `src/allocator.rs` | 跨 PD 选 chunklet（HA 域约束 + balance + spare 预留）|
| ld/ | `src/ld/` | `LogicalDisk` trait + raid0/raid1/raid5/raid6 实现 + stripe 计算 |
| cpg | `src/cpg.rs` | `Cpg`：声明式池策略 + 按需扩 LD（Phase 7） |
| spare | `src/spare.rs` | spare 池 + rebuild 调度 + 多对多 worker |
| repair | `src/repair.rs` | scrub / parity verify / bad chunklet 隔离 |
| metrics | `src/metrics.rs` | 运行时计数器/延迟累计，供 soak 和诊断脚本读取 |
| testing | `src/testing/` | 故障注入点（IO error / partial write）+ sparse-file harness |
| bin | `src/bin/` | `chunkletctl`（CLI）+ `chunklet-soak`（standalone soak） |

## 关键不变式（动之前先读）

### 锁定的设计决策（不要回头讨论）

- **Chunklet size = 1 GiB 固定**。落在 superblock 的 `chunklet_size_log2 = 30`。改这个 = 数据迁移。
- **HA 域**：枚举有 `Pd / Numa / PcieSwitch`，**目前只实现 `Pd`**。其它返回 `Unsupported`，留着以后填。
- **Manifest 完全 on-disk**：每块 PD 头/尾各一份 superblock + bitmap，COW pair（`gen_a` / `gen_b` 双槽轮换）。**不依赖 metadb / 不依赖任何外部 KV**。这是 chunklet 站得住的根本——onyx 集成后 metadb 才能放心住在某个 chunklet LD 上而不形成循环依赖。
- **dm-style stack-on-top 不做**：chunklet 不暴露 ublk / 不做 device mapper。上层（Phase 8 的 onyx）通过 `BlockBackend` trait 直接调 LD。

### 锁序

- **PD IO 锁**：`PhysicalDisk` 内部不持锁，O_DIRECT pread/pwrite 由 OS 内核同步。多线程并发安全。
- **Pool manifest 锁**：`Pool::manifest_lock: Mutex<()>`。修改任意 PD superblock / 跨 PD allocator state 必须持有。读路径（`pool.find_chunklet(...)`）走 `RwLock<PoolState>::read()`，不需要 manifest lock。
- **LD lifecycle 锁**：每个 LD 一个 `RwLock<LdState>`。**写出 stripe 时只持 `read()`**（多 stripe 可并发），**rebuild / drain / 删除 LD 持 `write()`**。
- **Allocator 锁**：`Allocator::inner: Mutex<AllocatorInner>`。粒度全局——分配是低频路径，加锁可接受。绝不在持 allocator lock 时做 IO。
- **跨 PD 取锁**：必须按 `PdId` 升序，避免和 rebuild worker 死锁。

### Superblock COW pair（crash safety 的核心）

每块 PD 头部布局：

```
offset 0           : superblock slot A (4 KiB, header + manifest body + CRC32C)
offset 4 KiB       : superblock slot B (4 KiB, 同上)
offset 8 KiB       : chunklet bitmap + state table (变长, 按 chunklet 数对齐到 4 KiB)
offset 1 MiB       : 第一个 chunklet 起始
...
末尾 1 MiB         : tail mirror (slot A + slot B + bitmap 的镜像副本)
```

写顺序铁律：

1. 算新 manifest body → 写到当前 inactive slot（gen 较小那个）→ fsync
2. 读回校 CRC（trust-but-verify）
3. 同样写 tail mirror 的 inactive slot → fsync
4. 内存里 bump active slot
5. 下次写从新的 inactive 开始，确保任何时刻**至少一个完整副本**可读

恢复时四份 superblock（head A/B + tail A/B）各自校 CRC，挑出最高 gen 的有效 body
做权威。Pool::scan() 跨 PD 收集后多数派裁决（按 `pool_id` 一致 + `manifest_gen` 最高）。

### Chunklet bitmap 写入

- bitmap 改动**必须先**反映到 superblock 的 manifest body（`bitmap_crc32c` 字段），然后
  在同一个 COW slot 切换里一起 fsync。**不允许** bitmap 单独写。
- `mark_used` / `mark_free` / `mark_spare` 都是 `Pool::commit_alloc(...)` 的内部步
  骤，外部不得直接调。

### 启动顺序（admit / scan）

```
Pool::open(devices) -> {
  1. 并发打开所有 device 的 PD
  2. 并发读每盘 4 份 superblock，CRC 过滤
  3. 多数派裁决 pool_id (要求 >= ceil(N/2) + 1 一致)
  4. 跨 PD 收集所有 LD descriptor + chunklet 占用
  5. 检查每个 LD 的 set 内 chunklet 是否齐全 (degraded LD 标记 needs_rebuild)
  6. 重建内存 allocator state (free_per_pd, spare_per_pd)
  7. 返回 Pool 句柄；上层可枚举 LD / 创建新 LD
}
```

`admit` 单独操作：把空盘插进已有 pool，写新 PD 的 superblock，把 PD entry 加进所
有现有 PD 的 superblock manifest（COW pair 切换）。

### LD I/O 路径（必须严格遵守）

```rust
// LD 暴露给上层的接口
trait LogicalDisk {
    fn capacity_bytes(&self) -> u64;
    fn block_size(&self) -> usize;       // 通常 4096
    fn strip_size(&self) -> usize;       // RAID strip, 决定上层 packer 对齐
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()>;
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()>;
    fn write_vectored(&self, ops: &[(u64, &[u8])]) -> Result<()>;
}
```

写路径分两条：

- **full-stripe 写**（offset+len 整 stripe 对齐）：直接算 P / Q，并发下发到所有
  set 成员。零 RMW，零旧数据读取。R5/R6 的目标命中率 ≥ 95%（靠 onyx packer 的
  strip 对齐保证）。
- **partial RMW**：必须读旧 data + 旧 parity → 算 delta → 增量更新 parity → 写回。
  R5 的 P delta = `xor(new_data, old_data, old_p)`；R6 的 Q delta 走 GF(2⁸) `g^i ·
  (new_data ⊕ old_data) ⊕ old_q`。**不允许对 partial 写做 full-stripe 重算**——会
  污染 set 内不相关的 data。

读路径：

- 健康 set：直接从主 chunklet 读（mirror 选 latency 最低的副本）。
- degraded set：从存活 chunklet 重算缺失数据。R5 缺 1 = XOR；R6 缺 1 = XOR；R6 缺 2
  = 求解 P/Q 二元方程（用 `reed-solomon-erasure` 的 reconstruct）。

### Reed-Solomon 选型

- **full-stripe encode**：`reed-solomon-erasure` crate（开 `simd-accel` feature），
  适合 RAID-6（k+2，小 parity 数），benchmark 上比 `reed-solomon-simd` 在 ≤42
  recovery shards 场景更快。
- **partial RMW delta**：自己写。XOR 一行即可（带 SIMD intrinsics 是 phase 4 优
  化）；GF(2⁸) g^i 增量按 Anvin《The mathematics of RAID-6》算法照抄，约 200 行。
- **decode / reconstruct**：用 `reed-solomon-erasure` 的 `reconstruct(...)`，不要
  自己复现 Vandermonde 求逆。

### Sparing & rebuild

- 每盘按 `spare_pct`（默认 5%）保留 free chunklet 不发出去——这部分不参与 normal
  alloc，只在 rebuild 时用。
- spare 选取优先级（HPE 同款）：同 PD 类型 → 同 HA 域 → 同 NUMA → 全局 free。
- rebuild worker：N 个并发，每个锁一个 chunklet（`LdState::write` 局部模式），从
  set 内存活成员重算数据，写到 spare，allocator commit 把 victim chunklet 标
  `bad`、spare 标 `used`。**单次 commit 必须原子**（superblock COW pair 切一次）。
- 背压：rebuild rate 受 user IO 限制（看 PD 的 `iostat` `%util`，>80% 暂停）。
- crash mid-rebuild：通过 chunklet `generation` 字段判断，rebuild 写入 `gen+1`，旧
  数据 `gen`，启动时 chunklet header gen < superblock 记录的 target gen → 重新
  rebuild 这个 chunklet。

### Chunklet header

每个 chunklet 前 4 KiB 是 chunklet header（不是用户数据）：

```
magic(8) | chunklet_id(8) | owner_ld_uuid(16) | role(1) | generation(4) | reserved | crc32c(4)
```

role: 0=data, 1=parity-p, 2=parity-q, 3=spare, 4=bad
generation: 每次 rebuild / 重新分配 +1

LD 暴露的容量 = `chunklet_size - 4096`（4 KiB header overhead）。

### 不变式：set 内 PD 唯一性

allocator **必须**保证一个 LD 的同一 set 内的 chunklet 落在不同 PD 上。这是
RAID 容错的根。违反 = 一个 PD 失败可能干掉整个 set。每次 allocator commit 之
前 `debug_assert` 校验。

## 测试基础设施

- **sparse file backend**：`PhysicalDisk::open_file_for_test(path, size)`。Phase 0~5
  全程在 10 个 sparse file 上跑（10 × 80 GiB ≈ 800 GiB，单机 ext4 上够用）。
- **fault injection**：`testing::fault::*`：partial write、IO error、torn write、
  superblock crc 翻转、chunklet header gen 倒退。每条都有对应 ignored 测试。
- **真盘验收**：Phase 6+ 上 nvme-box（10 块 NVMe），soak ≥ 7×24h，禁止用 Python
  harness 做性能判断。

## Phase 路线 (gate clear)

| Phase | 关键产出 | 退出门 |
|-------|----------|--------|
| P0 | crate 骨架、PD/Pool、superblock COW pair、CLI scan/init/admit | sparse file 10 盘 round-trip + 模拟断电恢复 |
| P1 | Allocator、LdPlain (RAID-0) | 创建 RAID-0 LD、读写、重启状态一致 |
| P2 | LdMirror (R1/R10) + 副本读策略 | 拔盘读写不停、replace 自愈 |
| P3 | LdRaid5（full-stripe + partial RMW + degraded read） | 单盘 fail rebuild 数据一致，fio 24h CRC 零错 |
| P4 | LdRaid6（RS encode + 增量 P/Q delta + 双盘 degraded） | 双盘 fail 重建一致；scrub 检出注入 bit-rot |
| P5 | spare 池 + 多对多 rebuild | rebuild 时间 ≤ 单盘 1/N + 30% |
| P6 | scrub / parity verify / bad chunklet 隔离 | 周期 scrub 24h 无误报，注入错误必检出 |
| P7 | CPG（声明式策略） + add/drain disk + 跨盘 rebalance | 加盘后 chunklet 分布方差收敛；drain 不停 IO |
| P7.5 | standalone soak（nvme-box 7×24h，故障注入） | 无 silent corruption / 无 panic / 重启状态一致 |
| **P8** | **onyx 集成（HOLD）** | 仅在 P7.5 通过且用户确认后开工 |

## 代码风格

- **单文件不超过 1000 行**。接近上限就拆子模块（例如 `superblock.rs` → `superblock/mod.rs` + `superblock/codec.rs` + `superblock/cow.rs`）。模块边界按职责分，不要按"凑长度"切。
- 私有 helper 不加 doc comment，除非 WHY 不显然（锁序、fault-injection hook、不变式）。
- 模块顶部的 `//!` 说明**责任 + 并发模型**，别写"这个模块做 X"。
- 不要轻易引入新 crate。当前依赖：`parking_lot` / `crc32c` / `crossbeam-channel` /
  `nix` / `libc` / `uuid` / `thiserror` / `tracing` / `rand` /
  `reed-solomon-erasure`（feature `simd-accel`）/ `tempfile`（test）/ `proptest`（test）/
  `clap`（bin）。**不**引入 `bincode` / `serde`：on-disk 格式全部手写编码（superblock
  / bitmap / chunklet header），固定布局、版本字段显式管。
- `unsafe` 需要写原因注释。预计只在 `AlignedBuf` / SIMD intrinsics 几处。
- 禁止为了让测试过去绕过校验（关 assert、放宽 invariant check）。

## 和 onyx-storage 的关系

- onyx-storage 在 `/root/onyx_storage`。**Phase 8 之前 chunklet 与 onyx 零依赖**——
  不在 onyx workspace 里、不被 onyx Cargo.toml 引用、不读 onyx 配置、不写 onyx 的
  metadb 任何 namespace。
- Phase 8 才加 onyx 侧的 `BlockBackend` trait + `ChunkletBackend` adapter，
  把 onyx 的 LV3 退役。**这一步必须先与用户确认再动手**——onyx 主线在
  并发推进 buffer / metadb 集成相关的修改。
- 父项目的 CLAUDE.md 讲 ublk / buffer / packer / GC / dedup pipeline，与 chunklet
  内部约束不重叠。切 `cd /root/onyx_storage` 工作时读那边的 CLAUDE.md。

## 提交规范

- 提交不要带 Claude 署名（参照 onyx 主仓库 feedback memory）。
- **commit message 用英文**，开头标 phase（如 `[P1] add allocator + LdPlain`）。
  正文也英文，描述改了什么 + 为什么 + 验收门。
- 每个 phase 一个独立 PR / 独立 commit。
- on-disk 格式变更必须 bump `SUPERBLOCK_VERSION`，并在 PR 描述里写明迁移策略
  （Phase 0~7 期间：no migration, 重做 pool）。
