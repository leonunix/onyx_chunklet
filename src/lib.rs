//! onyx-chunklet
//!
//! 用户态 RAID / 块编排引擎，借鉴 HPE 3PAR / Primera 的 chunklet 抽象。
//!
//! # 并发模型
//!
//! - `PhysicalDisk` 内部无锁，O_DIRECT pread/pwrite 由内核同步。
//! - `Pool` 持有 `manifest_lock: Mutex<()>` 保护跨 PD 的 manifest 提交；读路径走
//!   `RwLock<PoolState>::read()`。
//! - `LogicalDisk` 持有 `RwLock<LdState>`：写 stripe 持 read（多 stripe 并发），
//!   rebuild / 删除持 write。
//! - 跨 PD 取锁必须按 `PdId` 升序；跨 LD 取锁必须按 `LdId` 升序。
//!
//! # Phase 0 范围
//!
//! 目前只覆盖 PD/Pool/superblock COW pair。Allocator / LD / CPG 在后续 phase。

pub mod allocator;
pub mod bitmap;
pub mod chunklet;
pub mod error;
pub mod io;
pub mod ld;
pub mod pd;
pub mod pool;
pub mod superblock;
pub mod types;

pub use error::{ChunkletError, ChunkletResult};
pub use ld::{LdDescriptor, LdList, LdPlain, LogicalDisk};
pub use pd::PhysicalDisk;
pub use pool::{Pool, PoolConfig};
pub use types::{
    ChunkletId, CpgId, HaDomain, LdId, LdMember, LdRole, PdId, PoolId, RaidLevel,
};
