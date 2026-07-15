//! Physical disk: per-PD on-disk state + manifest COW commit.
//!
//! # Concurrency
//!
//! - `PhysicalDisk::commit_manifest` is the only writer of superblock + bitmap;
//!   `Pool` serializes commits via its own `manifest_lock`. Internally we hold
//!   `state.write()` across the full commit (bitmap fsync + superblock fsync)
//!   so readers see a consistent (gen, body, bitmap) tuple.
//! - User-data IO (`read_chunklet_user` / `write_chunklet_user`) takes
//!   `state.read()` only to look up `total_chunklets`; the actual `RawDevice`
//!   pread/pwrite is lock-free.
//!
//! # Layout (head 1 MiB, mirrored at tail 1 MiB)
//!
//! ```text
//! [0..4 KiB)        superblock slot A
//! [4 KiB..8 KiB)    superblock slot B
//! [8 KiB..264 KiB)  bitmap slot A
//! [264 KiB..520 KiB) bitmap slot B
//! [520 KiB..1 MiB)  reserved
//! ```
//!
//! Tail mirror: identical layout starting at `pd_size - 1 MiB`.

use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::bitmap::Bitmap;
use crate::error::{ChunkletError, ChunkletResult};
use crate::io::backend::make_backend;
use crate::io::{AlignedBuf, IoBackend, IoBackendKind, RawDevice};
use crate::superblock::{slot_len_from_header, SuperblockBody, SuperblockSlot};
use crate::types::{
    PdId, PoolId, BITMAP_SLOT_A_OFFSET, BITMAP_SLOT_BYTES, BITMAP_SLOT_B_OFFSET, BLOCK_SIZE,
    CHUNKLET_HEADER_BYTES, CHUNKLET_SIZE, MAX_CHUNKLETS_PER_PD, PD_RESERVED_BYTES,
    SUPERBLOCK_SLOT_A_OFFSET, SUPERBLOCK_SLOT_B_OFFSET,
};

pub struct PhysicalDisk {
    raw: RawDevice,
    state: RwLock<PdState>,
    numa_node: parking_lot::RwLock<Option<u16>>,
    /// Pool-wide backend for fan-out writes. Set at PD construction
    /// (Pool::create / open / open_with_missing / admit) and cloned by
    /// `parallel_strip_writes` from any PD in the batch.
    backend: parking_lot::RwLock<std::sync::Arc<dyn crate::io::IoBackend>>,
    /// Test/diagnostic hook: when set, every `read_chunklet_user` on this PD
    /// returns a `Device` error, simulating a member whose reads fault while the
    /// LD still believes it healthy (a link glitch / dying disk in the window
    /// before isolation). Installed at runtime like [`Self::set_backend`] (NOT
    /// `cfg(test)`, so integration fault-injection tests can drive the
    /// reconstruct-on-read path uniformly — the fast read, sibling reads, and
    /// RAID5/6 parity survivor reads all funnel through `read_chunklet_user`).
    /// Default false; never consulted on the write path.
    read_faulting: std::sync::atomic::AtomicBool,
}

struct PdState {
    pool_id: PoolId,
    pd_id: PdId,
    pd_seq_in_pool: u32,
    total_chunklets: u32,
    manifest_gen: u64,
    body: SuperblockBody,
    bitmap: Bitmap,
    /// Which superblock slot (0 = A, 1 = B) currently holds the live state.
    active_sb_slot: u8,
    /// Which bitmap slot (0 = A, 1 = B) currently holds the live state.
    /// Equals `body.bitmap_slot_id`.
    active_bitmap_slot: u8,
}

#[derive(Clone, Debug)]
pub struct PdInfo {
    pub pool_id: PoolId,
    pub pd_id: PdId,
    pub pd_seq_in_pool: u32,
    pub numa_node: Option<u16>,
    pub total_chunklets: u32,
    pub manifest_gen: u64,
    pub size_bytes: u64,
    pub path: std::path::PathBuf,
}

impl PhysicalDisk {
    /// Initialize a blank PD with a fresh manifest at `gen = 1`, slots A.
    ///
    /// `ld_list_bytes` and `cpg_list_bytes` are the encoded `LdList` /
    /// `CpgList` the new PD inherits. Pool::create passes empty lists;
    /// Pool::admit passes the current pool-wide LD + CPG list so the new
    /// PD's manifest stays consistent with its peers.
    pub fn init(
        raw: RawDevice,
        pool_id: PoolId,
        pd_id: PdId,
        pd_seq_in_pool: u32,
        pool_pd_count: u32,
        pd_list: Vec<crate::superblock::PoolPdEntry>,
        spare_pct: u8,
        ld_list_bytes: Vec<u8>,
        cpg_list_bytes: Vec<u8>,
    ) -> ChunkletResult<Arc<Self>> {
        let pd_size = raw.size();
        let total_chunklets = compute_total_chunklets(pd_size)?;

        let mut bitmap = Bitmap::new(total_chunklets)?;
        // Reserve the configured spare_pct as Spare chunklets up front.
        // Phase 5 will manage spare promotion / replacement; for P0 we just
        // reflect the policy in the bitmap so capacity reporting is honest.
        let spare_n = compute_spare_count(total_chunklets, spare_pct);
        for i in 0..spare_n {
            bitmap.set(i, crate::types::ChunkletState::Spare)?;
        }

        let mut body = SuperblockBody::new_empty(
            pd_size / BLOCK_SIZE,
            (PD_RESERVED_BYTES / BLOCK_SIZE) as u32,
            (PD_RESERVED_BYTES / BLOCK_SIZE) as u32,
            total_chunklets,
            pd_seq_in_pool,
            pool_pd_count,
            spare_pct,
        );
        body.pd_list = pd_list;
        body.ld_list_bytes = ld_list_bytes;
        body.cpg_list_bytes = cpg_list_bytes;
        body.bitmap_slot_id = 0;
        body.bitmap_crc32c = bitmap.crc32c();

        let state = PdState {
            pool_id,
            pd_id,
            pd_seq_in_pool,
            total_chunklets,
            manifest_gen: 1,
            body,
            bitmap,
            active_sb_slot: 0,
            active_bitmap_slot: 0,
        };

        let numa_node = crate::numa::detect_pd_node(raw.path());
        let pd = Arc::new(Self {
            raw,
            state: RwLock::new(state),
            numa_node: parking_lot::RwLock::new(numa_node),
            backend: parking_lot::RwLock::new(default_backend()),
            read_faulting: std::sync::atomic::AtomicBool::new(false),
        });
        // Write initial state to slot A (head + tail).
        pd.write_initial()?;
        Ok(pd)
    }

    /// Open an existing PD, reading all four superblock slots and choosing
    /// the highest-gen valid one as authoritative.
    pub fn open(raw: RawDevice) -> ChunkletResult<Arc<Self>> {
        let pd_size = raw.size();
        let mut candidates: Vec<(u64, SuperblockSlot, u8)> = Vec::with_capacity(4);

        // Read all 4 superblock slots; collect the valid ones.
        for &(offset, slot_id) in &slot_offsets(pd_size).superblock_slots {
            match read_superblock_slot(&raw, offset) {
                Ok(slot) => candidates.push((slot.manifest_gen, slot, slot_id)),
                Err(_) => continue,
            }
        }

        if candidates.is_empty() {
            return Err(ChunkletError::NoValidSuperblock {
                path: raw.path().to_path_buf(),
            });
        }

        // Pick the slot with the highest manifest_gen. Tie-break by slot_id (head A first).
        candidates.sort_by_key(|(gen, _, slot_id)| (std::cmp::Reverse(*gen), *slot_id));
        let (_, slot, active_sb_slot) = candidates.into_iter().next().unwrap();

        // Read the bitmap region pointed to by this slot.
        let bitmap = read_bitmap(&raw, &slot.body)?;

        let state = PdState {
            pool_id: slot.pool_id,
            pd_id: slot.pd_id,
            pd_seq_in_pool: slot.body.pd_seq_in_pool,
            total_chunklets: slot.body.total_chunklets,
            manifest_gen: slot.manifest_gen,
            active_bitmap_slot: slot.body.bitmap_slot_id,
            body: slot.body,
            bitmap,
            active_sb_slot,
        };

        let numa_node = crate::numa::detect_pd_node(raw.path());
        Ok(Arc::new(Self {
            raw,
            state: RwLock::new(state),
            numa_node: parking_lot::RwLock::new(numa_node),
            backend: parking_lot::RwLock::new(default_backend()),
            read_faulting: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    /// Replace the IO backend in use by this PD. Pool calls this on every
    /// PD it owns right after open/init, so all PDs in a pool share one
    /// backend Arc.
    pub fn set_backend(&self, b: std::sync::Arc<dyn crate::io::IoBackend>) {
        b.register_pd(self.pd_id());
        *self.backend.write() = b;
    }

    /// Current IO backend (cheap clone — `Arc<dyn IoBackend>`).
    pub fn backend(&self) -> std::sync::Arc<dyn crate::io::IoBackend> {
        self.backend.read().clone()
    }

    /// Install (or clear) the per-PD read-fault hook (see the `read_faulting`
    /// field). While set, every `read_chunklet_user` on this PD returns a
    /// `Device` error, simulating a member whose reads EIO in the window before
    /// isolation. Used by fault-injection tests to exercise reconstruct-on-read;
    /// not consulted on the write path.
    pub fn set_read_faulting(&self, on: bool) {
        self.read_faulting
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn numa_node(&self) -> Option<u16> {
        *self.numa_node.read()
    }

    /// Raw fd of the underlying device. Used by direct-fd IO submission
    /// paths (currently only `UringBackend`) that bypass `RawDevice`'s
    /// `read_at`/`write_at` loops, so the caller is responsible for
    /// alignment + bounds checking. `pub(crate)` keeps the fd inside
    /// chunklet — downstream crates have no business grabbing it.
    pub(crate) fn raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.raw.as_raw_fd()
    }

    /// Whether completed O_DIRECT writes still need an explicit device-cache
    /// flush before they are durable.
    pub(crate) fn sync_required(&self) -> bool {
        self.raw.sync_required()
    }

    pub fn pool_id(&self) -> PoolId {
        self.state.read().pool_id
    }

    pub fn pd_id(&self) -> PdId {
        self.state.read().pd_id
    }

    pub fn manifest_gen(&self) -> u64 {
        self.state.read().manifest_gen
    }

    pub fn total_chunklets(&self) -> u32 {
        self.state.read().total_chunklets
    }

    pub fn size_bytes(&self) -> u64 {
        self.raw.size()
    }

    pub fn path(&self) -> &Path {
        self.raw.path()
    }

    pub fn info(&self) -> PdInfo {
        let s = self.state.read();
        PdInfo {
            pool_id: s.pool_id,
            pd_id: s.pd_id,
            pd_seq_in_pool: s.pd_seq_in_pool,
            numa_node: *self.numa_node.read(),
            total_chunklets: s.total_chunklets,
            manifest_gen: s.manifest_gen,
            size_bytes: self.raw.size(),
            path: self.raw.path().to_path_buf(),
        }
    }

    /// Snapshot of the current body + bitmap (cheap clones; tests + Pool::scan).
    pub fn snapshot(&self) -> (SuperblockBody, Bitmap, u64) {
        let s = self.state.read();
        (s.body.clone(), s.bitmap.clone(), s.manifest_gen)
    }

    /// Apply a manifest mutation atomically. The closure may mutate the body
    /// (PD list, LD list bytes, etc.) and the bitmap; the new manifest is
    /// committed to disk via the inactive slot pair.
    ///
    /// Caller is responsible for upholding cross-PD invariants (Pool's
    /// `manifest_lock` serializes commits across PDs).
    pub fn commit_manifest<F>(&self, mutate: F) -> ChunkletResult<u64>
    where
        F: FnOnce(&mut SuperblockBody, &mut Bitmap) -> ChunkletResult<()>,
    {
        let mut s = self.state.write();

        // Pick inactive slot ids.
        let new_sb_slot = 1 - s.active_sb_slot;
        let new_bitmap_slot = 1 - s.active_bitmap_slot;
        let new_gen = s.manifest_gen + 1;

        // Apply caller-supplied mutation to clones first. If any IO below
        // fails, the in-memory state remains aligned with the active on-disk
        // generation instead of reflecting an uncommitted future manifest.
        let mut new_body = s.body.clone();
        let mut new_bitmap = s.bitmap.clone();
        mutate(&mut new_body, &mut new_bitmap)?;

        // Update body to reflect the new bitmap slot + crc.
        new_body.bitmap_slot_id = new_bitmap_slot;
        new_body.bitmap_crc32c = new_bitmap.crc32c();

        // Step 1: write the new bitmap to head + tail (inactive slot).
        let bitmap_bytes = new_bitmap.encode();
        let bitmap_offset = bitmap_slot_offset(new_bitmap_slot);
        let layout = slot_offsets(self.raw.size());
        self.raw
            .write_at(&bitmap_bytes, layout.head_base + bitmap_offset)?;
        self.raw
            .write_at(&bitmap_bytes, layout.tail_base + bitmap_offset)?;
        self.raw.sync()?;

        // Step 2: encode + write the new superblock to head + tail.
        let slot = SuperblockSlot {
            pool_id: s.pool_id,
            pd_id: s.pd_id,
            manifest_gen: new_gen,
            body: new_body.clone(),
        };
        let sb_bytes = slot.encode()?;
        let sb_offset = superblock_slot_offset(new_sb_slot);
        self.raw.write_at(&sb_bytes, layout.head_base + sb_offset)?;
        self.raw.write_at(&sb_bytes, layout.tail_base + sb_offset)?;
        self.raw.sync()?;

        // Step 3: bump in-memory pointers. Only after both fsyncs succeed.
        // Keep the top-level `pd_seq_in_pool` mirror in lock-step with the
        // committed body: `retire_failed_pd` re-denses seqs by mutating
        // `body.pd_seq_in_pool` in the closure, and `info()` reads this mirror,
        // so it must track the body or a re-densed PD would report a stale seq
        // until the next `open` (nothing mutated it before, so this is a no-op
        // for every existing caller).
        s.pd_seq_in_pool = new_body.pd_seq_in_pool;
        s.body = new_body;
        s.bitmap = new_bitmap;
        s.manifest_gen = new_gen;
        s.active_sb_slot = new_sb_slot;
        s.active_bitmap_slot = new_bitmap_slot;
        Ok(new_gen)
    }

    /// Initial write of the freshly-built manifest. Writes both slot A copies
    /// (head + tail), then for safety copies them into slot B as well so all
    /// 4 slots are valid at gen 1 — recovery on a half-initialized PD just
    /// reads gen 1 from any slot.
    fn write_initial(&self) -> ChunkletResult<()> {
        let s = self.state.read();
        let layout = slot_offsets(self.raw.size());

        let bitmap_bytes = s.bitmap.encode();
        // Both bitmap slots get the same content at init.
        self.raw
            .write_at(&bitmap_bytes, layout.head_base + bitmap_slot_offset(0))?;
        self.raw
            .write_at(&bitmap_bytes, layout.head_base + bitmap_slot_offset(1))?;
        self.raw
            .write_at(&bitmap_bytes, layout.tail_base + bitmap_slot_offset(0))?;
        self.raw
            .write_at(&bitmap_bytes, layout.tail_base + bitmap_slot_offset(1))?;
        self.raw.sync()?;

        let slot = SuperblockSlot {
            pool_id: s.pool_id,
            pd_id: s.pd_id,
            manifest_gen: s.manifest_gen,
            body: s.body.clone(),
        };
        let sb_bytes = slot.encode()?;
        self.raw
            .write_at(&sb_bytes, layout.head_base + superblock_slot_offset(0))?;
        self.raw
            .write_at(&sb_bytes, layout.head_base + superblock_slot_offset(1))?;
        self.raw
            .write_at(&sb_bytes, layout.tail_base + superblock_slot_offset(0))?;
        self.raw
            .write_at(&sb_bytes, layout.tail_base + superblock_slot_offset(1))?;
        self.raw.sync()?;
        Ok(())
    }

    /// Read user data from a chunklet. `offset` is relative to the start of
    /// the user region (header excluded).
    pub fn read_chunklet_user(
        &self,
        chunklet_index: u32,
        offset: u64,
        buf: &mut [u8],
    ) -> ChunkletResult<()> {
        self.bind_current_to_local_node();
        self.read_chunklet_user_unbound(chunklet_index, offset, buf)
    }

    /// Caller-thread fallback for an execution context whose affinity is
    /// already owned by the upper scheduler.
    pub(crate) fn read_chunklet_user_unbound(
        &self,
        chunklet_index: u32,
        offset: u64,
        buf: &mut [u8],
    ) -> ChunkletResult<()> {
        if self
            .read_faulting
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(ChunkletError::Device {
                path: self.raw.path().to_path_buf(),
                reason: "injected read fault (set_read_faulting)".into(),
            });
        }
        let abs = self.chunklet_user_abs_offset(chunklet_index, offset, buf.len() as u64)?;
        self.raw.read_at(buf, abs)
    }

    pub fn write_chunklet_user(
        &self,
        chunklet_index: u32,
        offset: u64,
        buf: &[u8],
    ) -> ChunkletResult<()> {
        self.bind_current_to_local_node();
        self.write_chunklet_user_unbound(chunklet_index, offset, buf)
    }

    /// Caller-thread fallback that preserves the scheduler worker's affinity.
    pub(crate) fn write_chunklet_user_unbound(
        &self,
        chunklet_index: u32,
        offset: u64,
        buf: &[u8],
    ) -> ChunkletResult<()> {
        let abs = self.chunklet_user_abs_offset(chunklet_index, offset, buf.len() as u64)?;
        self.raw.write_at(buf, abs)
    }

    /// Write the on-disk chunklet header (4 KiB). Caller must `sync()` after
    /// any batch of header writes for durability.
    pub fn write_chunklet_header(
        &self,
        chunklet_index: u32,
        header_bytes: &[u8; CHUNKLET_HEADER_BYTES as usize],
    ) -> ChunkletResult<()> {
        let total = self.state.read().total_chunklets;
        if chunklet_index >= total {
            return Err(ChunkletError::Invariant(format!(
                "chunklet index {} >= total {}",
                chunklet_index, total
            )));
        }
        let chunklet_base = PD_RESERVED_BYTES + (chunklet_index as u64) * CHUNKLET_SIZE;
        self.raw.write_at(header_bytes, chunklet_base)
    }

    /// Read the raw chunklet header bytes (4 KiB).
    pub fn read_chunklet_header_bytes(
        &self,
        chunklet_index: u32,
    ) -> ChunkletResult<[u8; CHUNKLET_HEADER_BYTES as usize]> {
        let total = self.state.read().total_chunklets;
        if chunklet_index >= total {
            return Err(ChunkletError::Invariant(format!(
                "chunklet index {} >= total {}",
                chunklet_index, total
            )));
        }
        let chunklet_base = PD_RESERVED_BYTES + (chunklet_index as u64) * CHUNKLET_SIZE;
        let mut buf = [0u8; CHUNKLET_HEADER_BYTES as usize];
        // The chunklet header is exactly one 4 KiB block — aligned for O_DIRECT.
        self.raw.read_at(&mut buf, chunklet_base)?;
        Ok(buf)
    }

    pub fn sync(&self) -> ChunkletResult<()> {
        self.raw.sync()
    }

    /// Cheap liveness probe: a single aligned read of the head superblock
    /// region via the raw device. Returns `true` if the read syscall
    /// succeeds (the device answers IO), `false` on any IO error (disk
    /// pulled → EIO / ENODEV). Deliberately does NOT decode or CRC-check
    /// the bytes — a torn slot mid-`commit_manifest` still counts as alive;
    /// the only question is whether the device responds. Lock-free
    /// (`RawDevice` IO takes no PD lock), so a health watchdog can poll it
    /// concurrently with normal IO. O_DIRECT means a pulled disk fails here
    /// instead of being masked by the page cache.
    pub fn probe_liveness(&self) -> bool {
        match AlignedBuf::new(BLOCK_SIZE as usize) {
            Ok(mut buf) => self.raw.read_at(buf.as_mut_slice(), 0).is_ok(),
            Err(_) => false,
        }
    }

    fn bind_current_to_local_node(&self) {
        crate::numa::bind_current_to_node(*self.numa_node.read());
    }

    /// Resolve a (chunklet, in-chunklet offset, length) into an absolute
    /// device offset, validating bounds. `pub(crate)` so direct-fd IO
    /// submission paths can bypass `RawDevice`'s read/write loops while
    /// still going through the same offset math.
    pub(crate) fn chunklet_user_abs_offset(
        &self,
        chunklet_index: u32,
        offset: u64,
        len: u64,
    ) -> ChunkletResult<u64> {
        let total = self.state.read().total_chunklets;
        if chunklet_index >= total {
            return Err(ChunkletError::Invariant(format!(
                "chunklet index {} >= total {}",
                chunklet_index, total
            )));
        }
        let user_capacity = CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES;
        if offset
            .checked_add(len)
            .map_or(true, |end| end > user_capacity)
        {
            return Err(ChunkletError::Invariant(format!(
                "user IO out of chunklet bounds: offset={} len={} cap={}",
                offset, len, user_capacity
            )));
        }
        let chunklet_base = PD_RESERVED_BYTES + (chunklet_index as u64) * CHUNKLET_SIZE;
        Ok(chunklet_base + CHUNKLET_HEADER_BYTES + offset)
    }
}

/// Compute total chunklets that fit on a PD given its raw size.
/// Default backend stamped on every `PhysicalDisk` at construction
/// time. `Pool::create` overwrites with the pool's configured backend
/// right after; standalone PD tests + ad-hoc `PhysicalDisk::open` use
/// this default. Single source of truth lives in `make_backend` so a
/// future change to `IoBackendKind::default()` propagates here.
fn default_backend() -> Arc<dyn IoBackend> {
    make_backend(IoBackendKind::default())
}

fn compute_total_chunklets(pd_size: u64) -> ChunkletResult<u32> {
    if pd_size < 4 * PD_RESERVED_BYTES {
        return Err(ChunkletError::Config(format!(
            "PD too small: {} bytes (need at least {})",
            pd_size,
            4 * PD_RESERVED_BYTES
        )));
    }
    let usable = pd_size - 2 * PD_RESERVED_BYTES;
    let total = usable / CHUNKLET_SIZE;
    if total == 0 {
        return Err(ChunkletError::Config(format!(
            "PD has no room for any chunklet: usable={} bytes (need >= {})",
            usable, CHUNKLET_SIZE
        )));
    }
    if total > MAX_CHUNKLETS_PER_PD as u64 {
        return Err(ChunkletError::Config(format!(
            "PD too large: would need {} chunklets, max {}",
            total, MAX_CHUNKLETS_PER_PD
        )));
    }
    Ok(total as u32)
}

fn compute_spare_count(total: u32, spare_pct: u8) -> u32 {
    let spare = ((total as u64) * (spare_pct as u64) + 99) / 100;
    spare.min(total as u64) as u32
}

/// Initial read size for a superblock slot: one read covers the 64-byte header
/// plus any realistic manifest body, so the common path is a single IO. A body
/// larger than this (heavy post-rebuild fragmentation) triggers exactly one
/// exact-length follow-up read.
const INITIAL_SLOT_READ: usize = 64 * 1024;

/// Read + decode one superblock slot at `offset`. The slot occupies only the
/// used, block-aligned prefix of its 32 MiB reserved region, so the read is
/// sized from the header's advertised body length rather than the full capacity.
pub fn read_superblock_slot(raw: &RawDevice, offset: u64) -> ChunkletResult<SuperblockSlot> {
    let mut buf = AlignedBuf::new(INITIAL_SLOT_READ)?;
    raw.read_at(buf.as_mut_slice(), offset)?;
    let needed = slot_len_from_header(buf.as_slice())?;
    if needed <= INITIAL_SLOT_READ {
        SuperblockSlot::decode(buf.as_slice())
    } else {
        let mut big = AlignedBuf::new(needed)?;
        raw.read_at(big.as_mut_slice(), offset)?;
        SuperblockSlot::decode(big.as_slice())
    }
}

fn superblock_slot_offset(slot_id: u8) -> u64 {
    match slot_id {
        0 => SUPERBLOCK_SLOT_A_OFFSET,
        1 => SUPERBLOCK_SLOT_B_OFFSET,
        _ => unreachable!("slot_id checked at decode"),
    }
}

fn bitmap_slot_offset(slot_id: u8) -> u64 {
    match slot_id {
        0 => BITMAP_SLOT_A_OFFSET,
        1 => BITMAP_SLOT_B_OFFSET,
        _ => unreachable!("slot_id checked at decode"),
    }
}

struct SlotOffsets {
    head_base: u64,
    tail_base: u64,
    /// (offset within base, slot_id) for each of the 4 superblock slots:
    /// head A, head B, tail A, tail B.
    superblock_slots: [(u64, u8); 4],
}

fn slot_offsets(pd_size: u64) -> SlotOffsets {
    let head_base = 0u64;
    let tail_base = pd_size - PD_RESERVED_BYTES;
    SlotOffsets {
        head_base,
        tail_base,
        superblock_slots: [
            (head_base + SUPERBLOCK_SLOT_A_OFFSET, 0),
            (head_base + SUPERBLOCK_SLOT_B_OFFSET, 1),
            (tail_base + SUPERBLOCK_SLOT_A_OFFSET, 0),
            (tail_base + SUPERBLOCK_SLOT_B_OFFSET, 1),
        ],
    }
}

fn read_bitmap(raw: &RawDevice, body: &SuperblockBody) -> ChunkletResult<Bitmap> {
    let pd_size = raw.size();
    let layout = slot_offsets(pd_size);
    let bitmap_offset = bitmap_slot_offset(body.bitmap_slot_id);

    // Try head first, then tail.
    for &base in &[layout.head_base, layout.tail_base] {
        let mut buf = AlignedBuf::new(BITMAP_SLOT_BYTES as usize)?;
        if raw
            .read_at(buf.as_mut_slice(), base + bitmap_offset)
            .is_err()
        {
            continue;
        }
        let computed = crc32c::crc32c(buf.as_slice());
        if computed != body.bitmap_crc32c {
            tracing::warn!(
                "bitmap crc mismatch at base={} stored={:08x} computed={:08x}",
                base,
                body.bitmap_crc32c,
                computed
            );
            continue;
        }
        return Bitmap::decode(buf.as_slice(), body.total_chunklets);
    }

    Err(ChunkletError::Crc {
        what: "bitmap (no valid mirror)".into(),
        stored: body.bitmap_crc32c,
        computed: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChunkletState, PoolId};
    use tempfile::TempDir;

    fn sparse_pd(dir: &TempDir, name: &str, size: u64) -> RawDevice {
        let path = dir.path().join(name);
        RawDevice::open_or_create(&path, size).unwrap()
    }

    /// 4 GiB sparse file: 4 GiB - 2 MiB reserved = ~4094 MiB / 1 GiB = 3 chunklets.
    const TEST_PD_SIZE: u64 = 4 * 1024 * 1024 * 1024;

    #[test]
    fn init_then_open_round_trip() {
        let dir = TempDir::new().unwrap();
        let pool_id = PoolId::new_v4();
        let pd_id = PdId::new_v4();
        let raw = sparse_pd(&dir, "pd0", TEST_PD_SIZE);
        let path = raw.path().to_path_buf();

        let pd = PhysicalDisk::init(raw, pool_id, pd_id, 0, 1, vec![], 5, vec![], vec![]).unwrap();
        let info = pd.info();
        assert_eq!(info.pool_id, pool_id);
        assert_eq!(info.pd_id, pd_id);
        assert_eq!(info.manifest_gen, 1);
        assert!(info.total_chunklets >= 3);
        drop(pd);

        let raw2 = RawDevice::open(&path).unwrap();
        let pd2 = PhysicalDisk::open(raw2).unwrap();
        let info2 = pd2.info();
        assert_eq!(info2.pool_id, pool_id);
        assert_eq!(info2.pd_id, pd_id);
        assert_eq!(info2.manifest_gen, 1);
    }

    #[test]
    fn commit_bumps_gen_and_alternates_slots() {
        let dir = TempDir::new().unwrap();
        let raw = sparse_pd(&dir, "pd0", TEST_PD_SIZE);
        let path = raw.path().to_path_buf();
        let pd = PhysicalDisk::init(
            raw,
            PoolId::new_v4(),
            PdId::new_v4(),
            0,
            1,
            vec![],
            0,
            vec![],
            vec![],
        )
        .unwrap();

        // After init: gen=1, active_sb=0, active_bitmap=0.
        for expected_gen in 2..=5u64 {
            pd.commit_manifest(|_body, bitmap| {
                bitmap.set(0, ChunkletState::Used)?;
                Ok(())
            })
            .unwrap();
            assert_eq!(pd.manifest_gen(), expected_gen);
        }

        // Reopen and verify state survived.
        drop(pd);
        let raw2 = RawDevice::open(&path).unwrap();
        let pd2 = PhysicalDisk::open(raw2).unwrap();
        assert_eq!(pd2.manifest_gen(), 5);
        let (_, bitmap, _) = pd2.snapshot();
        assert_eq!(bitmap.get(0).unwrap(), ChunkletState::Used);
    }

    #[test]
    fn probe_liveness_true_on_healthy_pd() {
        let dir = TempDir::new().unwrap();
        let raw = sparse_pd(&dir, "pd0", TEST_PD_SIZE);
        let pd = PhysicalDisk::init(
            raw,
            PoolId::new_v4(),
            PdId::new_v4(),
            0,
            1,
            vec![],
            0,
            vec![],
            vec![],
        )
        .unwrap();
        // A backing file that answers reads is "alive".
        assert!(pd.probe_liveness());
    }

    #[test]
    fn user_io_round_trip() {
        let dir = TempDir::new().unwrap();
        let raw = sparse_pd(&dir, "pd0", TEST_PD_SIZE);
        let pd = PhysicalDisk::init(
            raw,
            PoolId::new_v4(),
            PdId::new_v4(),
            0,
            1,
            vec![],
            0,
            vec![],
            vec![],
        )
        .unwrap();

        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        pd.write_chunklet_user(0, 0, &payload).unwrap();
        let mut readback = vec![0u8; 4096];
        pd.read_chunklet_user(0, 0, &mut readback).unwrap();
        assert_eq!(readback, payload);
    }

    #[test]
    fn rejects_pd_too_small() {
        let dir = TempDir::new().unwrap();
        let raw = sparse_pd(&dir, "tiny", 64 * 1024 * 1024); // 64 MiB
        let err = PhysicalDisk::init(
            raw,
            PoolId::new_v4(),
            PdId::new_v4(),
            0,
            1,
            vec![],
            0,
            vec![],
            vec![],
        )
        .err()
        .expect("expected init to fail");
        assert!(matches!(err, ChunkletError::Config(_)));
    }
}
