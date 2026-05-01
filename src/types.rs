use std::fmt;

use uuid::Uuid;

/// 4 KiB block size (PD-level addressing unit).
pub const BLOCK_SIZE: u64 = 4096;

/// Default chunklet size: 1 GiB. Locked by spec.
pub const CHUNKLET_SIZE_LOG2: u8 = 30;
pub const CHUNKLET_SIZE: u64 = 1 << CHUNKLET_SIZE_LOG2;

/// Reserved head/tail region per PD (1 MiB each).
/// Holds: superblock COW pair, bitmap COW pair, room for future metadata.
pub const PD_RESERVED_BYTES: u64 = 1 << 20;

/// Slot offsets within the head/tail reserved region.
pub const SUPERBLOCK_SLOT_A_OFFSET: u64 = 0;
pub const SUPERBLOCK_SLOT_B_OFFSET: u64 = 4096;
pub const BITMAP_SLOT_A_OFFSET: u64 = 8192;
pub const BITMAP_SLOT_B_OFFSET: u64 = 8192 + 256 * 1024; // 264 KiB

/// Bitmap region size per slot: 256 KiB. Supports up to 256 Ki chunklets = 256 TiB per PD.
pub const BITMAP_SLOT_BYTES: u64 = 256 * 1024;

/// Maximum number of chunklets per PD (limited by bitmap slot size, 1 byte / chunklet).
pub const MAX_CHUNKLETS_PER_PD: u32 = (BITMAP_SLOT_BYTES) as u32;

/// Per-chunklet header: 4 KiB at the start of every chunklet.
pub const CHUNKLET_HEADER_BYTES: u64 = 4096;

/// On-disk superblock format version. Bumped on any layout change.
pub const SUPERBLOCK_VERSION: u32 = 1;

/// Magic for chunklet superblock: ASCII "ONYXCHK1".
pub const SUPERBLOCK_MAGIC: &[u8; 8] = b"ONYXCHK1";

macro_rules! uuid_newtype {
    ($name:ident, $tag:literal) => {
        #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(pub Uuid);

        impl $name {
            pub fn new_v4() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn nil() -> Self {
                Self(Uuid::nil())
            }

            pub fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(Uuid::from_bytes(bytes))
            }

            pub fn to_bytes(self) -> [u8; 16] {
                *self.0.as_bytes()
            }

            pub fn is_nil(self) -> bool {
                self.0.is_nil()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", $tag, self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

uuid_newtype!(PoolId, "Pool");
uuid_newtype!(PdId, "Pd");
uuid_newtype!(LdId, "Ld");
uuid_newtype!(CpgId, "Cpg");

/// Chunklet identity within a PD: (pd_id, index).
/// `index` is the chunklet ordinal on the PD, 0 = first chunklet.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ChunkletId {
    pub pd: PdId,
    pub index: u32,
}

impl ChunkletId {
    pub fn new(pd: PdId, index: u32) -> Self {
        Self { pd, index }
    }

    /// Byte offset of this chunklet within its PD.
    pub fn pd_offset_bytes(self) -> u64 {
        PD_RESERVED_BYTES + (self.index as u64) * CHUNKLET_SIZE
    }

    /// Byte offset of the user-data region (after the chunklet header).
    pub fn user_offset_bytes(self) -> u64 {
        self.pd_offset_bytes() + CHUNKLET_HEADER_BYTES
    }

    /// Bytes available for user data within this chunklet.
    pub fn user_capacity_bytes() -> u64 {
        CHUNKLET_SIZE - CHUNKLET_HEADER_BYTES
    }
}

impl fmt::Display for ChunkletId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.pd, self.index)
    }
}

/// State of a chunklet, encoded as 1 byte in the bitmap.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum ChunkletState {
    Free = 0,
    Used = 1,
    Spare = 2,
    Bad = 3,
    Migrating = 4,
}

impl ChunkletState {
    pub fn from_byte(b: u8) -> crate::ChunkletResult<Self> {
        match b {
            0 => Ok(Self::Free),
            1 => Ok(Self::Used),
            2 => Ok(Self::Spare),
            3 => Ok(Self::Bad),
            4 => Ok(Self::Migrating),
            other => Err(crate::ChunkletError::Format(format!(
                "unknown chunklet state byte: {}",
                other
            ))),
        }
    }
}

/// HA failure domain (used by allocator to spread RAID set members).
/// Phase 0 only `Pd` is wired; `Numa` / `PcieSwitch` are placeholders.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum HaDomain {
    /// One member per PD (default; works for all single-host NVMe pools).
    Pd,
    /// One member per NUMA node. Not implemented yet.
    Numa,
    /// One member per PCIe switch. Not implemented yet.
    PcieSwitch,
}

impl HaDomain {
    pub fn is_supported(self) -> bool {
        matches!(self, HaDomain::Pd)
    }

    pub fn from_u8(b: u8) -> ChunkletResult<Self> {
        match b {
            0 => Ok(Self::Pd),
            1 => Ok(Self::Numa),
            2 => Ok(Self::PcieSwitch),
            other => Err(crate::ChunkletError::Format(format!(
                "unknown HaDomain byte: {}",
                other
            ))),
        }
    }
}

use crate::ChunkletResult;

/// RAID level supported by an LD. Phase numbers indicate when each lands.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum RaidLevel {
    /// Linear concat (P1). No redundancy, no striping; offset → single chunklet.
    Plain = 0,
    /// Mirror with N copies (P2). N=2 is RAID-1; combined with stripe rows = RAID-10.
    Mirror = 1,
    /// RAID-5: N data + 1 parity (P3).
    Raid5 = 2,
    /// RAID-6: N data + P + Q (P4).
    Raid6 = 3,
}

impl RaidLevel {
    pub fn from_u8(b: u8) -> ChunkletResult<Self> {
        match b {
            0 => Ok(Self::Plain),
            1 => Ok(Self::Mirror),
            2 => Ok(Self::Raid5),
            3 => Ok(Self::Raid6),
            other => Err(crate::ChunkletError::Format(format!(
                "unknown RaidLevel byte: {}",
                other
            ))),
        }
    }
}

/// Role of a chunklet within an LD's RAID set.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum LdRole {
    Data = 0,
    ParityP = 1,
    ParityQ = 2,
    /// Reserved for future spare-promotion records.
    Spare = 3,
}

impl LdRole {
    pub fn from_u8(b: u8) -> ChunkletResult<Self> {
        match b {
            0 => Ok(Self::Data),
            1 => Ok(Self::ParityP),
            2 => Ok(Self::ParityQ),
            3 => Ok(Self::Spare),
            other => Err(crate::ChunkletError::Format(format!(
                "unknown LdRole byte: {}",
                other
            ))),
        }
    }
}

/// One chunklet member of an LD: which PD, which chunklet on that PD, and
/// what role it plays.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct LdMember {
    pub pd: PdId,
    pub chunklet_index: u32,
    pub role: LdRole,
}
