//! NUMA discovery and thread-affinity helpers for chunklet IO paths.
//!
//! Chunklet is intentionally independent from the main onyx-storage crate, so
//! this module owns the small subset it needs: map a PD path to a NUMA node,
//! discover CPUs for that node, and bind short-lived IO workers to the local
//! CPU set before touching the NVMe.

use std::cell::Cell;
use std::path::Path;
use std::sync::OnceLock;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NumaNode {
    pub id: u16,
    pub cpus: Vec<usize>,
}

pub fn detect_pd_node(path: &Path) -> Option<u16> {
    detect_pd_node_from_root(path, Path::new("/sys"))
}

pub(crate) fn detect_pd_node_from_root(path: &Path, sys_root: &Path) -> Option<u16> {
    let block = block_device_name(path)?;
    let path = sys_root
        .join("class")
        .join("block")
        .join(block)
        .join("device")
        .join("numa_node");
    let raw = std::fs::read_to_string(path).ok()?;
    parse_numa_node(raw.trim())
}

pub fn detect_nodes() -> Vec<NumaNode> {
    detect_nodes_from_root(Path::new("/sys/devices/system/node"))
}

pub(crate) fn detect_nodes_from_root(node_root: &Path) -> Vec<NumaNode> {
    let mut nodes = Vec::new();
    let Ok(entries) = std::fs::read_dir(node_root) else {
        return nodes;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(id) = name
            .strip_prefix("node")
            .and_then(|s| s.parse::<u16>().ok())
        else {
            continue;
        };
        let Ok(raw_cpus) = std::fs::read_to_string(entry.path().join("cpulist")) else {
            continue;
        };
        let cpus = parse_cpu_list(&raw_cpus);
        if !cpus.is_empty() {
            nodes.push(NumaNode { id, cpus });
        }
    }
    nodes.sort_by_key(|n| n.id);
    nodes
}

pub fn cpus_for_node(node: u16) -> Vec<usize> {
    cached_nodes()
        .into_iter()
        .find(|n| n.id == node)
        .map(|n| n.cpus.clone())
        .unwrap_or_default()
}

pub fn bind_current_to_node(node: Option<u16>) {
    let Some(node) = node else {
        return;
    };
    let already_bound = LAST_BOUND_NODE.with(|last| last.get() == Some(node));
    if already_bound {
        return;
    }
    let cpus = cpus_for_node(node);
    if cpus.is_empty() {
        return;
    }
    match set_current_cpus(&cpus) {
        Ok(()) => LAST_BOUND_NODE.with(|last| last.set(Some(node))),
        Err(err) => {
            tracing::warn!(node, error = %err, "failed to bind chunklet IO worker to NUMA node");
        }
    }
}

static NODES: OnceLock<Vec<NumaNode>> = OnceLock::new();

thread_local! {
    static LAST_BOUND_NODE: Cell<Option<u16>> = const { Cell::new(None) };
}

fn cached_nodes() -> &'static [NumaNode] {
    NODES.get_or_init(detect_nodes).as_slice()
}

fn block_device_name(path: &Path) -> Option<String> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    block_device_name_from_canonical(&canonical)
}

fn block_device_name_from_canonical(path: &Path) -> Option<String> {
    let mut cur: Option<&Path> = Some(path);
    while let Some(p) = cur {
        if p.starts_with("/dev") {
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                return Some(name.to_string());
            }
        }
        if p.starts_with("/sys/class/block") || p.starts_with("/sys/block") {
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                return Some(name.to_string());
            }
        }
        cur = p.parent();
    }
    path.file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
}

fn parse_numa_node(raw: &str) -> Option<u16> {
    let node = raw.parse::<i32>().ok()?;
    (node >= 0).then_some(node as u16)
}

pub fn parse_cpu_list(raw: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in raw
        .trim()
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) = (start.parse::<usize>(), end.parse::<usize>()) {
                if start <= end {
                    cpus.extend(start..=end);
                }
            }
        } else if let Ok(cpu) = part.parse::<usize>() {
            cpus.push(cpu);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    cpus
}

#[cfg(target_os = "linux")]
fn set_current_cpus(cpus: &[usize]) -> std::io::Result<()> {
    if cpus.is_empty() {
        return Ok(());
    }
    const CPU_SETSIZE: usize = 1024;
    const BITS_PER_WORD: usize = usize::BITS as usize;
    let mut set = [0usize; CPU_SETSIZE / BITS_PER_WORD];
    for &cpu in cpus {
        if cpu < CPU_SETSIZE {
            set[cpu / BITS_PER_WORD] |= 1usize << (cpu % BITS_PER_WORD);
        }
    }
    let rc = unsafe {
        libc::sched_setaffinity(
            0,
            std::mem::size_of_val(&set),
            set.as_ptr().cast::<libc::cpu_set_t>(),
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn set_current_cpus(_cpus: &[usize]) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn cpu_list_parser_handles_ranges() {
        assert_eq!(parse_cpu_list("0,2,4-6,6\n"), vec![0, 2, 4, 5, 6]);
    }

    #[test]
    fn node_minus_one_means_unknown() {
        assert_eq!(parse_numa_node("-1"), None);
        assert_eq!(parse_numa_node("1"), Some(1));
    }

    #[test]
    fn detects_pd_node_from_fake_sysfs() {
        let dir = TempDir::new().unwrap();
        let path = dir
            .path()
            .join("sys")
            .join("class")
            .join("block")
            .join("nvme0n1")
            .join("device");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("numa_node"), "1\n").unwrap();
        assert_eq!(
            detect_pd_node_from_root(Path::new("/dev/nvme0n1"), &dir.path().join("sys")),
            Some(1)
        );
    }

    #[test]
    fn detects_nodes_from_fake_sysfs() {
        let dir = TempDir::new().unwrap();
        let node0 = dir.path().join("node0");
        let node1 = dir.path().join("node1");
        std::fs::create_dir_all(&node0).unwrap();
        std::fs::create_dir_all(&node1).unwrap();
        std::fs::write(node0.join("cpulist"), "0-3\n").unwrap();
        std::fs::write(node1.join("cpulist"), "4,6-7\n").unwrap();
        let nodes = detect_nodes_from_root(dir.path());
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].cpus, vec![0, 1, 2, 3]);
        assert_eq!(nodes[1].cpus, vec![4, 6, 7]);
    }

    #[test]
    fn extracts_block_name_from_paths() {
        assert_eq!(
            block_device_name_from_canonical(&std::path::PathBuf::from("/dev/nvme1n1p1")),
            Some("nvme1n1p1".into())
        );
        assert_eq!(
            block_device_name_from_canonical(&std::path::PathBuf::from("/sys/class/block/nvme0n1")),
            Some("nvme0n1".into())
        );
    }
}
