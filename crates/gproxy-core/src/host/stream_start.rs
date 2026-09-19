use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use bytes::Bytes;

use crate::CoreError;

/// Shared allocation budget for uncommitted stream prefixes and inspection
/// scratch. Reservations fail immediately: waiting while holding other prefix
/// allocations could deadlock all requests in the pool.
pub struct StreamStartBudget {
    limit: usize,
    used: AtomicUsize,
}

impl StreamStartBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
        })
    }

    /// A process-wide pool, also shared by independently constructed cores.
    pub fn process_default() -> Arc<Self> {
        static POOL: OnceLock<Arc<StreamStartBudget>> = OnceLock::new();
        POOL.get_or_init(|| Self::new(128 * 1024 * 1024)).clone()
    }

    pub fn in_use(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub(crate) fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation, CoreError> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .map_err(|_| CoreError::StreamStartOverloaded)?;
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
}

pub(crate) struct Reservation {
    budget: Arc<StreamStartBudget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// One raw prefix, never a vector of transport fragments. Charge allocation
/// capacity, including both old and new allocations during growth. A Bytes
/// owner carries the reservation through replay, captures and their clones.
pub(crate) struct PrefixBuffer {
    data: Vec<u8>,
    allocation: Reservation,
}

impl PrefixBuffer {
    pub(crate) fn new(budget: &Arc<StreamStartBudget>) -> Self {
        Self {
            data: Vec::new(),
            allocation: budget.reserve(0).expect("empty reservation"),
        }
    }

    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<(), CoreError> {
        let required = self
            .data
            .len()
            .checked_add(bytes.len())
            .ok_or(CoreError::StreamStartOverloaded)?;
        if required > self.data.capacity() {
            let capacity = required
                .max(4096)
                .checked_next_power_of_two()
                .ok_or(CoreError::StreamStartOverloaded)?;
            return self.grow_and_append(capacity, bytes);
        }
        self.data.extend_from_slice(bytes);
        Ok(())
    }

    fn grow_and_append(&mut self, capacity: usize, bytes: &[u8]) -> Result<(), CoreError> {
        let mut allocation = self.allocation.budget.reserve(capacity)?;
        let mut data = Vec::new();
        data.try_reserve_exact(capacity)
            .map_err(|_| CoreError::StreamStartOverloaded)?;
        let mut extra = allocation.budget.reserve(data.capacity() - capacity)?;
        allocation.bytes += extra.bytes;
        extra.bytes = 0;
        data.extend_from_slice(&self.data);
        data.extend_from_slice(bytes);
        self.data = data;
        self.allocation = allocation;
        Ok(())
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

impl AsRef<[u8]> for PrefixBuffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn competing_requests_never_exceed_the_shared_budget() {
        let budget = StreamStartBudget::new(64);
        let start = Arc::new(std::sync::Barrier::new(33));
        let held = Arc::new(std::sync::Barrier::new(33));
        let release = Arc::new(std::sync::Barrier::new(33));
        let mut threads = Vec::new();
        for _ in 0..32 {
            let (budget, start, held, release) =
                (budget.clone(), start.clone(), held.clone(), release.clone());
            threads.push(std::thread::spawn(move || {
                start.wait();
                let permit = budget.reserve(8);
                assert!(budget.in_use() <= budget.limit());
                held.wait();
                release.wait();
                permit.is_ok()
            }));
        }
        start.wait();
        held.wait();
        assert_eq!(budget.in_use(), 64);
        release.wait();
        let admitted = threads
            .into_iter()
            .map(|thread| usize::from(thread.join().unwrap()))
            .sum::<usize>();
        assert_eq!(admitted, 8);
        assert_eq!(budget.in_use(), 0);
        assert!(budget.reserve(usize::MAX).is_err());
        assert_eq!(budget.in_use(), 0);
    }

    #[test]
    fn growth_charges_capacity_and_replay_clones_keep_the_reservation() {
        let budget = StreamStartBudget::new(12 * 1024);
        let mut buffer = PrefixBuffer::new(&budget);
        buffer.append(&vec![b'a'; 4096]).unwrap();
        assert_eq!(budget.in_use(), 4096);
        buffer.append(b"b").unwrap();
        assert_eq!(budget.in_use(), 8192);
        assert!(buffer.append(&vec![b'c'; 8192]).is_err());
        assert_eq!(buffer.as_slice().len(), 4097);
        assert_eq!(budget.in_use(), 8192);
        let bytes = buffer.into_bytes();
        let tail = bytes.slice(4096..);
        drop(bytes);
        assert_eq!(budget.in_use(), 8192);
        assert_eq!(&tail[..], b"b");
        drop(tail);
        assert_eq!(budget.in_use(), 0);
    }

    #[test]
    fn independent_default_hosts_share_one_pool() {
        assert!(Arc::ptr_eq(
            &StreamStartBudget::process_default(),
            &StreamStartBudget::process_default()
        ));
    }
}
