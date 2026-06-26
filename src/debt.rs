use std::{cell::Cell, sync::{Arc, atomic::{AtomicBool, AtomicPtr, Ordering}}};

const MAXS_SLOTS: usize = 64;

thread_local! {
    static DEBT_SLOT: Cell<Option<usize>> = Cell::new(None)
}
pub(crate) struct DebtSlot<T> { 
    ptr: AtomicPtr<T>,
    registered: AtomicBool,
    paid: AtomicBool
}

pub(crate) struct DebtRegistry<T> { 
    slots: Vec<DebtSlot<T>>
}

impl<T> DebtRegistry<T> { 
    pub(crate) fn new() -> Self { 
        let mut slots = Vec::new();
        for _ in 0..MAXS_SLOTS { 
            slots.push(DebtSlot{
                ptr: AtomicPtr::new(std::ptr::null_mut()),
                registered: AtomicBool::new(false),
                paid: AtomicBool::new(false)
            });
        }
        Self { slots }
    }

    pub(crate) fn acquire_slot(&self) -> usize { 
        DEBT_SLOT.with(|slot_id|  {
            if let Some(slot) = slot_id.get() { 
                return slot;
            }
            for (idx, slot) in self.slots.iter().enumerate() { 
                if slot.registered.compare_exchange(
                    false, 
                        true, 
                        Ordering::AcqRel, 
                        Ordering::Acquire).is_ok() { 
                        DEBT_SLOT.set(Some(idx));
                        return idx;
                    }
            }
            panic!("no available debt slots yet");
        })
    }

    pub(crate) fn register(&self, ptr: *mut T) -> usize { 
        let slot = self.acquire_slot();
        self.slots[slot].ptr.store(ptr, Ordering::Release);
        self.slots[slot].paid.store(false, Ordering::Release);
        slot
    }

    pub(crate) fn pay(&self, ptr: *mut T) { 
        for slot in &self.slots { 
            let debt_ptr = slot.ptr.load(Ordering::Acquire);
            if debt_ptr != ptr {
                continue;
            }
            if slot.paid.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                unsafe { 
                    Arc::increment_strong_count(ptr);
                }
            }
        }
    }

    pub fn collect(
        &self,
        slot: usize,
    ) -> Option<Arc<T>> {
        let paid = self.slots[slot]
            .paid
            .load(Ordering::Acquire);

        if !paid {
            return None;
        }

        let ptr = self.slots[slot]
            .ptr
            .load(Ordering::Acquire);

        if ptr.is_null() {
            return None;
        }

        self.slots[slot]
            .ptr
            .store(
                std::ptr::null_mut(),
                Ordering::Release,
            );

        Some(unsafe {
            Arc::from_raw(ptr as *const T)
        })
    }
    
}