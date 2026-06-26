use std::{cell::Cell, sync::{Arc, atomic::{AtomicBool, AtomicPtr, AtomicU8, Ordering}}};

const MAXS_SLOTS: usize = 64;

thread_local! {
    static DEBT_SLOT: Cell<Option<usize>> = Cell::new(None)
}


const EMPTY: u8 = 0;
const REGISTERED: u8 = 1;
const PAID: u8 = 2;
const CONSUMED: u8 = 3;


pub(crate) struct DebtSlot<T> { 
    ptr: AtomicPtr<T>,
    registered: AtomicBool,
    state: AtomicU8
}

pub(crate) struct DebtRegistry<T> { 
    slots: Vec<DebtSlot<T>>
}

pub(crate) enum DebtResult<T> { 
    Paid(Arc<T>),
    ReaderOwns(*mut T),
    Pending
}

impl<T> DebtRegistry<T> { 
    pub(crate) fn new() -> Self { 
        let mut slots = Vec::new();
        for _ in 0..MAXS_SLOTS { 
            slots.push(DebtSlot{
                ptr: AtomicPtr::new(std::ptr::null_mut()),
                registered: AtomicBool::new(false),
                state: AtomicU8::new(EMPTY)
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
        self.slots[slot].state.store(REGISTERED, Ordering::Release);
        slot
    }

    pub(crate) fn pay(&self, ptr: *mut T) { 
        for slot in &self.slots { 
            let debt_ptr = slot.ptr.load(Ordering::Acquire);
            if debt_ptr != ptr {
                continue;
            }
            if slot.state.compare_exchange(REGISTERED, PAID, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                unsafe { 
                    Arc::increment_strong_count(ptr);
                }
            }
        }
    }

    pub fn collect(
        &self,
        slot: usize,
    ) -> DebtResult<T> {
        // case 1: writer wins the race
        if self.slots[slot].state.compare_exchange(PAID, CONSUMED, Ordering::AcqRel,Ordering::Acquire).is_ok() { 
            // means writer already paid
            let ptr = self.slots[slot].ptr.swap(std::ptr::null_mut(), Ordering::AcqRel);
            return DebtResult::Paid(unsafe { Arc::from_raw(ptr) });
        }

        // case 1: reader wins the race
        if self.slots[slot].state.compare_exchange(REGISTERED, CONSUMED, Ordering::AcqRel,Ordering::Acquire).is_ok() { 
            // means writer already paid
            let ptr = self.slots[slot].ptr.swap(std::ptr::null_mut(), Ordering::AcqRel);
            return DebtResult::ReaderOwns(ptr);
        } 
        DebtResult::Pending
    }
    
}