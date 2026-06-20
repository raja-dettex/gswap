use std::{cell::Cell, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering}}};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
mod gen_lock;
mod guard;

const EMPTY_GENERATOIN: usize = usize::MAX;
const MAX_READERS: usize = 64;

thread_local! {
    static SLOT_ID: Cell<Option<usize>> = Cell::new(None);
}
struct ReaderSlot { 
    generation: AtomicUsize,
    registered: AtomicBool
}
struct RetiredPtr<T> { 
    ptr: *mut Arc<T>,
    generation: usize
}
pub  struct GSwap<T> { 
    current: AtomicPtr<Arc<T>>,
    global_generation: AtomicUsize,
    readers: Vec<ReaderSlot>,
    retired: Mutex<Vec<RetiredPtr<T>>>,
}

unsafe impl<T: Send + Sync> Send for RetiredPtr<T> {}
unsafe impl<T: Send + Sync> Sync for RetiredPtr<T> {}

// impl<T> Drop for GSwap<T> {
//     fn drop(&mut self) {
//         let guard = crossbeam_epoch::pin();
//         let shared = self.ptr.swap(crossbeam_epoch::Shared::null(), Ordering::AcqRel, &guard);
//         // safety here when finally gswap is dropped, it takes the mutable reference of gswap none other holds it then
//         // the last published ptr is swapped with null, we have exclusive ownership of the old pointer
//         // own it by into_owned allocation
//         // and finally dropping released the owned allocation
//         if !shared.is_null() { 
//             unsafe { drop(shared.into_owned()); }
//         }
//     }
// }


impl<T> GSwap<T> { 
    pub fn new(item: T) -> Self { 
        let boxed = Box::new(Arc::new(item));
        let mut readers= Vec::new();
        for _ in 0..MAX_READERS { 
            readers.push(ReaderSlot { 
                generation: AtomicUsize::new(EMPTY_GENERATOIN),
                registered: AtomicBool::new(false)
            });
        }
        Self { 
            current: AtomicPtr::new(Box::into_raw(boxed)),
            readers,
            global_generation: AtomicUsize::new(0),
            retired: Mutex::new(Vec::new()),
        }
    }
    
    pub fn acquire_slot(&self) -> usize { 
        SLOT_ID.with(|slot_id| { 
            if let Some(slot) = slot_id.get() { 
                return slot
            }
            for (idx, slot) in self.readers.iter().enumerate() { 
                let acquired = slot.registered.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok();
                if acquired { 
                    slot_id.set(Some(idx));
                    return idx;
                }
            }
            panic!("no available readers slot");
        })
    }
    // all the load is guarded by epoch based reclaimation
    // any thread comes and laod the pointer it gives back a shared pointer Shared<'g, Arc<T>> which is *const Arc<T> and guarded by guard
    // guard is pinned here and its lifetime is the lifetime of the caller. caller has to ensure that
    pub fn load(&self) -> Arc<T> { 
        // slot registration for now slot is fixed to 0, 
        // TODO: implement slot registration
        let slot = self.acquire_slot();
        let generation = self.global_generation.load(Ordering::Acquire);
        self.readers[slot].generation.store(generation, Ordering::Release);
        let ptr = self.current.load(Ordering::Acquire);
        let arc = unsafe { Arc::clone(&*ptr) };
        self.readers[slot].generation.store(EMPTY_GENERATOIN, Ordering::Release);
        arc

    }
    // store swaps the ptr with new ptr owned by crossbeam epoch Owned 
    // and we dont drop the guard immediately like drop(guard)
    // instead we do defer_destroy on the guard which essentially means destructor is stored and will be reclaimed after all the pinned guards have been dropped
    pub fn store(&self, item: T) {
        let boxed = Box::new(Arc::new(item));
        let old = self.current.swap(Box::into_raw(boxed), Ordering::Release);
        let old_gen = self.global_generation.fetch_add(1, Ordering::Release);
        self.retired.lock().expect("cant hold").push(RetiredPtr{
            ptr: old, 
            generation: old_gen
        });
        self.try_reclaim();
    }
    // pub fn swap(&self, value: T) -> Arc<T> { 
    //     let boxed = Box::new(Arc::new(value));
    //     let old = self.current.swap(Box::into_raw(boxed), Ordering::Release);
    //     self.retired.lock().expect("cant hold from swap").push(RetiredPtr(old));
    //     let old_arc = unsafe { Arc::clone(&*old) };
    //     self.try_reclaim();
    //     old_arc
    // }


    pub fn try_reclaim(&self) { 
        let oldest_reader = self.readers.iter()
            .filter_map(|reader| {
                let generation = reader.generation.load(Ordering::Acquire);
                if generation == EMPTY_GENERATOIN { 
                    None
                } else { 
                    Some(generation)
                }
            })
            .min()
            .unwrap_or(
                self.global_generation.load(Ordering::Acquire) + 1
            );
        let mut retired_list = self.retired.lock().expect("retired list");
        let mut i = 0;
        while i < retired_list.len() { 
            if retired_list[i].generation < oldest_reader { 
                let retired_ptr = retired_list.swap_remove(i);
                unsafe { drop(Box::from_raw(retired_ptr.ptr));}
            } else { 
                i += 1;
            }
        }
    }
    
}

#[cfg(miri)]
const READ_ITERS: usize = 100;

#[cfg(not(miri))]
const READ_ITERS: usize = 100_000;

#[cfg(miri)]
const WRITE_ITERS: usize = 50;

#[cfg(not(miri))]
const WRITE_ITERS: usize = 10_000;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

use crate::{GSwap, READ_ITERS, WRITE_ITERS};


    //#[test]
    pub fn load_store() { 
        let swap = GSwap::new(5u64);
        assert_eq!(*swap.load(), 5);
        swap.store(10u64);
        assert_eq!(*swap.load(), 10);

    }

    // #[test]
    // fn swap_returns_old() {
    //     let swap = GSwap::new(5u64);
    //     println!("swapping");
    //     let old = swap.swap(10);
    //     println!("Deref old");
    //     assert_eq!(*old, 5);
    //     assert_eq!(*swap.load(), 10);
    // }

    //#[test]
    pub fn reader_keeps_old_versions_alive() { 
        let swap = GSwap::new(10u64);
        let old = swap.load();
        swap.store(15u64);
        assert_eq!(*old, 10);
    }
    #[test]
    pub fn many_readers() { 
        use std::thread;
        let swap = Arc::new(GSwap::new(42u64));
        let mut handles = Vec::new();
        for _ in 0..8 { 
            let swap = Arc::clone(&swap );
            handles.push(thread::spawn(move || {
                use crate::READ_ITERS;

                for _ in 0..READ_ITERS { 
                    let value = swap.load();
                    assert_eq!(*value, 42);
                }
            }));
        }
        
        for handle in handles { 
            let _ = handle.join();
        }
    }
    
    #[test]
    pub fn many_readers_one_writer() { 
        use std::thread;
        let swap = Arc::new(GSwap::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..8 { 
            let swap = Arc::clone(&swap );
            handles.push(thread::spawn(move || {
                for _ in 0..READ_ITERS { 
                    let v = swap.load();
                    let _ = *v;
                }
            }));
        }
        for _ in 0..8 { 
            let swap: Arc<GSwap<u64>> = swap.clone();
            handles.push(thread::spawn(move || {
                for i in 0..WRITE_ITERS { 
                    let v = swap.store(i as u64);
                }
            }))
        }
        for handle in handles { 
            let _ = handle.join();
        }
    }
    
    
}