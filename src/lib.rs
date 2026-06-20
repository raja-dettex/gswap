use std::{cell::Cell, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering}}};
use crossbeam_epoch::{Atomic, Owned, Shared};

use crate::{debt::DebtRegistry, gen_lock::GenLock, guard::Guard};
mod gen_lock;
mod guard;
mod debt;

const EMPTY_GENERATOIN: usize = usize::MAX;
const MAX_READERS: usize = 64;

thread_local! {
    static SLOT_ID: Cell<Option<usize>> = Cell::new(None);
}

struct RetiredPtr<T> { 
    ptr: *mut Arc<T>,
}
pub struct LoadGuard<'a, T> { 
    ptr: *mut Arc<T>,
    debt_slot: usize,
    owner: &'a GSwap<T>,
    guard: Guard<'a>
}

// safety here is we are derefencing a valid pointer and contructing the borrow of arc of raw pointer
// the raw pointer itself is originated from one arc refer to [GSwap::load_borrow]
impl<T> LoadGuard<'_, T> { 
    pub fn upgrade(&self) -> Arc<T> {
        if let Some(arc) = self.owner.debt_registry.collect(self.debt_slot) { 
            return arc;
        }
        unsafe { Arc::clone(&*self.ptr) }
    }
}
pub  struct GSwap<T> { 
    current: AtomicPtr<Arc<T>>,
    gen_lock: GenLock,
    debt_registry: DebtRegistry<T>,
    retired: Mutex<Vec<RetiredPtr<T>>>,
}

unsafe impl<T: Send + Sync> Send for RetiredPtr<T> {}
unsafe impl<T: Send + Sync> Sync for RetiredPtr<T> {}


impl<T> GSwap<T> { 
    pub fn new(item: T) -> Self { 
        let boxed = Box::new(Arc::new(item));
        let gen_lock = GenLock::new();
        Self { 
            current: AtomicPtr::new(Box::into_raw(boxed)),
            gen_lock,
            debt_registry: DebtRegistry::new(),
            retired: Mutex::new(Vec::new()),
        }
    }
    
    // pub fn acquire_slot(&self) -> usize { 
    //     SLOT_ID.with(|slot_id| { 
    //         if let Some(slot) = slot_id.get() { 
    //             return slot
    //         }
    //         for (idx, slot) in self.readers.iter().enumerate() { 
    //             let acquired = slot.registered.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok();
    //             if acquired { 
    //                 slot_id.set(Some(idx));
    //                 return idx;
    //             }
    //         }
    //         panic!("no available readers slot");
    //     })
    // }
    
    // pub fn load(&self) -> Arc<T> { 
    //     // slot registration for now slot is fixed to 0, 
    //     // TODO: implement slot registration
    //     let slot = self.acquire_slot();
    //     let generation = self.global_generation.load(Ordering::Acquire);
    //     self.readers[slot].generation.store(generation, Ordering::Release);
    //     let ptr = self.current.load(Ordering::Acquire);
    //     let arc = unsafe { Arc::clone(&*ptr) };
    //     self.readers[slot].generation.store(EMPTY_GENERATOIN, Ordering::Release);
    //     arc

    // }
    // this proves the later derefecing of the ptr is safe 
    // protected by guard. 
    pub fn load_borrow(&self) -> LoadGuard<'_, T> { 
        let guard = Guard::new(&self.gen_lock);
        let ptr = self.current.load(Ordering::Acquire);
        let debt_slot = self.debt_registry.register(ptr);
        LoadGuard { ptr, debt_slot, owner: self, guard }
    }

    // the safe reclaimation gurantee here is gen_lock.wait_for_readers(old_gen);
    pub fn swap(&self, value: T)  { 
        let boxed = Box::new(Arc::new(value));
        let old = self.current.swap(Box::into_raw(boxed), Ordering::Release);
        let old_gen = self.gen_lock.flip_generation();
        self.retired.lock().expect("cant hold from swap").push(RetiredPtr { 
            ptr: old,
        });
        self.gen_lock.wait_for_readers(old_gen);
        self.try_reclaim();
    }


    pub fn try_reclaim(&self) { 
        for retired in self.retired.lock().expect("retired list").drain(..) { 
            self.debt_registry.pay(retired.ptr);
            unsafe { drop(Box::from_raw(retired.ptr)); }
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


    #[test]
    pub fn load_store() { 
        let swap = GSwap::new(5u64);
        assert_eq!(*swap.load_borrow().upgrade(), 5);
        swap.swap(10u64);
        assert_eq!(*swap.load_borrow().upgrade(), 10);

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

    #[test]
    pub fn reader_keeps_old_versions_alive() { 
        let swap = GSwap::new(10u64);
        let old_guard = swap.load_borrow();
        let old = old_guard.upgrade();
        swap.swap(15u64);
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
                    let value = swap.load_borrow().upgrade();
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
                    let v = swap.load_borrow().upgrade();
                    let _ = *v;
                }
            }));
        }
        for _ in 0..8 { 
            let swap: Arc<GSwap<u64>> = swap.clone();
            handles.push(thread::spawn(move || {
                for i in 0..WRITE_ITERS { 
                    let v = swap.swap(i as u64);
                }
            }))
        }
        for handle in handles { 
            let _ = handle.join();
        }
    }
    
    
}