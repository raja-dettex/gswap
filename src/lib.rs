use std::{cell::Cell, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering}}};
use crossbeam_epoch::{Atomic, Owned, Shared};

use crate::{debt::DebtRegistry, gen_lock::GenLock, guard::Guard};
mod gen_lock;
mod guard;
mod debt;

struct RetiredPtr<T> { 
    ptr: *mut T,
}
pub struct LoadGuard<'a, T> { 
    ptr: *mut T,
    debt_slot: usize,
    owner: &'a GSwap<T>,
}

// safety here is we are derefencing a valid pointer and contructing the borrow of arc of raw pointer
// the raw pointer itself is originated from one arc refer to [GSwap::load_borrow]
impl<T> LoadGuard<'_, T> { 
    pub fn upgrade(&self) -> Arc<T> {

        // fast path: swap paid the debt and arc strong count is incremented. 
        if let Some(arc) = self.owner.debt_registry.collect(self.debt_slot) { 
            return arc;
        }

        // slow path
        let _guard = Guard::new(&self.owner.gen_lock);
        
        unsafe { 
            
            Arc::increment_strong_count(self.ptr as *const T);
            Arc::from_raw(self.ptr as *const T) 
        }
    }
}
pub  struct GSwap<T> { 
    // published pointer
    current: AtomicPtr<T>,
    
    // fallback synchronization
    gen_lock: GenLock,

    // fast path ownership transfer
    debt_registry: DebtRegistry<T>,

    // list of deferred destruction
    retired: Mutex<Vec<RetiredPtr<T>>>,
}

unsafe impl<T: Send + Sync> Send for RetiredPtr<T> {}
unsafe impl<T: Send + Sync> Sync for RetiredPtr<T> {}


impl<T> GSwap<T> { 
    pub fn new(item: T) -> Self { 
        let ptr = Arc::into_raw(Arc::new(item)) as *mut T;
        let gen_lock = GenLock::new();
        Self { 
            current: AtomicPtr::new(ptr),
            gen_lock,
            debt_registry: DebtRegistry::new(),
            retired: Mutex::new(Vec::new()),
        }
    }

    
    pub fn load_borrow(&self) -> LoadGuard<'_, T> { 
        let ptr = self.current.load(Ordering::Acquire);
        let debt_slot = self.debt_registry.register(ptr);
        LoadGuard { ptr, debt_slot, owner: self }
    }

    pub fn swap(&self, value: T)  { 
        let boxed = Arc::new(value);
        let old = self.current.swap(Arc::into_raw(boxed) as *mut T, Ordering::Release);

        // fast path
        self.debt_registry.pay(old);
        self.retired.lock().expect("should hold it").push(RetiredPtr { ptr: old });
        
        
        self.try_reclaim();
    }


    pub fn try_reclaim(&self) { 
        for retired in self.retired.lock().expect("retired list").drain(..) { 
            self.debt_registry.pay(retired.ptr);
            unsafe { drop(Arc::from_raw(retired.ptr)); }
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