use std::sync::{Arc, atomic::{AtomicPtr, AtomicUsize, Ordering}};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

pub  struct GSwap<T> { 
    ptr: Atomic<Arc<T>>
}

impl<T> Drop for GSwap<T> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        let shared = self.ptr.swap(crossbeam_epoch::Shared::null(), Ordering::AcqRel, &guard);
        // safety here when finally gswap is dropped, it takes the mutable reference of gswap none other holds it then
        // the last published ptr is swapped with null, we have exclusive ownership of the old pointer
        // own it by into_owned allocation
        // and finally dropping released the owned allocation
        if !shared.is_null() { 
            unsafe { drop(shared.into_owned()); }
        }
    }
}


impl<T> GSwap<T> { 
    pub fn new(item: T) -> Self { 
        Self { 
            ptr: Atomic::new(Arc::new(item))
        }
    }
    // all the load is guarded by epoch based reclaimation
    // any thread comes and laod the pointer it gives back a shared pointer Shared<'g, Arc<T>> which is *const Arc<T> and guarded by guard
    // guard is pinned here and its lifetime is the lifetime of the caller. caller has to ensure that
    pub fn load(&self) -> Arc<T> { 
        let guard = crossbeam_epoch::pin();
        let shared = self.ptr.load(Ordering::SeqCst, &guard);
        let arc_ref = unsafe { shared.as_ref().expect("share ptr should never be null") };
        Arc::clone(arc_ref)
    }
    // store swaps the ptr with new ptr owned by crossbeam epoch Owned 
    // and we dont drop the guard immediately like drop(guard)
    // instead we do defer_destroy on the guard which essentially means destructor is stored and will be reclaimed after all the pinned guards have been dropped
    pub fn store(&self, item: T) {
        let guard = crossbeam_epoch::pin(); 
        let old = self.ptr.swap(Owned::new(Arc::new(item)), Ordering::AcqRel, &guard);
        unsafe { guard.defer_destroy(old);}
    }
    pub fn swap(&self, value: T) -> Arc<T> { 
        let guard = crossbeam_epoch::pin();
        let old = self.ptr.swap(Owned::new(Arc::new(value)), Ordering::AcqRel, &guard);
        let old_arc = unsafe { old.into_owned().into_box()};
        *old_arc
    }
    
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

use crate::GSwap;


    #[test]
    pub fn load_store() { 
        let swap = GSwap::new(5u64);
        assert_eq!(*swap.load(), 5);
        swap.store(10u64);
        assert_eq!(*swap.load(), 10);

    }

    #[test]
    fn swap_returns_old() {
        let swap = GSwap::new(5u64);

        let old = swap.swap(10);

        assert_eq!(*old, 5);
        assert_eq!(*swap.load(), 10);
    }

    #[test]
    pub fn many_readers_one_writer() { 
        use std::thread;
        let swap = Arc::new(GSwap::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..8 { 
            let swap = swap.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100_000 { 
                    let v = swap.load();
                    let _ = *v;
                }
            }))
        }
        for _ in 0..8 { 
            let swap = swap.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 { 
                    let v = swap.store(i);
                }
            }))
        }
        for handle in handles { 
            let _ = handle.join();
        }
    }
    
    
}