use std::sync::{Arc, Mutex, atomic::{AtomicPtr, AtomicUsize, Ordering}};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};


struct RetiredPtr<T>(*mut Arc<T>);
pub  struct GSwap<T> { 
    current: AtomicPtr<Arc<T>>,
    retired: Mutex<Vec<RetiredPtr<T>>>,
    active_readers: AtomicUsize
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
        Self { 
            current: AtomicPtr::new(Box::into_raw(boxed)),
            retired: Mutex::new(Vec::new()),
            active_readers: AtomicUsize::new(0)
        }
    }
    // all the load is guarded by epoch based reclaimation
    // any thread comes and laod the pointer it gives back a shared pointer Shared<'g, Arc<T>> which is *const Arc<T> and guarded by guard
    // guard is pinned here and its lifetime is the lifetime of the caller. caller has to ensure that
    pub fn load(&self) -> Arc<T> { 
        self.active_readers.fetch_add(1, Ordering::Acquire);
        let ptr = self.current.load(Ordering::Acquire);
        let arc = unsafe { Arc::clone(&*ptr) };
        self.active_readers.fetch_sub(1, Ordering::Release);
        arc

    }
    // store swaps the ptr with new ptr owned by crossbeam epoch Owned 
    // and we dont drop the guard immediately like drop(guard)
    // instead we do defer_destroy on the guard which essentially means destructor is stored and will be reclaimed after all the pinned guards have been dropped
    pub fn store(&self, item: T) {
        let boxed = Box::new(Arc::new(item));
        let old = self.current.swap(Box::into_raw(boxed), Ordering::Release);
        self.retired.lock().expect("cant hold").push(RetiredPtr(old));
        self.try_reclaim();
    }
    pub fn swap(&self, value: T) -> Arc<T> { 
        let boxed = Box::new(Arc::new(value));
        let old = self.current.swap(Box::into_raw(boxed), Ordering::Release);
        self.retired.lock().expect("cant hold from swap").push(RetiredPtr(old));
        let old_arc = unsafe { Arc::clone(&*old) };
        self.try_reclaim();
        old_arc
    }


    pub fn try_reclaim(&self) { 
        if self.active_readers.load(Ordering::Acquire) != 0 { 
            return;
        }
        for retired_ptr in self.retired.lock().expect("can not hold from reclaimn").drain(..) { 
            unsafe { drop(Box::from_raw(retired_ptr.0)); }
        }
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
        println!("swapping");
        let old = swap.swap(10);
        println!("Deref old");
        assert_eq!(*old, 5);
        assert_eq!(*swap.load(), 10);
    }

    #[test]
    pub fn reader_keeps_old_versions_alive() { 
        let swap = GSwap::new(10u64);
        let old = swap.load();
        swap.store(15u64);
        assert_eq!(*old, 10);
    }

    #[test]
    pub fn many_readers_one_writer() { 
        use std::thread;
        let swap = Arc::new(GSwap::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..8 { 
            let swap = Arc::clone(&swap );
            handles.push(thread::spawn(move || {
                for _ in 0..100_000 { 
                    let v = swap.load();
                    let _ = *v;
                }
            }));
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