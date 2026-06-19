use std::sync::{Arc, atomic::{AtomicPtr, AtomicUsize, Ordering}};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};


#[derive(Clone)]
pub (crate) struct GSwap<T> { 
    ptr: Atomic<Arc<T>>
}

pub struct Inner<T> { 
    strong: AtomicUsize,
    weak : AtomicUsize,
    data: T
}

pub(crate) struct ReadGuard<'g, T> { 
    shared: Shared<'g, T>,
    guard: Guard
}

// impl<T> Drop for ReadGuard<T> {
//     fn drop(&mut self) {
//         unsafe { (*self.readers).fetch_sub(1, Ordering::SeqCst); }
//     }
// }

// impl<T> std::ops::Deref for ReadGuard<T> {
//     type Target = T;

//     fn deref(&self) -> &Self::Target {
//         &*self.arc
//     }
// }


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
    
    // pub fn read(&self) -> ReadGuard<T> {
    //     self.readers.fetch_add(1, Ordering::SeqCst); 
    //     let ptr = self.item.load(Ordering::SeqCst);
    //     let arc = unsafe { &*ptr.clone()};
    //     ReadGuard { 
    //         arc: arc.clone(),
    //         readers: &self.readers
    //     }
    // }

    // pub fn write(&self, item: T)  { 
    //     // TODO: first check all the readers are dropped or not,
    //     let new_boxed = Box::new(Arc::new(item));
    //     let ptr = Box::into_raw(new_boxed);
    //     let old = self.item.swap(ptr, Ordering::SeqCst);
    //     while self.readers.load(Ordering::Acquire) != 0 { 
    //         std::hint::spin_loop();
    //     }
    //     unsafe { 
    //         drop(Box::from_raw(old));
    //     }
    // }
}


#[cfg(test)]
mod tests {
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
        let swap = GSwap::new(0u64);
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

    // #[test]
    // pub fn single_reader_single_writer() { 
    //     let gs = GSwap::new(5 as u8);
    //     let item = gs.read().arc.clone();
    //     println!("item: {}", item); 

    //     // write to it now and then read
    //     gs.write(6);
    //     let another = gs.read().arc.clone();
    //     println!("item: {}", another); 
    
    // }
    // #[test]
    // pub fn concurrent_readers_and_writers() { 
    //     let gs = GSwap::new(5 as u8);
    //     let item = gs.read().arc.clone();
    //     println!("item: {}", item); 
    //     std::thread::spawn(move || { 
    //         let item = gs.read().arc.clone();
    //     });
    //     // write to it now and then read
    //     gs.write(6);
    //     let another = gs.read().arc.clone();
    //     println!("item: {}", another); 
    
    // }

    
    
}