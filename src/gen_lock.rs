use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct GenLock { 
    generation: AtomicUsize,
    readers_even: AtomicUsize, 
    readers_odd: AtomicUsize
}

impl GenLock { 
    pub fn new() -> Self { 
        Self { 
            generation: AtomicUsize::new(0),
            readers_even: AtomicUsize::new(0),
            readers_odd: AtomicUsize::new(0)
        }
    }

    pub fn entry(&self) -> usize { 
        let generation = self.generation.load(Ordering::Acquire);
        if generation % 2 == 0 {
            self.readers_even.fetch_add(1, Ordering::Release);
        } else { 
            self.readers_odd.fetch_add(1, Ordering::Release);
        }
        generation
    }

    pub fn exit(&self, generation: usize)  { 
        if generation % 2 == 0 {
            self.readers_even.fetch_sub(1, Ordering::Release);
        } else { 
            self.readers_odd.fetch_sub(1, Ordering::Release);
        }
    }  


    pub fn flip_generation(&self) -> usize { 
        self.generation.fetch_add(1, Ordering::Release)
    } 

    pub fn wait_for_readers(&self) { 
        let generation = self.generation.load(Ordering::Acquire);
        if generation % 2 == 0 {
            if self.readers_even.load(Ordering::Acquire) != 0 { 
                std::hint::spin_loop();
            }
        } else {
            if self.readers_odd.load(Ordering::Acquire) != 0 { 
                std::hint::spin_loop();
            } 
        }        
    }
}