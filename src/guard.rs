use crate::gen_lock::GenLock;

pub(crate) struct Guard<'a> { 
    gen_lock: &'a GenLock,
    generation: usize
}

impl<'a> Guard<'a> { 
    pub fn new(gen_lock: &'a GenLock) -> Self { 
        let generation = gen_lock.entry();
        Self {
            gen_lock, 
            generation
        }
    }
}

impl<'a> Drop for Guard<'a> {
    fn drop(&mut self) {
        self.gen_lock.exit(self.generation);
    }
}

#[cfg(test)]
mod tests {
    use crate::{gen_lock::GenLock, guard::Guard};
 
    #[test]
    pub fn guard_release_reader() { 
        let lock = GenLock::new();
        { 
            let _guard = Guard::new(&lock);
        }
    }
}