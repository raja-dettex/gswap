use criterion::{criterion_group, criterion_main, Criterion};


pub fn load_benchmark(c: &mut Criterion) { 
    let swap = gswap::GSwap::new(42u64);
    let b = c.bench_function("gwap_load", |b| { 
        b.iter(|| { 
            swap.load()
        });
    });
}

criterion_group!(benches, load_benchmark);
criterion_main!(benches);