use criterion::{Criterion, criterion_group, criterion_main};

fn throughput_benchmark(_c: &mut Criterion) {
    // TODO: Add benchmarks
}

criterion_group!(benches, throughput_benchmark);
criterion_main!(benches);
