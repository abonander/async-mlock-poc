use async_mlock_poc::MmapFile;
use criterion::{criterion_group, criterion_main, Criterion};
use std::io::Write;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio::runtime::Runtime;

const FILE_SIZE: usize = 64 * 1024;

fn bench_tokio_file(c: &mut Criterion) {
    let file = generate_file();

    c.bench_function("tokio::fs::File", |b| {
        let runtime = new_runtime_current_thread();

        b.to_async(runtime).iter(|| async {
            let file = tokio::fs::File::open(file.path())
                .await
                .expect("failed to open file");

            bench_buf_read(BufReader::new(file)).await;
        });
    });
}

fn bench_mmap_file(c: &mut Criterion) {
    let file = generate_file();

    c.bench_function("MmapFile", |b| {
        let runtime = new_runtime_current_thread();

        b.to_async(runtime).iter(|| async {
            let file = MmapFile::open(file.path())
                .await
                .expect("failed to open file");

            bench_buf_read(file).await;
        });
    });
}

fn generate_file() -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("failed to create tempfile");

    let output = (0u8..=255).cycle().take(FILE_SIZE).collect::<Vec<_>>();

    file.write_all(&output)
        .expect("failed to write to tempfile");
    file.flush().expect("failed to flush");

    file
}

async fn bench_buf_read<R: AsyncBufRead + Unpin>(mut read: R) {
    let mut bytes = (0u8..=255).cycle().enumerate();

    loop {
        let buf = read.fill_buf().await.expect("fill_buf error");

        if buf.is_empty() { return; }

        let len = buf.len();

        for (&b_read, (i, b)) in buf.iter().zip(&mut bytes) {
            assert_eq!(b, b_read, "mismatch at byte {i}");
        }

        read.consume(len);
    }
}

fn new_runtime_current_thread() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build runtime")
}

criterion_group!(benches, bench_tokio_file, bench_mmap_file);
criterion_main!(benches);
