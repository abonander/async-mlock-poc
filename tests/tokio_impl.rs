use std::io::Write;
use tempfile::NamedTempFile;
use tokio::io::AsyncBufReadExt;
use tokio::runtime::Runtime;
use async_mlock_poc::MmapFile;

const FILE_SIZE: usize = 64 * 1024;

const ITERATIONS: usize = 1000;

#[test]
fn test_async_buf_read() {
    let mut file = NamedTempFile::new().expect("failed to create tempfile");

    let output = (0u8..=255).cycle().take(FILE_SIZE).collect::<Vec<_>>();

    file.write_all(&output)
        .expect("failed to write to tempfile");
    file.flush().expect("failed to flush");

    for _ in 0 .. ITERATIONS {
        Runtime::new().unwrap().block_on(async {
            let mut file = MmapFile::open(file.path())
                .await
                .expect("failed to open file");

            let mut bytes = (0u8..=255).cycle().enumerate();

            loop {
                let buf = file.fill_buf().await.expect("fill_buf error");

                if buf.is_empty() { return; }

                let len = buf.len();

                for (&b_read, (i, b)) in buf.iter().zip(&mut bytes) {
                    assert_eq!(b, b_read, "mismatch at byte {i}");
                }

                file.consume(len);
            }
        })
    }
}
