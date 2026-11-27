use std::os::unix::fs::FileExt;

fn f(file: &std::fs::File, bytes: &[u8]) {
    file.write_at(bytes, 0).ok();
}
