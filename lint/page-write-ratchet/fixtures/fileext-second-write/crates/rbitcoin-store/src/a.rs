use std::os::unix::fs::FileExt;

fn f(file: &std::fs::File, bytes: &[u8]) {
    file.write_at(bytes, 0).ok();
}

fn g(file: &std::fs::File, bytes: &[u8]) {
    file.write_all_at(bytes, 4096).ok();
}
