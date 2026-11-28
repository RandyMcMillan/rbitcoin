extern "system" {
    fn  WriteFile(h: *mut u8, buf: *const u8, n: u32) -> i32;
}
pub(crate) fn
win_set_eof(h: *mut u8, len: u64) -> bool {
    true
}
fn f() { unsafe { WriteFile(h, ptr, len) }; let _ = win_set_eof(h, 0); }
