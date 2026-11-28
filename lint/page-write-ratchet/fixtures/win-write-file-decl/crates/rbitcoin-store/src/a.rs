extern "system" {
    fn WriteFile(h: *mut u8, buf: *const u8, n: u32) -> i32;
}
fn f() { unsafe { WriteFile(h, ptr, len) }; }
