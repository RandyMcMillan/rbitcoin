mod tests {
    fn fixture_only() {
        std::mem::forget(1u8);
        std::thread::spawn(|| {});
    }
}
