fn f() {
    let g = self.inner.write();
    let h = std::sync::RwLock::write(&lock);
    opts.read(true).write(true);
    let w = self.write_guard();
}
