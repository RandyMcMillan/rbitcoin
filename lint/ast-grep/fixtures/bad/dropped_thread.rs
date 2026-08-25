fn leak_thread() {
    std::thread::spawn(|| {});
}

fn leak_thread_short() {
    thread::spawn(|| {});
}
