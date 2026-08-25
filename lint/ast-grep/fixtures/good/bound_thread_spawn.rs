fn join_thread() {
    let h = std::thread::spawn(|| {});
    h.join().unwrap();
}

fn join_short() {
    let h = thread::spawn(|| {});
    h.join().unwrap();
}
