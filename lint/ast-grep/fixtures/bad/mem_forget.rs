fn forget_bytes(x: Vec<u8>) {
    std::mem::forget(x);
}

fn leak_box(x: Box<u8>) {
    Box::leak(x);
}
