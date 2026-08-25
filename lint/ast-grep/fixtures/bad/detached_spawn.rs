fn leak_task() {
    tokio::spawn(async {});
}

fn discard_handle() {
    let _ = tokio::spawn(async {});
}
