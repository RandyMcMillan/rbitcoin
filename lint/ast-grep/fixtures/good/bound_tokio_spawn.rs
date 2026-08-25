fn keep_handle() {
    let h = tokio::spawn(async {});
    let _ = h;
}
