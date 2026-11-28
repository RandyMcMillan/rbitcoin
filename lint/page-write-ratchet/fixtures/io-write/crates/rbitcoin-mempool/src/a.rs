fn f() {
    file.seek(SeekFrom::Start(off))?;
    file.write(&bytes)?;
}
