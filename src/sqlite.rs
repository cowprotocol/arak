// TODO:
// - Implement `Database` trait.

#[cfg(test)]
mod tests {
    #[test]
    fn rusqlite_works() {
        rusqlite::Connection::open_in_memory().unwrap();
    }
}
