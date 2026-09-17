#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

#[cfg(test)]
mod tests {
    #[test]
    fn crate_metadata_is_available() {
        assert_eq!(env!("CARGO_PKG_NAME"), "skys3");
    }
}
