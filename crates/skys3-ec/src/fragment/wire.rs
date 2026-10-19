//! Little-endian primitives for fragment headers, with every length and
//! count checked against its bound and against the bytes that remain.

use std::collections::BTreeMap;

use skys3_log::record::{FieldError, Problem};
use skys3_types::{Epoch, EpochSeq, Seq};

/// Appends fields to a header buffer.
pub(crate) struct Writer<'a> {
    out: &'a mut Vec<u8>,
}

impl<'a> Writer<'a> {
    pub(crate) fn new(out: &'a mut Vec<u8>) -> Self {
        Self { out }
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.out.push(value);
    }

    pub(crate) fn u16(&mut self, value: u16) {
        self.out.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.out.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.out.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn position(&mut self, position: EpochSeq) {
        self.u64(position.epoch.get());
        self.u64(position.seq.get());
    }

    pub(crate) fn raw(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }

    /// Writes a `u8` count, checked against `min..=max`.
    pub(crate) fn count8(
        &mut self,
        field: &'static str,
        len: usize,
        min: usize,
        max: usize,
    ) -> Result<(), FieldError> {
        let len = check_len(field, len, min, max)?;
        self.u8(u8::try_from(len).map_err(|_| too_long(field, len, max))?);
        Ok(())
    }

    /// Writes a `u16` count, checked against `min..=max`.
    pub(crate) fn count16(
        &mut self,
        field: &'static str,
        len: usize,
        min: usize,
        max: usize,
    ) -> Result<(), FieldError> {
        let len = check_len(field, len, min, max)?;
        self.u16(u16::try_from(len).map_err(|_| too_long(field, len, max))?);
        Ok(())
    }

    /// Writes text with a `u8` length.
    pub(crate) fn str8(
        &mut self,
        field: &'static str,
        text: &str,
        min: usize,
        max: usize,
    ) -> Result<(), FieldError> {
        self.count8(field, text.len(), min, max)?;
        self.raw(text.as_bytes());
        Ok(())
    }

    /// Writes text with a `u16` length.
    pub(crate) fn str16(
        &mut self,
        field: &'static str,
        text: &str,
        min: usize,
        max: usize,
    ) -> Result<(), FieldError> {
        self.count16(field, text.len(), min, max)?;
        self.raw(text.as_bytes());
        Ok(())
    }
}

/// Reads fields from a header buffer.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// The bytes not read yet.
    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn take(&mut self, field: &'static str, len: usize) -> Result<&'a [u8], FieldError> {
        if len > self.bytes.len() {
            return Err(FieldError {
                field,
                problem: Problem::Truncated,
            });
        }
        let (taken, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], FieldError> {
        let bytes = self.take(field, N)?;
        // `take` returned exactly N bytes.
        Ok(bytes.try_into().unwrap_or([0; N]))
    }

    pub(crate) fn u8(&mut self, field: &'static str) -> Result<u8, FieldError> {
        Ok(self.array::<1>(field)?[0])
    }

    pub(crate) fn u16(&mut self, field: &'static str) -> Result<u16, FieldError> {
        self.array(field).map(u16::from_le_bytes)
    }

    pub(crate) fn u32(&mut self, field: &'static str) -> Result<u32, FieldError> {
        self.array(field).map(u32::from_le_bytes)
    }

    pub(crate) fn u64(&mut self, field: &'static str) -> Result<u64, FieldError> {
        self.array(field).map(u64::from_le_bytes)
    }

    pub(crate) fn position(&mut self, field: &'static str) -> Result<EpochSeq, FieldError> {
        let epoch = Epoch::new(self.u64(field)?);
        let seq = Seq::new(self.u64(field)?);
        Ok(EpochSeq::new(epoch, seq))
    }

    /// Reads a `u8` count and checks it against `min..=max`.
    pub(crate) fn count8(
        &mut self,
        field: &'static str,
        min: usize,
        max: usize,
    ) -> Result<usize, FieldError> {
        let len = usize::from(self.u8(field)?);
        check_len(field, len, min, max)
    }

    /// Reads a `u16` count and checks it against `min..=max`.
    pub(crate) fn count16(
        &mut self,
        field: &'static str,
        min: usize,
        max: usize,
    ) -> Result<usize, FieldError> {
        let len = usize::from(self.u16(field)?);
        check_len(field, len, min, max)
    }

    /// Reads text with a `u8` length.
    pub(crate) fn str8(
        &mut self,
        field: &'static str,
        min: usize,
        max: usize,
    ) -> Result<String, FieldError> {
        let len = self.count8(field, min, max)?;
        self.text(field, len)
    }

    /// Reads text with a `u16` length.
    pub(crate) fn str16(
        &mut self,
        field: &'static str,
        min: usize,
        max: usize,
    ) -> Result<String, FieldError> {
        let len = self.count16(field, min, max)?;
        self.text(field, len)
    }

    fn text(&mut self, field: &'static str, len: usize) -> Result<String, FieldError> {
        let bytes = self.take(field, len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| FieldError {
            field,
            problem: Problem::NotUtf8,
        })
    }

    /// Checks that every byte was read.
    pub(crate) fn finish(self, field: &'static str) -> Result<(), FieldError> {
        match self.bytes.len() {
            0 => Ok(()),
            extra => Err(FieldError {
                field,
                problem: Problem::TrailingBytes(extra as u64),
            }),
        }
    }
}

/// Inserts a decoded map entry, requiring keys in strictly increasing
/// order so that every map has exactly one encoding.
pub(crate) fn insert_sorted<K: Ord, V>(
    field: &'static str,
    map: &mut BTreeMap<K, V>,
    key: K,
    value: V,
) -> Result<(), FieldError> {
    if map.last_key_value().is_some_and(|(last, _)| *last >= key) {
        return Err(FieldError {
            field,
            problem: Problem::Unsorted,
        });
    }
    map.insert(key, value);
    Ok(())
}

/// The error for a field whose value is not valid for its type.
pub(crate) fn invalid(field: &'static str, reason: impl ToString) -> FieldError {
    FieldError {
        field,
        problem: Problem::Invalid(reason.to_string()),
    }
}

/// The error for a field that contradicts another.
pub(crate) fn inconsistent(field: &'static str, reason: &'static str) -> FieldError {
    FieldError {
        field,
        problem: Problem::Inconsistent(reason),
    }
}

fn too_long(field: &'static str, len: usize, max: usize) -> FieldError {
    FieldError {
        field,
        problem: Problem::TooLong {
            len: len as u64,
            max: max as u64,
        },
    }
}

fn check_len(field: &'static str, len: usize, min: usize, max: usize) -> Result<usize, FieldError> {
    if len > max {
        return Err(too_long(field, len, max));
    }
    if len < min {
        return Err(FieldError {
            field,
            problem: Problem::Empty,
        });
    }
    Ok(len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_text_are_bounded_both_ways() {
        let mut out = Vec::new();
        let mut w = Writer::new(&mut out);
        assert_eq!(
            w.count8("f", 300, 0, 400).unwrap_err().problem,
            Problem::TooLong { len: 300, max: 400 }
        );
        assert_eq!(w.str16("f", "", 1, 9).unwrap_err().problem, Problem::Empty);
        assert_eq!(
            w.str8("f", "abc", 0, 2).unwrap_err().problem,
            Problem::TooLong { len: 3, max: 2 }
        );
        w.str8("f", "ab", 0, 2).unwrap();
        w.u32(7);
        w.raw(&[0xff, 0xfe]);
        let mut r = Reader::new(&out);
        assert_eq!(r.str8("f", 0, 2).unwrap(), "ab");
        assert_eq!(r.u32("f").unwrap(), 7);
        assert_eq!(r.remaining(), 2);
        let mut bad = Reader::new(&out[3..]);
        assert_eq!(bad.u64("f").unwrap_err().problem, Problem::Truncated);
        let mut utf8 = Reader::new(&[2, 0xff, 0xfe]);
        assert_eq!(utf8.str8("f", 0, 9).unwrap_err().problem, Problem::NotUtf8);
        let mut short = Reader::new(&[3, b'a']);
        assert_eq!(
            short.str8("f", 0, 9).unwrap_err().problem,
            Problem::Truncated
        );
        let mut big = Reader::new(&[3, 0]);
        assert_eq!(
            big.count16("f", 0, 2).unwrap_err().problem,
            Problem::TooLong { len: 3, max: 2 }
        );
        assert_eq!(
            Reader::new(&[1]).finish("f").unwrap_err().problem,
            Problem::TrailingBytes(1)
        );
    }

    #[test]
    fn maps_must_be_sorted() {
        let mut map = BTreeMap::new();
        insert_sorted("f", &mut map, 2, ()).unwrap();
        for key in [1, 2] {
            assert_eq!(
                insert_sorted("f", &mut map, key, ()).unwrap_err().problem,
                Problem::Unsorted
            );
        }
        assert_eq!(
            inconsistent("f", "why").to_string(),
            "f: is inconsistent: why"
        );
        assert_eq!(invalid("f", "bad").to_string(), "f: is invalid: bad");
    }
}
