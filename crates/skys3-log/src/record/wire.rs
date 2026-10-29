//! The primitive encodings the record bodies are built from.
//!
//! Integers are little-endian. A variable-length field is a length prefix
//! followed by that many bytes; an option is a presence byte (0 or 1)
//! followed by the value if present. The writer and the reader check the
//! same bounds, so the encoder never produces what the decoder rejects.

use skys3_types::{Epoch, EpochSeq, Seq};

use super::error::{FieldError, Problem};

/// Appends primitive values to an encoding buffer.
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

    /// Writes a presence byte.
    pub(crate) fn present(&mut self, present: bool) {
        self.u8(u8::from(present));
    }

    /// Writes a count or length as a `u8`, checking `min..=max`.
    pub(crate) fn len8(
        &mut self,
        field: &'static str,
        len: usize,
        min: usize,
        max: usize,
    ) -> Result<(), FieldError> {
        self.u8(prefix(field, len, min, max)?);
        Ok(())
    }

    /// Writes a count or length as a `u16`, checking `min..=max`.
    pub(crate) fn len16(
        &mut self,
        field: &'static str,
        len: usize,
        min: usize,
        max: usize,
    ) -> Result<(), FieldError> {
        self.u16(prefix(field, len, min, max)?);
        Ok(())
    }

    /// Writes a count or length as a `u32`, checking `min..=max`.
    pub(crate) fn len32(
        &mut self,
        field: &'static str,
        len: usize,
        min: usize,
        max: usize,
    ) -> Result<(), FieldError> {
        self.u32(prefix(field, len, min, max)?);
        Ok(())
    }

    /// Writes text with a `u8` length prefix.
    pub(crate) fn str8(
        &mut self,
        field: &'static str,
        text: &str,
        min: usize,
        max: usize,
    ) -> Result<(), FieldError> {
        self.len8(field, text.len(), min, max)?;
        self.raw(text.as_bytes());
        Ok(())
    }

    /// Writes text with a `u16` length prefix.
    pub(crate) fn str16(
        &mut self,
        field: &'static str,
        text: &str,
        min: usize,
        max: usize,
    ) -> Result<(), FieldError> {
        self.len16(field, text.len(), min, max)?;
        self.raw(text.as_bytes());
        Ok(())
    }
}

fn too_long(field: &'static str, len: usize, max: u64) -> FieldError {
    FieldError::new(
        field,
        Problem::TooLong {
            len: len as u64,
            max,
        },
    )
}

/// Checks `len` against `min..=max` and converts it to the prefix type.
fn prefix<T: TryFrom<usize>>(
    field: &'static str,
    len: usize,
    min: usize,
    max: usize,
) -> Result<T, FieldError> {
    check_len(field, len, min, max)?;
    T::try_from(len).map_err(|_| too_long(field, len, max as u64))
}

fn check_len(field: &'static str, len: usize, min: usize, max: usize) -> Result<usize, FieldError> {
    if len > max {
        return Err(too_long(field, len, max as u64));
    }
    if len < min {
        return Err(FieldError::new(field, Problem::Empty));
    }
    Ok(len)
}

/// Reads primitive values from a record's kind-specific header, checking
/// every length against the bytes that remain before using it.
pub(crate) struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { rest: bytes }
    }

    pub(crate) fn take(&mut self, field: &'static str, len: usize) -> Result<&'a [u8], FieldError> {
        if len > self.rest.len() {
            return Err(FieldError::new(field, Problem::Truncated));
        }
        let (taken, rest) = self.rest.split_at(len);
        self.rest = rest;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], FieldError> {
        let mut array = [0; N];
        array.copy_from_slice(self.take(field, N)?);
        Ok(array)
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

    /// Reads a presence byte.
    pub(crate) fn present(&mut self, field: &'static str) -> Result<bool, FieldError> {
        match self.u8(field)? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(FieldError::new(field, Problem::InvalidTag(tag))),
        }
    }

    /// Reads a `u8` count or length and checks it is within `min..=max`.
    pub(crate) fn len8(
        &mut self,
        field: &'static str,
        min: usize,
        max: usize,
    ) -> Result<usize, FieldError> {
        check_len(field, self.u8(field)?.into(), min, max)
    }

    /// Reads a `u16` count or length and checks it is within `min..=max`.
    pub(crate) fn len16(
        &mut self,
        field: &'static str,
        min: usize,
        max: usize,
    ) -> Result<usize, FieldError> {
        check_len(field, self.u16(field)?.into(), min, max)
    }

    /// Reads a `u32` count or length and checks it is within `min..=max`.
    pub(crate) fn len32(
        &mut self,
        field: &'static str,
        min: usize,
        max: usize,
    ) -> Result<usize, FieldError> {
        let len = self.u32(field)?;
        let len = usize::try_from(len).map_err(|_| too_long(field, usize::MAX, max as u64))?;
        check_len(field, len, min, max)
    }

    /// Checks that `count` entries of at least `min_entry_len` bytes each
    /// can fit in what remains, so a hostile count cannot make the caller
    /// reserve memory the record does not back.
    pub(crate) fn check_count(
        &self,
        field: &'static str,
        count: usize,
        min_entry_len: usize,
    ) -> Result<(), FieldError> {
        match count.checked_mul(min_entry_len) {
            Some(needed) if needed <= self.rest.len() => Ok(()),
            _ => Err(FieldError::new(field, Problem::Truncated)),
        }
    }

    fn text(field: &'static str, bytes: &[u8]) -> Result<String, FieldError> {
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| FieldError::new(field, Problem::NotUtf8))
    }

    /// Reads text with a `u8` length prefix.
    pub(crate) fn str8(
        &mut self,
        field: &'static str,
        min: usize,
        max: usize,
    ) -> Result<String, FieldError> {
        let len = self.len8(field, min, max)?;
        Self::text(field, self.take(field, len)?)
    }

    /// Reads text with a `u16` length prefix.
    pub(crate) fn str16(
        &mut self,
        field: &'static str,
        min: usize,
        max: usize,
    ) -> Result<String, FieldError> {
        let len = self.len16(field, min, max)?;
        Self::text(field, self.take(field, len)?)
    }

    /// Checks that every byte has been read.
    pub(crate) fn finish(self, field: &'static str) -> Result<(), FieldError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(FieldError::new(
                field,
                Problem::TrailingBytes(self.rest.len() as u64),
            ))
        }
    }
}
