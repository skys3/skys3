//! Fuzzes the intra-cluster frame parser, which reads frames from
//! authenticated but untrusted peers (design §12).

#![no_main]
#![forbid(unsafe_code)]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use skys3_net::{Frame, FrameError, MAX_HEADER_LEN, MAX_PAYLOAD_LEN, PREFIX_LEN, read_frame};

fuzz_target!(|data: &[u8]| {
    let decoded = Frame::decode(data);
    let mut reader = data;
    let read = runtime().block_on(read_frame(&mut reader));

    match decoded {
        // A decoded frame is within the limits, re-encodes, and decodes
        // back to itself; the stream reader reads the same frame.
        Ok(Some((frame, len))) => {
            assert!(len <= data.len());
            assert!(frame.payload.len() <= MAX_PAYLOAD_LEN as usize);
            let encoded = frame.encode().expect("a decoded frame encodes");
            assert!(encoded.len() <= PREFIX_LEN + MAX_HEADER_LEN as usize + frame.payload.len());
            assert_eq!(
                Frame::decode(&encoded).unwrap(),
                Some((frame.clone(), encoded.len()))
            );
            assert_eq!(read.unwrap(), Some(frame));
            assert_eq!(reader.len(), data.len() - len);
        }
        // An incomplete frame is a clean end only before its first byte.
        Ok(None) if data.is_empty() => assert!(matches!(read, Ok(None))),
        Ok(None) => assert!(matches!(read, Err(FrameError::Truncated)), "{read:?}"),
        // The stream reader refuses what the decoder refuses.
        Err(_) => assert!(read.is_err(), "{read:?}"),
    }
});

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime")
    })
}
