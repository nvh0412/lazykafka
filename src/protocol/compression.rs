//! Decompression of the records section of a v2 record batch.
//!
//! Codec ids come from the low 3 bits of the batch `attributes`:
//! 0 = none, 1 = gzip, 2 = snappy, 3 = lz4, 4 = zstd.

use std::io::Read;

use crate::error::{KafkaError, Result};

/// Upper bound for decompressed output, to guard against decompression bombs.
pub const MAX_DECOMPRESSED_SIZE: usize = 64 * 1024 * 1024;

/// Header of the Xerial snappy framing written by the Java producer:
/// `\x82SNAPPY\0` + version (i32) + compatible version (i32), then chunks of
/// `[i32 BE length][raw snappy block]`.
const XERIAL_MAGIC: &[u8; 8] = b"\x82SNAPPY\0";
const XERIAL_HEADER_LEN: usize = 16;

pub fn codec_name(codec: i16) -> &'static str {
    match codec {
        0 => "none",
        1 => "gzip",
        2 => "snappy",
        3 => "lz4",
        4 => "zstd",
        _ => "unknown",
    }
}

pub fn decompress(codec: i16, input: &[u8]) -> Result<Vec<u8>> {
    let result = match codec {
        0 => return Ok(input.to_vec()),
        1 => read_limited(flate2::read::GzDecoder::new(input)),
        2 => return decompress_snappy(input),
        3 => read_limited(lz4_flex::frame::FrameDecoder::new(input)),
        4 => {
            let decoder = ruzstd::decoding::StreamingDecoder::new(input)
                .map_err(|e| KafkaError::DecodeError(format!("zstd: invalid frame: {}", e)))?;
            read_limited(decoder)
        }
        other => {
            return Err(KafkaError::DecodeError(format!(
                "unsupported compression codec {}",
                other
            )));
        }
    };

    result.map_err(|e| match e {
        KafkaError::DecodeError(msg) => {
            KafkaError::DecodeError(format!("{}: {}", codec_name(codec), msg))
        }
        other => other,
    })
}

/// Read a decoder to the end, failing if it produces more than the size cap.
fn read_limited<R: Read>(reader: R) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut limited = reader.take(MAX_DECOMPRESSED_SIZE as u64 + 1);
    limited
        .read_to_end(&mut out)
        .map_err(|e| KafkaError::DecodeError(format!("decompression failed: {}", e)))?;
    if out.len() > MAX_DECOMPRESSED_SIZE {
        return Err(KafkaError::DecodeError(format!(
            "decompressed size exceeds {} bytes",
            MAX_DECOMPRESSED_SIZE
        )));
    }
    Ok(out)
}

fn snappy_block(block: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let len = snap::raw::decompress_len(block)
        .map_err(|e| KafkaError::DecodeError(format!("snappy: {}", e)))?;
    if out.len().saturating_add(len) > MAX_DECOMPRESSED_SIZE {
        return Err(KafkaError::DecodeError(format!(
            "snappy: decompressed size exceeds {} bytes",
            MAX_DECOMPRESSED_SIZE
        )));
    }
    let decoded = snap::raw::Decoder::new()
        .decompress_vec(block)
        .map_err(|e| KafkaError::DecodeError(format!("snappy: {}", e)))?;
    out.extend_from_slice(&decoded);
    Ok(())
}

fn decompress_snappy(input: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();

    if input.starts_with(XERIAL_MAGIC) {
        let mut rest = input.get(XERIAL_HEADER_LEN..).ok_or_else(|| {
            KafkaError::DecodeError("snappy: truncated xerial header".to_string())
        })?;
        while !rest.is_empty() {
            if rest.len() < 4 {
                return Err(KafkaError::DecodeError(
                    "snappy: truncated xerial chunk length".to_string(),
                ));
            }
            let chunk_len = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
            rest = &rest[4..];
            let chunk_len = usize::try_from(chunk_len).map_err(|_| {
                KafkaError::DecodeError("snappy: negative xerial chunk length".to_string())
            })?;
            let chunk = rest.get(..chunk_len).ok_or_else(|| {
                KafkaError::DecodeError("snappy: xerial chunk exceeds input".to_string())
            })?;
            snappy_block(chunk, &mut out)?;
            rest = &rest[chunk_len..];
        }
    } else {
        snappy_block(input, &mut out)?;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const PAYLOAD: &[u8] = b"hello kafka hello kafka hello kafka compression test payload";

    #[test]
    fn none_passthrough() {
        assert_eq!(decompress(0, PAYLOAD).unwrap(), PAYLOAD);
    }

    #[test]
    fn gzip_roundtrip() {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(PAYLOAD).unwrap();
        let compressed = enc.finish().unwrap();
        assert_eq!(decompress(1, &compressed).unwrap(), PAYLOAD);
    }

    #[test]
    fn snappy_raw_roundtrip() {
        let compressed = snap::raw::Encoder::new().compress_vec(PAYLOAD).unwrap();
        assert_eq!(decompress(2, &compressed).unwrap(), PAYLOAD);
    }

    #[test]
    fn snappy_xerial_roundtrip() {
        let mut framed = Vec::new();
        framed.extend_from_slice(XERIAL_MAGIC);
        framed.extend_from_slice(&1i32.to_be_bytes());
        framed.extend_from_slice(&1i32.to_be_bytes());
        for chunk in [&PAYLOAD[..20], &PAYLOAD[20..]] {
            let c = snap::raw::Encoder::new().compress_vec(chunk).unwrap();
            framed.extend_from_slice(&(c.len() as i32).to_be_bytes());
            framed.extend_from_slice(&c);
        }
        assert_eq!(decompress(2, &framed).unwrap(), PAYLOAD);
    }

    #[test]
    fn snappy_xerial_bad_chunk_length_errors() {
        let mut framed = Vec::new();
        framed.extend_from_slice(XERIAL_MAGIC);
        framed.extend_from_slice(&[0; 8]);
        framed.extend_from_slice(&1000i32.to_be_bytes());
        framed.extend_from_slice(&[1, 2, 3]);
        assert!(decompress(2, &framed).is_err());

        let mut neg = Vec::new();
        neg.extend_from_slice(XERIAL_MAGIC);
        neg.extend_from_slice(&[0; 8]);
        neg.extend_from_slice(&(-1i32).to_be_bytes());
        assert!(decompress(2, &neg).is_err());
    }

    #[test]
    fn lz4_roundtrip() {
        let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
        enc.write_all(PAYLOAD).unwrap();
        let compressed = enc.finish().unwrap();
        assert_eq!(decompress(3, &compressed).unwrap(), PAYLOAD);
    }

    #[test]
    fn zstd_decodes_known_frame() {
        // A zstd frame holding a raw (uncompressed) block with the content "hello zstd",
        // built by hand so the test needs no zstd encoder:
        // magic, frame header (single segment, 1-byte content size), block header, data.
        let content = b"hello zstd";
        let mut frame = vec![0x28, 0xB5, 0x2F, 0xFD, 0x20, content.len() as u8];
        // block header: last_block=1, type=raw(0), size=len  => (size << 3) | 1
        let header = ((content.len() as u32) << 3) | 1;
        frame.extend_from_slice(&header.to_le_bytes()[..3]);
        frame.extend_from_slice(content);
        assert_eq!(decompress(4, &frame).unwrap(), content);
    }

    #[test]
    fn garbage_input_errors_not_panics() {
        for codec in 1..=4 {
            assert!(decompress(codec, &[0xde, 0xad, 0xbe, 0xef, 0x01, 0x02]).is_err());
            assert!(decompress(codec, &[]).is_err() || codec == 1 || codec == 3 || codec == 4);
        }
    }

    #[test]
    fn unknown_codec_errors() {
        assert!(decompress(7, PAYLOAD).is_err());
    }
}
