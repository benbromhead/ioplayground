use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes, BytesMut};
use itertools::Itertools;
use redis_protocol::prelude::*;
use std::collections::HashMap;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, info, trace, warn};


#[derive(Debug, Clone)]
pub struct RedisCodec {
    // Redis doesn't have an explicit "Response" type as part of the protocol
    decode_as_response: bool,
    batch_hint: usize,
    current_frames: Vec<Frame>,
}

impl RedisCodec {

    pub fn new(decode_as_response: bool, batch_hint: usize) -> RedisCodec {
        RedisCodec {
            decode_as_response,
            batch_hint,
            current_frames: vec![],
        }
    }

    pub fn get_batch_hint(&self) -> usize {
        self.batch_hint
    }

    fn decode_raw(&mut self, src: &mut BytesMut) -> Result<Option<Vec<Frame>>> {
        // TODO: get_batch_hint may be a premature optimisation
        while src.remaining() != 0 {
            trace!("remaining {}", src.remaining());

            match decode_bytes(&*src).map_err(|e| {
                info!("Error decoding redis frame {:?}", e);
                anyhow!("Error decoding redis frame {}", e)
            })? {
                (Some(frame), size) => {
                    trace!("Got frame {:?}", frame);
                    src.advance(size);
                    self.current_frames.push(frame);
                }
                (None, _) => {
                    if src.remaining() == 0 {
                        break;
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
        trace!(
            "frames {:?} - remaining {}",
            self.current_frames,
            src.remaining()
        );

        let mut return_buf: Vec<Frame> = vec![];
        std::mem::swap(&mut self.current_frames, &mut return_buf);

        if !return_buf.is_empty() {
            trace!("Batch size {:?}", return_buf.len());
            return Ok(Some(return_buf));
        }

        Ok(None)
    }

    fn encode_raw(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        encode_bytes(dst, &item)
            .map(|_| ())
            .map_err(|e| anyhow!("Uh - oh {} - {:#?}", e, item))
    }
}

impl Decoder for RedisCodec {
    type Item = Vec<Frame>;
    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        self.decode_raw(src)
    }
}

impl Encoder<Vec<Frame>> for RedisCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Vec<Frame>,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        item.into_iter().try_for_each(|m: Frame| {
            self.encode_raw(m, dst)
        })
        // .collect()
    }
}
