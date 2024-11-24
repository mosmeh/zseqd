use bstr::ByteSlice;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{io::Write, net::SocketAddr, num::ParseIntError, str::FromStr};
use tokio_util::codec::{Decoder, Encoder};

#[derive(thiserror::Error, Debug)]
pub enum RedisError {
    #[error("ERR {0}")]
    Response(String),

    #[error("MOVED {slot} {addr}")]
    Moved { slot: u16, addr: SocketAddr },
}

impl From<&str> for RedisError {
    fn from(s: &str) -> Self {
        Self::Response(s.to_string())
    }
}

impl From<String> for RedisError {
    fn from(s: String) -> Self {
        Self::Response(s)
    }
}

impl From<ParseIntError> for RedisError {
    fn from(err: ParseIntError) -> Self {
        Self::Response(err.to_string())
    }
}

impl From<anyhow::Error> for RedisError {
    fn from(err: anyhow::Error) -> Self {
        Self::Response(err.to_string())
    }
}

impl From<etcd_client::Error> for RedisError {
    fn from(err: etcd_client::Error) -> Self {
        Self::Response(err.to_string())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum RedisValue {
    SimpleString(&'static str),
    Integer(i64),
}

impl From<&'static str> for RedisValue {
    fn from(value: &'static str) -> Self {
        Self::SimpleString(value)
    }
}

impl From<i64> for RedisValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RespError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("invalid multibulk length")]
    InvalidMultibulkLength,

    #[error("invalid bulk length")]
    InvalidBulkLength,

    #[error("expected '{}', got '{}'", char::from(*.expected), char::from(*.got))]
    UnexpectedByte { expected: u8, got: u8 },

    #[error("unbalanced quotes in request")]
    UnbalancedQuotes,
}

#[derive(Default)]
pub struct RespCodec {
    multibulk_array_len: Option<usize>,
    multibulk_array: Vec<Bytes>,
}

impl RespCodec {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Decoder for RespCodec {
    type Item = Vec<Bytes>;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        if self.multibulk_array_len.is_some() || src[0] == b'*' {
            // We will resume or start decoding multibulk.
            // Single command in multibulk can be parsed across multiple calls
            // to decode_multibulk().
            self.decode_multibulk(src)
        } else {
            // Single command (line) in inline protocol is always parsed in
            // single call to decode_inline().
            decode_inline(src)
        }
        .map_err(Into::into)
    }
}

impl RespCodec {
    fn decode_multibulk(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<Vec<Bytes>>, ProtocolError> {
        fn parse_bytes<T: FromStr>(s: &[u8]) -> Result<T, ()> {
            s.to_str().map_err(|_| ())?.parse().map_err(|_| ())
        }

        let array_len = match self.multibulk_array_len {
            None => {
                let header_end = match src.find_byte(b'\r') {
                    Some(end) if end + 1 < src.len() => end, // if there is a room for trailing \n
                    _ => return Ok(None),
                };
                let len: i32 = match &src[..header_end] {
                    [] => 0,
                    [b'*', len_bytes @ ..] => {
                        parse_bytes(len_bytes).map_err(|_| ProtocolError::InvalidMultibulkLength)?
                    }
                    _ => return Err(ProtocolError::InvalidMultibulkLength),
                };
                src.advance(header_end + 2); // 2 for skipping \r\n
                if len <= 0 {
                    return Ok(Some(Vec::new()));
                }

                let array_len = len as usize;
                self.multibulk_array_len = Some(array_len);
                assert!(self.multibulk_array.is_empty());
                self.multibulk_array.reserve(array_len);
                array_len
            }
            Some(array_len) => array_len,
        };

        for _ in self.multibulk_array.len()..array_len {
            let header_end = match src.find_byte(b'\r') {
                Some(end) if end + 1 < src.len() => end,
                _ => return Ok(None),
            };
            let [b'$', len_bytes @ ..] = &src[..header_end] else {
                return Err(ProtocolError::UnexpectedByte {
                    expected: b'$',
                    got: src[0],
                });
            };
            let len: usize =
                parse_bytes(len_bytes).map_err(|_| ProtocolError::InvalidBulkLength)?;
            let header_line_end = header_end + 2;
            let bulk_line_end = header_line_end + len + 2;
            if bulk_line_end > src.len() {
                src.reserve(bulk_line_end - src.len());
                return Ok(None);
            }
            src.advance(header_line_end);
            self.multibulk_array.push(src.split_to(len).freeze());
            src.advance(2);
        }

        self.multibulk_array_len = None;
        Ok(Some(std::mem::take(&mut self.multibulk_array)))
    }
}

fn decode_inline(src: &mut BytesMut) -> Result<Option<Vec<Bytes>>, ProtocolError> {
    let Some(end) = src.find_byte(b'\n') else {
        return Ok(None);
    };
    let tokens = match split_args(&src[..end]) {
        Ok(tokens) => tokens,
        Err(SplitArgsError::UnbalancedQuotes) => return Err(ProtocolError::UnbalancedQuotes),
    };
    let tokens = tokens.into_iter().map(Bytes::from).collect();
    src.advance(end + 1);
    Ok(Some(tokens))
}

impl Encoder<Result<RedisValue, RedisError>> for RespCodec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: Result<RedisValue, RedisError>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        encode(&mut dst.writer(), &item)
    }
}

fn encode<W: Write>(writer: &mut W, value: &Result<RedisValue, RedisError>) -> std::io::Result<()> {
    match value {
        Ok(RedisValue::SimpleString(s)) => {
            writer.write_all(b"+")?;
            writer.write_all(s.as_bytes())?;
            writer.write_all(b"\r\n")
        }
        Ok(RedisValue::Integer(i)) => {
            write!(writer, ":{}\r\n", i)
        }
        Err(err) => {
            write!(writer, "-{}\r\n", err)
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum SplitArgsError {
    #[error("unbalanced quotes")]
    UnbalancedQuotes,
}

fn split_args(mut line: &[u8]) -> Result<Vec<Vec<u8>>, SplitArgsError> {
    enum State {
        Normal,
        InDoubleQuotes,
        InSingleQuotes,
    }

    let mut tokens = Vec::new();
    loop {
        match line.find_not_byteset(b" \n\r\t\x0b\x0c") {
            Some(i) => line = &line[i..],
            None => return Ok(tokens), // line is empty, or line consists only of space characters
        }
        let mut state = State::Normal;
        let mut current = Vec::new();
        loop {
            match state {
                State::Normal => match line.first() {
                    Some(b' ' | b'\n' | b'\r' | b'\t') => {
                        line = &line[1..];
                        break;
                    }
                    None => break,
                    Some(b'"') => state = State::InDoubleQuotes,
                    Some(b'\'') => state = State::InSingleQuotes,
                    Some(ch) => current.push(*ch),
                },
                State::InDoubleQuotes => match line {
                    [b'\\', b'x', a, b, _rest @ ..]
                        if a.is_ascii_hexdigit() && b.is_ascii_hexdigit() =>
                    {
                        current.push(hex_digit_to_int(*a) * 16 + hex_digit_to_int(*b));
                        line = &line[3..];
                    }
                    [b'\\', ch, _rest @ ..] => {
                        let byte = match *ch {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x8,
                            b'a' => 0x7,
                            ch => ch,
                        };
                        current.push(byte);
                        line = &line[1..];
                    }
                    [b'"', next, _rest @ ..] if !is_space(*next) => {
                        return Err(SplitArgsError::UnbalancedQuotes)
                    }
                    [b'"', _rest @ ..] => {
                        line = &line[1..];
                        break;
                    }
                    [] => return Err(SplitArgsError::UnbalancedQuotes),
                    [ch, _rest @ ..] => current.push(*ch),
                },
                State::InSingleQuotes => match line {
                    [b'\\', b'\'', _rest @ ..] => {
                        current.push(b'\'');
                        line = &line[1..];
                    }
                    [b'\'', next, _rest @ ..] if !is_space(*next) => {
                        return Err(SplitArgsError::UnbalancedQuotes)
                    }
                    [b'\'', _rest @ ..] => {
                        line = &line[1..];
                        break;
                    }
                    [] => return Err(SplitArgsError::UnbalancedQuotes),
                    [ch, _rest @ ..] => current.push(*ch),
                },
            }
            line = &line[1..];
        }
        tokens.push(current);
    }
}

// isspace() in C
const fn is_space(ch: u8) -> bool {
    matches!(ch, b' ' | b'\n' | b'\r' | b'\t' | 0xb | 0xc)
}

fn hex_digit_to_int(ch: u8) -> u8 {
    match ch {
        b'0'..=b'9' => ch - b'0',
        b'a'..=b'f' => ch - b'a' + 10,
        b'A'..=b'F' => ch - b'A' + 10,
        _ => unreachable!(),
    }
}
