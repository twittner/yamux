// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

#![type_length_limit="3561018"]

use async_std::{net::{TcpStream, TcpListener}, task};
use bytes::Bytes;
use futures::{future, prelude::*};
use futures_codec::{BytesCodec, Framed};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use rand::Rng;
use std::{fmt::Debug, io, net::{Ipv4Addr, SocketAddr, SocketAddrV4}};
use yamux::{Config, Connection, ConnectionError, Mode, RemoteControl};

#[test]
fn prop_send_recv() {
    let _ = env_logger::try_init();

    async fn property(msgs: Vec<Msg>) -> Result<TestResult, ConnectionError> {
        let num_requests = msgs.len();
        let iter = msgs.into_iter().map(Bytes::from);
        let (listener, address) = bind().await?;
        let server = server(listener);
        let client = client(address, iter.clone());
        let r = futures::try_join!(server, client)?.1;
        Ok(TestResult::from_bool(r.len() == num_requests && r.into_iter().eq(iter)))
    }

    async fn server(listener: TcpListener) -> Result<(), ConnectionError> {
        let (socket, _) = listener.accept().await?;
        let connection = Connection::new(socket, Config::default(), Mode::Server);
        repeat_echo(connection, 1).await
    }

    async fn client<I>(address: SocketAddr, iter: I) -> Result<Vec<Bytes>, ConnectionError>
    where
        I: Iterator<Item = Bytes>
    {
        let socket = TcpStream::connect(address).await?;
        let connection = Connection::new(socket, Config::default(), Mode::Client);
        let control = connection.remote_control();
        task::spawn(yamux::into_stream(connection).for_each(|_| future::ready(())));
        send_recv(control, iter).await
    }

    fn await_property_result(msgs: Vec<Msg>) -> TestResult {
        if msgs.is_empty() {
            return TestResult::discard()
        }
        task::block_on(async move {
            match property(msgs).await {
                Ok(r) => r,
                Err(e) => {
                    log::error!("prop_send_recv error: {:?}", e);
                    TestResult::error(e.to_string())
                }
            }
        })
    }

    QuickCheck::new().tests(1).quickcheck(await_property_result as fn(_) -> _)
}

//#[test]
//fn prop_max_streams() {
//    fn prop(n: usize) -> bool {
//        let (l, a) = bind();
//        let mut cfg = Config::default();
//        let max_streams = n % 100;
//        cfg.set_max_num_streams(max_streams);
//        let codec = BytesCodec::new();
//        let server = server(cfg.clone(), l).and_then(move |c| repeat_echo(c, codec, 1));
//        let client = client(cfg, a).and_then(move |conn| {
//            let mut v = Vec::new();
//            for _ in 0 .. max_streams {
//                v.push(new_stream(&conn))
//            }
//            Ok(conn.open_stream().is_err())
//        });
//        run(server, client)
//    }
//
//    quickcheck(prop as fn(_) -> _);
//}

//#[test]
//fn prop_send_recv_half_closed() {
//    fn prop(msg: Msg) -> bool {
//        let (l, a) = bind();
//        let cfg = Config::default();
//        let msg_len = msg.0.len();
//
//        // Server should be able to write on a stream shutdown by the client.
//        let server = server(cfg.clone(), l).and_then(|c| {
//            c.into_future().map_err(|(e,_)| e)
//                .and_then(|(stream, _)| {
//                    let s = stream.expect("S: No incoming stream");
//                    let buf = vec![0; msg_len];
//                    tokio::io::read_exact(s, buf)
//                        .and_then(|(s, buf)| {
//                            assert!(s.state() == Some(State::RecvClosed));
//                            tokio::io::write_all(s, buf)
//                        })
//                        .and_then(|(s, _buf)| {
//                            tokio::io::flush(s).map(|_| true)
//                        }).from_err()
//                })
//                .map_err(|e| error!("S: connection error: {}", e))
//        });
//
//        // Client should be able to read after shutting down the stream.
//        let client = client(cfg, a).and_then(move |c| {
//            let s = new_stream(&c);
//            tokio::io::write_all(s, msg.clone())
//                .and_then(|(s, _buf)| {
//                    tokio::io::shutdown(s)
//                })
//                .and_then(move |s| {
//                    assert!(s.state() == Some(State::SendClosed));
//                    let buf = vec![0; msg_len];
//                    tokio::io::read_exact(s, buf)
//                })
//                .and_then(move |(s, buf)| {
//                    assert!(s.state() == Some(State::Closed));
//                    future::ok(buf == msg.0)
//                })
//                .map_err(|e| error!("C: connection error: {}", e))
//        });
//
//        client.join(server).wait() == Ok((true, true))
//    }
//
//    QuickCheck::new().tests(3).quickcheck(prop as fn(_) -> _);
//}

#[derive(Clone, Debug)]
struct Msg(Vec<u8>);

impl From<Msg> for Bytes {
    fn from(m: Msg) -> Bytes {
        m.0.into()
    }
}

impl AsRef<[u8]> for Msg {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Arbitrary for Msg {
    fn arbitrary<G: Gen>(g: &mut G) -> Msg {
        let n: usize = g.gen_range(1, g.size() + 1);
        let mut v = vec![0; n];
        g.fill(&mut v[..]);
        Msg(v)
    }
}

async fn bind() -> io::Result<(TcpListener, SocketAddr)> {
    let i = Ipv4Addr::new(127, 0, 0, 1);
    let s = SocketAddr::V4(SocketAddrV4::new(i, 0));
    let l = TcpListener::bind(&s).await?;
    let a = l.local_addr()?;
    Ok((l, a))
}

/// For each incoming stream of `c` echo back `n` frames to the sender.
async fn repeat_echo(c: Connection<TcpStream>, n: u64) -> Result<(), ConnectionError> {
    let c = yamux::into_stream(c);
    futures::pin_mut!(c);
    c.try_for_each_concurrent(None, |stream| async {
        let (os, is) = Framed::new(stream, BytesCodec {}).split();
        is.take(n).forward(os).await?;
        Ok(())
    })
    .await?;
    Ok(())
}

/// For each message in `iter`, open a new stream, send the message and
/// collect the response. The sequence of responses will be returned.
async fn send_recv<I>(mut control: RemoteControl, iter: I) -> Result<Vec<Bytes>, ConnectionError>
where
    I: Iterator<Item = Bytes>
{
    let mut result = Vec::new();
    for msg in iter {
        let stream = control.open_stream().await?;
        log::debug!("C: new stream: {}", stream);
        let id = stream.id();
        let len = msg.len();
        let mut codec = Framed::new(stream, BytesCodec {});
        codec.send(msg).await?;
        log::debug!("C: {}: sent {} bytes", id, len);
        let data: Vec<Bytes> = codec.try_collect().await?;
        log::debug!("C: received {} bytes", data.len());
        result.extend(data)
    }
    log::debug!("C: closing connection");
    control.close().await?;
    Ok(result)
}

