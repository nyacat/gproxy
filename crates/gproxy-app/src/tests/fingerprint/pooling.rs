use std::borrow::Cow;
use std::io::ErrorKind;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use gproxy_channel_api::{Alpn, ClientProfile, Http2Profile, TlsVersion};
use gproxy_core::UpstreamTransport;
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn connection_pool_separates_profiles_and_reuses_equal_values() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        let mut id = 0;
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            id += 1;
            connections.spawn(serve(socket, id));
        }
    });
    let first = ClientProfile {
        alpn: Some(Cow::Borrowed(&[Alpn::Http1])),
        max_tls_version: Some(TlsVersion::Tls13),
        ..Default::default()
    };
    let same_values = ClientProfile {
        alpn: Some(Cow::Owned(vec![Alpn::Http1])),
        ..first.clone()
    };
    let different_tls = ClientProfile {
        max_tls_version: Some(TlsVersion::Tls12),
        ..first.clone()
    };
    let different_http2 = ClientProfile {
        http2: Some(Http2Profile {
            initial_window_size: Some(65_535),
            ..Default::default()
        }),
        ..first.clone()
    };
    let transport = gproxy_upstream::WreqTransport::new();
    let results = tokio::time::timeout(Duration::from_secs(5), async {
        let mut ids = Vec::new();
        for profile in [
            Some(&first),
            Some(&same_values),
            Some(&different_tls),
            Some(&different_http2),
            Some(&first),
            None,
            None,
        ] {
            let mut request = http::Request::get(&uri).body(Bytes::new()).unwrap();
            if let Some(profile) = profile {
                request.extensions_mut().insert(profile.clone());
            }
            let mut body = transport.send(request).await.unwrap().into_body();
            let mut bytes = Vec::new();
            while let Some(chunk) = body.next().await {
                bytes.extend_from_slice(&chunk.unwrap());
            }
            ids.push(String::from_utf8(bytes).unwrap());
        }
        ids
    })
    .await;
    server.abort();
    let ids = results.expect("local requests should finish");
    assert_eq!(ids, ["1", "1", "2", "3", "1", "4", "4"]);
}

async fn serve(socket: TcpStream, id: usize) {
    let body = id.to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    loop {
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            socket.readable().await.unwrap();
            let mut buffer = [0_u8; 4096];
            match socket.try_read(&mut buffer) {
                Ok(0) => return,
                Ok(count) => request.extend_from_slice(&buffer[..count]),
                Err(error) if error.kind() == ErrorKind::WouldBlock => continue,
                Err(_) => return,
            }
        }
        let mut pending = response.as_bytes();
        while !pending.is_empty() {
            socket.writable().await.unwrap();
            match socket.try_write(pending) {
                Ok(0) => return,
                Ok(count) => pending = &pending[count..],
                Err(error) if error.kind() == ErrorKind::WouldBlock => continue,
                Err(_) => return,
            }
        }
    }
}
