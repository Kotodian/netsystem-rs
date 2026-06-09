use std::net::SocketAddr;

use hammer_core::error::HammerResult;

use crate::tcp::TcpStream;
use crate::udp::UdpSocket;

pub async fn echo_once(stream: &TcpStream) -> HammerResult<()> {
    let recv = stream.recv_buffer().await?;
    stream.send_buffer(recv).await
}

pub async fn run_tcp_echo(stream: &TcpStream, iterations: usize) -> HammerResult<()> {
    for _ in 0..iterations {
        echo_once(stream).await?;
    }
    Ok(())
}

#[inline]
pub async fn run_echo_loop(stream: &TcpStream, iterations: usize) -> HammerResult<()> {
    run_tcp_echo(stream, iterations).await
}

pub async fn run_udp_echo(socket: &UdpSocket) -> HammerResult<SocketAddr> {
    let (recv, source) = socket.recv_from_buffer().await?;
    socket.send_buffer_to(recv, source).await?;
    Ok(source)
}

#[inline]
pub async fn run_udp_echo_once(socket: &UdpSocket) -> HammerResult<SocketAddr> {
    run_udp_echo(socket).await
}
