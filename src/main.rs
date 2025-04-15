use clap::Parser;
use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

#[derive(Parser)]
#[command(name = "swaybar")]
#[command(author = "thomas@gebert.app")]
#[command(version = "1.0")]
#[command(about = "nada")]

pub struct Args {
    #[arg(short, long)]
    pub port: Option<u32>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let port = args.port.unwrap_or(1337);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    println!("Listening on 127.0.0.1:{}", port);

    let (mut socket, _) = listener.accept().await?;
    println!("Client connected");

    let mut len_buf = [0u8; 8];
    socket.read_exact(&mut len_buf).await?;
    let mut cursor = Cursor::new(len_buf);
    let msg_len = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor)?;

    let mut buf = vec![0u8; msg_len as usize];
    socket.read_exact(&mut buf).await?;

    println!("Got message: {:?}", String::from_utf8_lossy(&buf));
    Ok(())
}
