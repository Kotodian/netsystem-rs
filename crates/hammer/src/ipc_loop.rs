use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::TcpListener;

pub async fn clnt_loop(listener: TcpListener) {
    loop {
        if hammer_runtime::engine::Engine::with_current(|engine| {
            engine.main_loop_exit_now.load(Ordering::Relaxed)
        })
        .unwrap_or(true)
        {
            return;
        }
        let accepted = tokio::select! {
            accepted = listener.accept() => Some(accepted),
            _ = tokio::time::sleep(Duration::from_millis(10)) => None,
        };
        let Some(accepted) = accepted else {
            continue;
        };
        match accepted {
            Ok((stream, _addr)) => {
                conn_loop(stream).await;
            }
            Err(e) => {
                tracing::error!("IPC accept error: {e}");
            }
        }
    }
}

async fn conn_loop(stream: tokio::net::TcpStream) {
    let (mut reader, mut writer) = stream.into_split();
    let mut buf = vec![0u8; 65536];

    loop {
        match hammer_ipc::frame::async_read_frame(&mut reader, &mut buf).await {
            Ok(Some(data)) => {
                let request: hammer_ipc::handler::IpcRequest = match bincode::deserialize(&data) {
                    Ok(req) => req,
                    Err(e) => {
                        tracing::error!("IPC deserialize error: {e}");
                        break;
                    }
                };

                let handler_result = hammer_runtime::engine::Engine::with_current(|engine| {
                    hammer_ipc::handler::dispatch_handler(engine, &request.name, &request.payload)
                });

                let response_payload = match handler_result {
                    Some(Some(bytes)) => bytes,
                    _ => {
                        let _ = hammer_ipc::frame::async_write_frame(&mut writer, &[]).await;
                        continue;
                    }
                };

                let response = hammer_ipc::handler::IpcResponse {
                    payload: response_payload,
                };
                match bincode::serialize(&response) {
                    Ok(bytes) => {
                        if let Err(e) =
                            hammer_ipc::frame::async_write_frame(&mut writer, &bytes).await
                        {
                            tracing::error!("IPC write error: {e}");
                            break;
                        }
                        if hammer_runtime::engine::Engine::with_current(|engine| {
                            engine.main_loop_exit_now.load(Ordering::Relaxed)
                        })
                        .unwrap_or(true)
                        {
                            return;
                        }
                    }
                    Err(e) => {
                        tracing::error!("IPC serialize error: {e}");
                        break;
                    }
                }
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                tracing::error!("IPC read error: {e}");
                break;
            }
        }
    }
}
