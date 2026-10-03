//! AF_UNIX CLI connections and their thread-zero Process Nodes.

use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::OnceLock;

use hammer_core::data_plane::{Frame, NodeId, NodeState};
use hammer_infra::pool::Pool;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{UnixListener, UnixStream};

use crate::cli::{CliError, CliMain};
use crate::error::{RuntimeError, RuntimeResult};
use crate::file::AsyncFileMain;
use crate::file::record::{File, FileFunctions};
use crate::{DataPlaneMain, NodeRuntime};

pub struct UnixCliMain {
    listener: RefCell<Option<u32>>,
    socket_path: RefCell<Option<PathBuf>>,
    files: RefCell<Pool<UnixCliFile>>,
    unused_process_nodes: RefCell<Vec<NodeId>>,
    next_process_number: Cell<u32>,
}

pub struct UnixCliFile {
    file_index: u32,
    process_node: NodeId,
}

// SAFETY: only thread zero calls methods that access the private RefCells.
// Process Futures run on its LocalSet and release every borrow before await.
unsafe impl Sync for UnixCliMain {}

static UNIX_CLI_MAIN: OnceLock<UnixCliMain> = OnceLock::new();

fn unix_cli_node(_: &mut DataPlaneMain, _: &mut NodeRuntime, _: &mut Frame) -> usize {
    0
}

impl UnixCliMain {
    pub fn init() {
        crate::ensure_main_thread().expect("Unix CLI initializes on thread zero");
        assert!(
            UNIX_CLI_MAIN
                .set(Self {
                    listener: RefCell::new(None),
                    socket_path: RefCell::new(None),
                    files: RefCell::new(Pool::new()),
                    unused_process_nodes: RefCell::new(Vec::new()),
                    next_process_number: Cell::new(0),
                })
                .is_ok(),
            "Unix CLI initializes once"
        );
    }

    #[inline]
    pub fn global() -> &'static Self {
        UNIX_CLI_MAIN
            .get()
            .expect("Unix CLI initializes before accepting connections")
    }

    pub async fn listen(&self, path: &Path) -> Result<(), CliError> {
        crate::ensure_main_thread().expect("Unix CLI listener belongs to thread zero");
        assert!(
            self.listener.borrow().is_none(),
            "Unix CLI listener initializes once"
        );
        let directory = path
            .parent()
            .expect("CLI socket path has a parent directory");
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(|source| CliError::SocketDirectory {
                path: directory.to_owned(),
                source,
            })?;
        match tokio::fs::symlink_metadata(path).await {
            Ok(metadata) if metadata.file_type().is_socket() => {
                match UnixStream::connect(path).await {
                    Ok(_) => {
                        return Err(CliError::SocketBind {
                            path: path.to_owned(),
                            source: io::Error::from(io::ErrorKind::AddrInUse),
                        });
                    }
                    Err(source) if source.kind() == io::ErrorKind::ConnectionRefused => {
                        tokio::fs::remove_file(path)
                            .await
                            .map_err(|source| CliError::SocketRemove {
                                path: path.to_owned(),
                                source,
                            })?;
                    }
                    Err(source) => {
                        return Err(CliError::SocketConfigure {
                            path: path.to_owned(),
                            source,
                        });
                    }
                }
            }
            Ok(_) => {
                return Err(CliError::SocketBind {
                    path: path.to_owned(),
                    source: io::Error::from(io::ErrorKind::AlreadyExists),
                });
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(CliError::SocketConfigure {
                    path: path.to_owned(),
                    source,
                });
            }
        }
        let listener = UnixListener::bind(path).map_err(|source| CliError::SocketBind {
            path: path.to_owned(),
            source,
        })?;
        let listener = match listener.into_std() {
            Ok(listener) => listener,
            Err(source) => {
                if let Err(cleanup) = tokio::fs::remove_file(path).await {
                    tracing::error!(%cleanup, "CLI socket cleanup failed after configure error");
                }
                return Err(CliError::SocketConfigure {
                    path: path.to_owned(),
                    source,
                });
            }
        };
        let files = AsyncFileMain::global();
        let index = match files.add(File::new(
            OwnedFd::from(listener),
            format!("CLI listener {}", path.display()),
            0,
            FileFunctions::default(),
        )) {
            Ok(index) => index,
            Err(source) => {
                if let Err(cleanup) = tokio::fs::remove_file(path).await {
                    tracing::error!(%cleanup, "CLI socket cleanup failed after File error");
                }
                return Err(CliError::FileRegister {
                    source: Box::new(source),
                });
            }
        };
        *self.listener.borrow_mut() = Some(index);
        *self.socket_path.borrow_mut() = Some(path.to_owned());
        Ok(())
    }

    pub async fn accept(&self, main: &Rc<RefCell<DataPlaneMain>>) -> RuntimeResult<()> {
        crate::ensure_main_thread()?;
        let files = AsyncFileMain::global();
        let listener_index = self
            .listener
            .borrow()
            .as_ref()
            .copied()
            .expect("CLI listener starts before accepting");
        let listener = files
            .file(listener_index)
            .expect("registered CLI listener remains live");
        loop {
            let descriptor = listener.accept().await?;
            self.add_file(main, files, descriptor).await?;
        }
    }

    async fn add_file(
        &self,
        main: &Rc<RefCell<DataPlaneMain>>,
        files: &'static AsyncFileMain,
        descriptor: OwnedFd,
    ) -> RuntimeResult<()> {
        self.reap_finished(main).await?;
        let reused = self.unused_process_nodes.borrow_mut().pop();
        let node = if let Some(node) = reused {
            node
        } else {
            let name = {
                let number = self.next_process_number.get();
                self.next_process_number
                    .set(number.checked_add(1).expect("CLI Process names fit u32"));
                Box::leak(format!("unix-cli-process-{number}").into_boxed_str())
            };
            crate::worker_thread_barrier_sync!({
                main.borrow()
                    .nodes
                    .try_register_process_node(name, unix_cli_node)
            })?
        };

        let file_index =
            match files.add(File::new(
                descriptor,
                format!("CLI connection {node:?}"),
                0,
                FileFunctions::default(),
            )) {
                Ok(index) => index,
                Err(source) => {
                    self.unused_process_nodes.borrow_mut().push(node);
                    return Err(source);
                }
            };
        let cli_file_index = self.files.borrow_mut().insert(UnixCliFile {
            file_index,
            process_node: node,
        });
        let future = unix_cli_process(Rc::downgrade(main), files, node, file_index);
        if let Err(source) = main.borrow_mut().start_process(node, future) {
            assert!(
                self.files.borrow_mut().remove(cli_file_index).is_some(),
                "new CLI connection remains registered"
            );
            files
                .remove(file_index)
                .expect("new CLI File has no in-flight operation");
            self.unused_process_nodes.borrow_mut().push(node);
            return Err(CliError::ProcessStart {
                source: Box::new(source),
            }
            .into());
        }
        Ok(())
    }

    async fn reap_finished(&self, main: &Rc<RefCell<DataPlaneMain>>) -> RuntimeResult<()> {
        let active = self
            .files
            .borrow()
            .iter()
            .map(|(index, file)| (index, file.file_index, file.process_node))
            .collect::<Vec<_>>();
        for (cli_file_index, file_index, node) in active {
            let finished = main.borrow_mut().nodes.take_finished_process(node);
            let Some(finished) = finished else { continue };
            let result = finished.task.await;
            let files = AsyncFileMain::global();
            let still_registered = files.file(file_index).is_some();
            if still_registered {
                files.remove(file_index)?;
            }
            main.borrow()
                .nodes
                .set_node_state(node, NodeState::Disabled)?;
            assert!(
                self.files.borrow_mut().remove(cli_file_index).is_some(),
                "completed CLI connection remains registered"
            );
            self.unused_process_nodes.borrow_mut().push(node);
            match result {
                Ok(Ok(())) => {}
                Ok(Err(source)) => {
                    tracing::warn!(node = ?node, %source, "CLI connection Process failed")
                }
                Err(source) => {
                    tracing::warn!(node = ?node, %source, "CLI connection Process exited")
                }
            }
        }
        Ok(())
    }

    pub async fn close_listener(&self, files: &AsyncFileMain) -> RuntimeResult<()> {
        crate::ensure_main_thread()?;
        let listener = *self.listener.borrow();
        if let Some(index) = listener {
            files.remove(index)?;
            *self.listener.borrow_mut() = None;
        }
        let path = self.socket_path.borrow().clone();
        if let Some(path) = path {
            tokio::fs::remove_file(&path)
                .await
                .map_err(|source| CliError::SocketRemove { path, source })?;
            *self.socket_path.borrow_mut() = None;
        }
        Ok(())
    }
}

#[hammer_component_macros::init_function(name = "unix_cli_init")]
fn init_unix_cli(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    UnixCliMain::init();
    Ok(())
}

#[hammer_component_macros::main_loop_exit_function(name = "unix_cli_exit")]
fn exit_unix_cli(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    if let Some(cli) = UNIX_CLI_MAIN.get() {
        let runtime = main
            .nodes
            .process_runtime
            .as_ref()
            .expect("thread-zero Process runtime remains available through exit");
        runtime.block_on(cli.close_listener(AsyncFileMain::global()))?;
    }
    Ok(())
}

async fn unix_cli_process(
    main: Weak<RefCell<DataPlaneMain>>,
    files: &'static AsyncFileMain,
    node: NodeId,
    file_index: u32,
) -> RuntimeResult<()> {
    let result = unix_cli_command(main, files, node, file_index).await;
    let cleanup = files.remove(file_index);
    match (result, cleanup) {
        (Err(primary), Err(cleanup)) => {
            tracing::error!(%cleanup, "CLI File cleanup failed after Process error");
            Err(primary)
        }
        (Err(primary), _) => Err(primary),
        (_, Err(cleanup)) => Err(cleanup),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn unix_cli_command(
    main: Weak<RefCell<DataPlaneMain>>,
    files: &AsyncFileMain,
    node: NodeId,
    file_index: u32,
) -> RuntimeResult<()> {
    let file = files
        .file(file_index)
        .expect("CLI Process File remains registered");
    let mut input = BufReader::new(file.as_ref());
    let mut line = Vec::new();
    let length = input
        .read_until(b'\n', &mut line)
        .await
        .map_err(|source| RuntimeError::FileRead { source })?;
    drop(input);
    if length == 0 {
        return Ok(());
    }
    let command = match std::str::from_utf8(&line) {
        Ok(text) => {
            let text = text.trim_end_matches(['\r', '\n']);
            let main = main.upgrade().ok_or(RuntimeError::ServiceClosed)?;
            let result = CliMain::global().dispatch(&mut main.borrow_mut(), text);
            result
        }
        Err(source) => Err(CliError::InputEncoding { source }),
    };
    let text = match command {
        Ok(task) => match task.await {
            Ok(Ok(text)) => text,
            Ok(Err(source)) => format!("{source}\n"),
            Err(source) => return Err(RuntimeError::ProcessTaskJoin { node, source }),
        },
        Err(source) => format!("{source}\n"),
    };
    let mut output = BufWriter::new(file.as_ref());
    output
        .write_all(text.as_bytes())
        .await
        .map_err(|source| RuntimeError::FileWrite { source })?;
    output
        .write_all(&[0])
        .await
        .map_err(|source| RuntimeError::FileWrite { source })?;
    output
        .flush()
        .await
        .map_err(|source| RuntimeError::FileWrite { source })?;
    Ok(())
}
