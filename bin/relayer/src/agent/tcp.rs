use std::{marker::PhantomData, net::SocketAddr, sync::Arc, time::Instant};

use futures::StreamExt;
use metrics::histogram;
use protocol::{key::ClusterValidator, proxy::AgentId};
use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc::{channel, Receiver, Sender},
};

use crate::{agent::AgentSessionControl, METRICS_AGENT_HISTOGRAM};
use tokio_yamux::{Session, StreamHandle};

use super::{AgentListener, AgentListenerEvent, AgentSession, AgentSessionId};

pub type TunnelTcpStream = StreamHandle;

pub struct AgentTcpListener<VALIDATE, HANDSHAKE> {
    validate: Arc<VALIDATE>,
    listener: TcpListener,
    internal_tx: Sender<AgentListenerEvent<TunnelTcpStream>>,
    internal_rx: Receiver<AgentListenerEvent<TunnelTcpStream>>,
    _tmp: PhantomData<HANDSHAKE>,
}

impl<VALIDATE, HANDSHAKE> AgentTcpListener<VALIDATE, HANDSHAKE> {
    pub async fn new(addr: SocketAddr, validate: VALIDATE) -> anyhow::Result<Self> {
        let (internal_tx, internal_rx) = channel(10);

        Ok(Self {
            listener: TcpListener::bind(addr).await?,
            internal_tx,
            internal_rx,
            validate: validate.into(),
            _tmp: PhantomData,
        })
    }
}

impl<VALIDATE: ClusterValidator<REQ>, REQ: DeserializeOwned + Send + Sync + 'static> AgentListener<TunnelTcpStream> for AgentTcpListener<VALIDATE, REQ> {
    async fn recv(&mut self) -> anyhow::Result<AgentListenerEvent<TunnelTcpStream>> {
        loop {
            select! {
                incoming = self.listener.accept() => {
                    let (stream, remote) = incoming?;
                    tokio::spawn(run_connection(self.validate.clone(), stream, remote, self.internal_tx.clone()));
                },
                event = self.internal_rx.recv() => break Ok(event.expect("should work")),
            }
        }
    }

    async fn shutdown(&mut self) {}
}

async fn run_connection<VALIDATE: ClusterValidator<REQ>, REQ>(
    validate: Arc<VALIDATE>,
    mut in_stream: TcpStream,
    remote: SocketAddr,
    internal_tx: Sender<AgentListenerEvent<TunnelTcpStream>>,
) -> anyhow::Result<()> {
    let started = Instant::now();
    log::info!("[AgentTcp] new connection from {}", remote);

    let mut buf = [0u8; 4096];
    let buf_len = in_stream.read(&mut buf).await?;

    log::info!("[AgentTcp] new connection got handhsake data {buf_len} bytes");

    let req = validate.validate_connect_req(&buf[0..buf_len])?;
    let domain = validate.generate_domain(&req)?;
    let agent_id = AgentId::from_domain(&domain);
    let session_id = AgentSessionId::rand();

    log::info!("[AgentTcp] new connection validated with domain {domain} agent_id: {agent_id}, session uuid: {session_id}");

    let res_buf = validate.sign_response_res(&req, None);
    in_stream.write_all(&res_buf).await?;
    let (control_tx, mut control_rx) = channel(10);

    internal_tx
        .send(AgentListenerEvent::Connected(agent_id, AgentSession::new(agent_id, session_id, domain, control_tx)))
        .await
        .expect("should send to main loop");

    log::info!("[AgentTcp] new connection {agent_id} {session_id}  started loop");
    let mut session = Session::new_client(in_stream, Default::default());
    histogram!(METRICS_AGENT_HISTOGRAM).record(started.elapsed().as_millis() as f32 / 1000.0);

    loop {
        select! {
            control = control_rx.recv() => match control {
                Some(control) => match control {
                    AgentSessionControl::CreateStream(tx) => {
                        log::info!("[AgentTcp] agent {agent_id} {session_id} create stream request");
                        match session.open_stream() {
                            Ok(stream) => {
                                log::info!("[AgentTcp] agent {agent_id} {session_id} created stream");
                                if let Err(_e) = tx.send(Ok(stream)) {
                                    log::error!("[AgentTcp] agent {agent_id} {session_id}  send created stream error");
                                }
                            },
                            Err(err) => {
                                if let Err(_e) = tx.send(Err(err.into())) {
                                    log::error!("[AgentTcp] agent {agent_id} {session_id}  send create stream's error, may be internal channel failed");
                                }
                            },
                        }
                    },
                },
                None => {
                    break;
                }
            },
            accept = session.next() => match accept {
                Some(Ok(stream)) => {
                    let internal_tx = internal_tx.clone();
                    tokio::spawn(async move {
                        internal_tx.send(AgentListenerEvent::IncomingStream(agent_id, stream)).await.expect("should send to main loop");
                    });
                },
                Some(Err(err)) => {
                    log::error!("[AgentTcp] agent {agent_id} {session_id} Tcp connection error {err:?}");
                    break;
                },
                None => {
                    log::error!("[AgentTcp] agent {agent_id} {session_id} Tcp connection broken with None");
                    break;
                }
            }
        }
    }

    log::info!("[AgentTcp] agent {agent_id} {session_id}  stopped loop");

    internal_tx.send(AgentListenerEvent::Disconnected(agent_id, session_id)).await.expect("should send to main loop");

    Ok(())
}