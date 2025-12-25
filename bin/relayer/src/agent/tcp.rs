use std::{
    marker::PhantomData,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::StreamExt;
use metrics::histogram;
use protocol::{
    key::{ClusterRequest, ClusterValidator},
    proxy::AgentId,
    // session::YamuxSession,
};
use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc::{channel, Receiver, Sender},
};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux_low_mem::{session::KeepAliveConfig, stream::YamuxStream as Stream, YamuxSession, YamuxSessionConfig};
// use yamux::Stream;

use crate::{agent::AgentSessionControl, METRICS_AGENT_HISTOGRAM};

use super::{AgentListener, AgentListenerEvent, AgentSession, AgentSessionId};

pub type TunnelTcpStream = Compat<Stream>;

pub struct AgentTcpListener<VALIDATE, HANDSHAKE: ClusterRequest> {
    validate: Arc<VALIDATE>,
    listener: TcpListener,
    internal_tx: Sender<AgentListenerEvent<HANDSHAKE::Context, TunnelTcpStream>>,
    internal_rx: Receiver<AgentListenerEvent<HANDSHAKE::Context, TunnelTcpStream>>,
    _tmp: PhantomData<HANDSHAKE>,
}

impl<VALIDATE, HANDSHAKE: ClusterRequest> AgentTcpListener<VALIDATE, HANDSHAKE> {
    pub async fn new(addr: SocketAddr, validate: VALIDATE) -> anyhow::Result<Self> {
        log::info!("[AgentTcp] starting with addr {addr}");
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

impl<VALIDATE: ClusterValidator<REQ>, REQ: DeserializeOwned + Send + Sync + 'static + ClusterRequest> AgentListener<REQ::Context, TunnelTcpStream> for AgentTcpListener<VALIDATE, REQ> {
    async fn recv(&mut self) -> anyhow::Result<AgentListenerEvent<REQ::Context, TunnelTcpStream>> {
        loop {
            select! {
                incoming = self.listener.accept() => {
                    let (stream, remote) = incoming?;
                    let validate = self.validate.clone();
                    let internal_tx = self.internal_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = run_connection(validate, stream, remote, internal_tx).await {
                            log::error!("[AgentTcp] connection {remote} error {e:?}");
                        }
                    });
                },
                event = self.internal_rx.recv() => break Ok(event.expect("should receive event from internal channel")),
            }
        }
    }

    async fn shutdown(&mut self) {}
}

async fn run_connection<VALIDATE: ClusterValidator<REQ>, REQ: ClusterRequest>(
    validate: Arc<VALIDATE>,
    mut in_stream: TcpStream,
    remote: SocketAddr,
    internal_tx: Sender<AgentListenerEvent<REQ::Context, TunnelTcpStream>>,
) -> anyhow::Result<()> {
    let started = Instant::now();
    log::info!("[AgentTcp] new connection from {}", remote);

    let mut buf = [0u8; 4096];
    let buf_len = in_stream.read(&mut buf).await?;

    log::info!("[AgentTcp] new connection got handhsake data {buf_len} bytes");

    let req = validate.validate_connect_req(&buf[0..buf_len])?;
    let domain = validate.generate_domain(&req)?;
    let agent_id = AgentId::try_from_domain(&domain)?;
    let session_id = AgentSessionId::rand();
    let agent_ctx = req.context();

    log::info!("[AgentTcp] new connection validated with domain {domain} agent_id: {agent_id}, session uuid: {session_id}");

    let res_buf = validate.sign_response_res(&req, None);
    in_stream.write_all(&res_buf).await?;
    let (control_tx, mut control_rx) = channel(10);

    internal_tx
        .send(AgentListenerEvent::Connected(
            agent_id,
            AgentSession::new(agent_id, session_id, domain, control_tx),
            Some(remote.to_string()),
        ))
        .await
        .expect("should send to main loop");

    log::info!("[AgentTcp] new connection {agent_id} {session_id}  started loop");
    // let mut session = Session::new_client(in_stream, Default::default());

    let cfg = YamuxSessionConfig {
        max_write_buffer: 65536,
        keep_alive_config: Some(KeepAliveConfig {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(30),
        }),
    };
    let mut session = YamuxSession::client(in_stream.compat(), cfg);
    histogram!(METRICS_AGENT_HISTOGRAM).record(started.elapsed().as_millis() as f32 / 1000.0);

    loop {
        select! {
            control = control_rx.recv() => match control {
                Some(control) => match control {
                    AgentSessionControl::CreateStream(tx) => {
                        log::info!("[AgentTcp] agent {agent_id} {session_id} create stream request");
                        let stream = session.open_stream();
                        log::info!("[AgentTls] agent {agent_id} {session_id} created stream");
                        if let Err(_e) = tx.send(Ok(stream.compat())) {
                            log::error!("[AgentTls] agent {agent_id} {session_id} send created stream error");
                        }
                    },
                    AgentSessionControl::LatestPing(_) => {}
                    AgentSessionControl::ForeceStop(_) => {}
                },
                None => {
                    break;
                }
            },
            accept = session.next() => match accept {
                Some(stream) => {
                    let internal_tx = internal_tx.clone();
                    let agent_ctx = agent_ctx.clone();
                    tokio::spawn(async move {
                        internal_tx.send(AgentListenerEvent::IncomingStream(agent_id, agent_ctx, stream.compat())).await.expect("should send to main loop");
                    });
                },
                None => {
                    log::error!("[AgentTcp] agent {agent_id} {session_id} Tcp connection broken with None");
                    break;
                }
            }
        }
    }

    log::info!("[AgentTcp] agent {agent_id} {session_id}  stopped loop");

    internal_tx
        .send(AgentListenerEvent::Disconnected(agent_id, session_id, Some(remote.to_string())))
        .await
        .expect("should send to main loop");

    Ok(())
}
