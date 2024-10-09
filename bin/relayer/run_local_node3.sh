RUST_LOG=info cargo run -- \
    --proxy-http-listener 127.0.0.1:11003 \
    --proxy-tls-listener 127.0.0.1:12003 \
    --proxy-rtsp-listener 127.0.0.1:5343 \
    --proxy-rtsps-listener 127.0.0.1:35343 \
    --agent-listener 127.0.0.1:13003 \
    --root-domain local.ha.8xff.io \
    --sdn-peer-id 3 \
    --sdn-listener 127.0.0.1:14003 \
    --sdn-seeds 1@127.0.0.1:14002 \