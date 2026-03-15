
## Checksum daemon (write)
Example systemd unit:
```
[Unit]
Description=Write checksum monitor
After=network.target

[Service]
WorkingDirectory=/opt/blackcat-darkmesh-write
Environment=CHECKSUM_INTERVAL_SEC=300
Environment=WRITE_WAL_PATH=/var/log/ao/write-wal.ndjson
Environment=AO_QUEUE_PATH=/var/lib/ao/outbox-queue.ndjson
ExecStart=/opt/blackcat-darkmesh-write/scripts/verify/checksum_daemon.sh
Restart=always

[Install]
WantedBy=multi-user.target
```
