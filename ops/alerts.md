# Sample Prometheus-style alerts for write services

- alert: WriteChecksumDaemonDown
  expr: up{job="write-checksum"} == 0
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "Write checksum daemon is not running"
    description: "No scrape for job=write-checksum. Check systemd unit ops/checksum-daemon.service"

- alert: WriteOutboxQueueLag
  expr: outbox_queue_pending > 100
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "Outbox queue backlog is high"
    description: "Pending events exceed 100. Inspect AO bridge connectivity or retry settings."

- alert: WriteWalSizeHigh
  expr: write_wal_bytes > 5242880
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Write WAL size above 5 MiB"
    description: "Rotate or archive WAL. Check for stuck retries or noisy clients."
