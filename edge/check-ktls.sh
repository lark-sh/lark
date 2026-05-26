#!/bin/bash
# Run this on the proxy server while traffic is flowing to verify kTLS is active

echo "=== kTLS Statistics ==="
cat /proc/net/tls_stat 2>/dev/null || echo "kTLS stats not available"

echo ""
echo "=== TLS Module ==="
lsmod | grep tls

echo ""
echo "=== Open File Descriptors (proxy process) ==="
PROXY_PID=$(pgrep -f lark-edge-linux)
if [ -n "$PROXY_PID" ]; then
    echo "Proxy PID: $PROXY_PID"
    ls /proc/$PROXY_PID/fd 2>/dev/null | wc -l
    echo "file descriptors open"
else
    echo "Proxy not running"
fi

echo ""
echo "=== Network Connections ==="
ss -s

echo ""
echo "=== CPU Usage (top 5 processes) ==="
ps aux --sort=-%cpu | head -6
