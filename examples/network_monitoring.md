# Network I/O Monitoring

Your Redis server now includes comprehensive network I/O monitoring similar to TinyRDM and other Redis GUIs.

## Features Added

### 1. Real-time Network Tracking

- **Bytes Received**: Tracks all incoming data from clients
- **Bytes Sent**: Tracks all outgoing data to clients
- **Connection Count**: Total connections established
- **Commands Processed**: Total Redis commands executed
- **Rate Calculations**: Real-time rates for input/output and commands per second

### 2. Enhanced INFO Command

The `INFO` command now includes network statistics:

```bash
redis-cli INFO stats
```

Returns metrics like:

```
# Stats
total_connections_received:5
total_commands_processed:142
instantaneous_ops_per_sec:12.50
total_net_input_bytes:2048
total_net_output_bytes:4096
instantaneous_input_kbps:2.45
instantaneous_output_kbps:4.12
```

### 3. HTTP Metrics Endpoint

Access real-time metrics via HTTP (port 8080):

```bash
# Complete metrics
curl http://localhost:8080/metrics

# Network-only metrics
curl http://localhost:8080/metrics/network
```

Response example:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "network_input_bytes": 2048,
  "network_output_bytes": 4096,
  "input_rate_kbps": 2.45,
  "output_rate_kbps": 4.12,
  "commands_per_sec": 12.5,
  "total_connections": 5,
  "total_commands": 142,
  "active_clients": 2,
  "uptime_seconds": 240,
  "used_memory_bytes": 1048576,
  "total_keys": 15
}
```

## Integration with Monitoring Tools

### TinyRDM-style Dashboards

The metrics can be consumed by any monitoring dashboard:

- **Network Input/Output**: Real-time bandwidth usage
- **Commands/Sec**: Operations per second rate
- **Active Connections**: Current client count
- **Memory Usage**: Current memory consumption

### Prometheus/Grafana

The HTTP endpoint can be scraped by Prometheus for Grafana dashboards.

### Custom Monitoring

Use the JSON API to build custom monitoring solutions.

## Usage Example

```bash
# Start Redis server (includes metrics server on port 8080)
./redis-server

# In another terminal, generate some traffic
redis-cli SET key1 "value1"
redis-cli GET key1
redis-cli INCR counter

# Check network metrics
curl http://localhost:8080/metrics/network | jq
```

## Performance Impact

- **Minimal Overhead**: Atomic counters for thread-safe tracking
- **Efficient Tracking**: Only increments counters on I/O operations
- **Rate Limiting**: Stats updated once per second to prevent excessive calculations
