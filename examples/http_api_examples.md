# Keyra HTTP API - Upstash Compatible

This document shows how to use Keyra's HTTP API, which is 100% compatible with Upstash Redis REST API.

## Base URL

When running locally: `http://localhost:8081`

## Authentication

Keyra supports multiple authentication methods:

### Bearer Token (Recommended)

```bash
curl -H "Authorization: Bearer your_password" \
     -H "Content-Type: application/json" \
     -d '["SET", "mykey", "myvalue"]' \
     http://localhost:8081/
```

### Basic Authentication

```bash
curl -u "username:your_password" \
     -H "Content-Type: application/json" \
     -d '["SET", "mykey", "myvalue"]' \
     http://localhost:8081/
```

## API Endpoints

### Single Command Execution

**Endpoint:** `POST /`

Execute a single Redis command by sending a JSON array with the command and arguments.

#### Examples

##### SET Command

```bash
curl -X POST http://localhost:8081/ \
  -H "Authorization: Bearer your_password" \
  -H "Content-Type: application/json" \
  -d '["SET", "user:1", "john_doe"]'
```

Response:

```json
{ "result": "OK" }
```

##### GET Command

```bash
curl -X POST http://localhost:8081/ \
  -H "Authorization: Bearer your_password" \
  -H "Content-Type: application/json" \
  -d '["GET", "user:1"]'
```

Response:

```json
{ "result": "john_doe" }
```

##### HSET Command (Hash)

```bash
curl -X POST http://localhost:8081/ \
  -H "Authorization: Bearer your_password" \
  -H "Content-Type: application/json" \
  -d '["HSET", "user:profile:1", "name", "John", "email", "john@example.com"]'
```

Response:

```json
{ "result": 2 }
```

##### HGETALL Command

```bash
curl -X POST http://localhost:8081/ \
  -H "Authorization: Bearer your_password" \
  -H "Content-Type: application/json" \
  -d '["HGETALL", "user:profile:1"]'
```

Response:

```json
{ "result": ["name", "John", "email", "john@example.com"] }
```

##### LPUSH Command (List)

```bash
curl -X POST http://localhost:8081/ \
  -H "Authorization: Bearer your_password" \
  -H "Content-Type: application/json" \
  -d '["LPUSH", "tasks", "task1", "task2", "task3"]'
```

Response:

```json
{ "result": 3 }
```

##### LRANGE Command

```bash
curl -X POST http://localhost:8081/ \
  -H "Authorization: Bearer your_password" \
  -H "Content-Type: application/json" \
  -d '["LRANGE", "tasks", "0", "-1"]'
```

Response:

```json
{ "result": ["task3", "task2", "task1"] }
```

##### SADD Command (Set)

```bash
curl -X POST http://localhost:8081/ \
  -H "Authorization: Bearer your_password" \
  -H "Content-Type: application/json" \
  -d '["SADD", "tags", "redis", "database", "cache"]'
```

Response:

```json
{ "result": 3 }
```

##### SMEMBERS Command

```bash
curl -X POST http://localhost:8081/ \
  -H "Authorization: Bearer your_password" \
  -H "Content-Type: application/json" \
  -d '["SMEMBERS", "tags"]'
```

Response:

```json
{ "result": ["redis", "database", "cache"] }
```

### Pipeline (Multiple Commands)

**Endpoint:** `POST /pipeline`

Execute multiple Redis commands in a single HTTP request.

```bash
curl -X POST http://localhost:8081/pipeline \
  -H "Authorization: Bearer your_password" \
  -H "Content-Type: application/json" \
  -d '[
    ["SET", "key1", "value1"],
    ["SET", "key2", "value2"],
    ["GET", "key1"],
    ["GET", "key2"]
  ]'
```

Response:

```json
[{ "result": "OK" }, { "result": "OK" }, { "result": "value1" }, { "result": "value2" }]
```

### Health Check

**Endpoint:** `GET /health`

Check the server status and get basic information.

```bash
curl http://localhost:8081/health
```

Response:

```json
{
  "status": "ok",
  "uptime": 3600.5,
  "connections": 5
}
```

## Error Handling

When an error occurs, the response will contain an error field:

```json
{ "error": "WRONGTYPE Operation against a key holding the wrong kind of value" }
```

For pipeline requests, individual commands can fail:

```json
[{ "result": "OK" }, { "error": "invalid command" }, { "result": "value" }]
```

## JavaScript/TypeScript Example

Using fetch API:

```javascript
async function redisCommand(command) {
  const response = await fetch('http://localhost:8081/', {
    method: 'POST',
    headers: {
      Authorization: 'Bearer your_password',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(command),
  })

  const data = await response.json()
  return data.result
}

// Usage examples
await redisCommand(['SET', 'mykey', 'myvalue'])
const value = await redisCommand(['GET', 'mykey'])
console.log(value) // "myvalue"
```

## Python Example

Using requests library:

```python
import requests
import json

def redis_command(command, base_url='http://localhost:8081', password='your_password'):
    headers = {
        'Authorization': f'Bearer {password}',
        'Content-Type': 'application/json'
    }

    response = requests.post(base_url,
                           headers=headers,
                           data=json.dumps(command))

    data = response.json()
    if 'error' in data:
        raise Exception(f"Redis error: {data['error']}")

    return data.get('result')

# Usage examples
redis_command(['SET', 'mykey', 'myvalue'])
value = redis_command(['GET', 'mykey'])
print(value)  # "myvalue"
```

## Compatibility with Upstash

This HTTP API is 100% compatible with Upstash Redis REST API, meaning:

1. **Same request format**: JSON arrays for commands
2. **Same response format**: `{"result": ...}` or `{"error": "..."}`
3. **Same authentication**: Bearer token or Basic auth
4. **Same endpoints**: `/` for single commands, `/pipeline` for multiple
5. **Same error handling**: Error responses in JSON format

You can use existing Upstash Redis clients and SDKs with Keyra without any modifications.

## CORS Support

The API includes CORS headers, making it suitable for browser-based applications:

- `Access-Control-Allow-Origin: *`
- `Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type, Authorization`
