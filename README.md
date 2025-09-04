Keyra

A lightweight, Redis-compatible TCP server written in Go. Supports standard Redis clients including ioredis.js and redis-cli.

## Features

- **Core Commands**: SET, GET, DEL, TYPE, TTL, PING, EXPIRE, EXPIREAT, AUTH
- **Advanced SET Options**: EX (seconds) and PX (milliseconds) expiration
- **Persistence**: Binary storage with automatic save/load on startup and shutdown
- **TTL Support**: Full time-to-live functionality with automatic key expiration
- **Manual Saves**: SAVE (synchronous) and BGSAVE (background) commands
- **Authentication**: Password protection with AUTH command support
- **GUI Commands**: KEYS, SCAN, EXISTS, DBSIZE, INFO, CLIENT commands
- **Thread-Safe**: Concurrent client connections with proper mutex protection

## Configuration

Configure via environment variables:

- `REDIS_STORAGE_PATH`: Custom file path for persistence
- `REDIS_DATA_DIR`: Custom directory (uses redis_data.rdb)
- `REDIS_SAVE_INTERVAL`: Periodic save frequency (e.g., 30s, 5m)
- `REDIS_PASSWORD`: Enable password authentication

## Compatibility

Compatible with standard Redis clients and GUI tools like Redis Desktop Manager.
