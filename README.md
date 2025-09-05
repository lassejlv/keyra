
<img width="512" height="512" alt="keyra" src="https://github.com/user-attachments/assets/89550895-635c-48e0-954e-a00ce0a5f27d" />

## Keyra

A lightweight, Redis-compatible server written in Go. 

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
