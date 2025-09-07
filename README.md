<div align="center">
<img width="256" height="256" alt="keyra" src="https://github.com/user-attachments/assets/89550895-635c-48e0-954e-a00ce0a5f27d" />

# Keyra

A lightweight, Redis-compatible server written in Go.

</div>

> [!WARNING]
> This is a work in progress, it is not ready for production use yet.

## Running

You can simply pull our docker image

```bash
docker pull ghcr.io/lassejlv/keyra:latest
```

or running it

```bash
docker run -d -e REDIS_PASSWORD=bob123 ghcr.io/lassejlv/keyra:latest
```

## Features

- **Core Commands**: SET, GET, DEL, TYPE, TTL, PING, EXPIRE, EXPIREAT, AUTH
- **Persistence**: Binary storage with automatic save/load on startup and shutdown
- **TTL Support**: Full time-to-live functionality with automatic key expiration
- **Manual Saves**: SAVE (synchronous) and BGSAVE (background) commands
- **Authentication**: Password protection with AUTH command support
- **Thread-Safe**: Concurrent client connections with proper mutex protection
