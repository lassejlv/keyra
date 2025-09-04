# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o redis-server .

# Final stage
FROM alpine:latest

# Install ca-certificates for any HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/redis-server .

# Create data directory
RUN mkdir -p /data

# Set environment variables
ENV REDIS_DATA_DIR=/data
ENV REDIS_SAVE_INTERVAL=30s

# Expose Redis port
EXPOSE 6379

# Run the server
CMD ["./redis-server"]
