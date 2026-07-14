# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /praetor-reconciler .

# Run Stage — pure-Go SSH (golang.org/x/crypto/ssh), so no openssh-client needed.
FROM alpine:3.19

# uid 1000 matches the executor so the shared SSH known_hosts volume (host-key
# trust-on-first-use) is readable/writable by both.
RUN adduser -D -u 1000 praetor \
 && mkdir -p /home/praetor/.ssh \
 && chown -R praetor:praetor /home/praetor/.ssh \
 && chmod 700 /home/praetor/.ssh

COPY --from=builder /praetor-reconciler /praetor-reconciler

ENV HOME=/home/praetor
USER 1000:1000

CMD ["/praetor-reconciler"]
