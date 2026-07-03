#!/usr/bin/env bash
# Regenerates the committed TLS test fixtures. Test-only material — the CA key
# is discarded afterwards, so a rerun replaces the whole chain at once.
set -euo pipefail
cd "$(dirname "$0")"

days=36500

openssl req -x509 -sha256 -newkey rsa:2048 -nodes \
  -keyout ca.key -out ca.crt -days "$days" \
  -subj "/CN=effect-grpc test CA"

openssl req -newkey rsa:2048 -nodes \
  -keyout server.key -out server.csr -subj "/CN=localhost"
openssl x509 -req -sha256 -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days "$days" \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

openssl req -newkey rsa:2048 -nodes \
  -keyout client.key -out client.csr -subj "/CN=effect-grpc test client"
openssl x509 -req -sha256 -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days "$days" \
  -extfile <(printf "extendedKeyUsage=clientAuth")

rm -f ca.key ca.srl server.csr client.csr
