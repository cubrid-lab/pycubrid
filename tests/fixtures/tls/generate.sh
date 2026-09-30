#!/usr/bin/env bash
# Regenerate the offline TLS test PKI used by tests/test_tls_matrix_offline.py.
#
# These keys and certificates are TEST-ONLY material for the in-process fake
# TLS broker (tests/helpers/tls_broker.py). They protect nothing and must never
# be used outside the test suite. The "valid" certificates are issued for 100
# years so the suite does not rot; expired.pem has a validity window fixed in
# the past on purpose.
#
# Usage: tests/fixtures/tls/generate.sh   (requires OpenSSL 1.1.1+ on PATH)
set -euo pipefail
cd "$(dirname "$0")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

san_ok="subjectAltName=DNS:localhost,IP:127.0.0.1"
ec="-newkey ec -pkeyopt ec_paramgen_curve:prime256v1"

# Test root CA.
openssl req -x509 $ec -nodes -days 36525 -keyout "$work/ca.key" -out ca.pem \
  -subj "/CN=pycubrid test CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

# One leaf key shared by every server certificate.
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 -out server.key
openssl req -new -key server.key -subj "/CN=localhost" -out "$work/server.csr"
openssl req -new -key server.key -subj "/CN=wrong-host.invalid" -out "$work/wrong.csr"

issue() {  # issue <csr> <out.pem> <san> [extra openssl ca args...]
  local csr="$1" out="$2" san="$3"
  shift 3
  mkdir -p "$work/db"
  : > "$work/db/index.txt"
  printf '%s\n' "$(openssl rand -hex 16)" > "$work/db/serial"
  cat > "$work/ca.cnf" <<CNF
[ca]
default_ca = test_ca
[test_ca]
database = $work/db/index.txt
serial = $work/db/serial
new_certs_dir = $work/db
certificate = ca.pem
private_key = $work/ca.key
default_md = sha256
policy = any
unique_subject = no
copy_extensions = none
x509_extensions = leaf
[any]
commonName = supplied
[leaf]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth
$san
CNF
  openssl ca -batch -notext -config "$work/ca.cnf" -in "$csr" \
    -out "$out" "$@"
}

# Back-date "valid" certificates so runner clock skew cannot make them
# not-yet-valid.
valid="-startdate 20250101000000Z -enddate 21250101000000Z"
issue "$work/server.csr" server.pem "$san_ok" $valid
issue "$work/wrong.csr" wrong_host.pem "subjectAltName=DNS:wrong-host.invalid" $valid
issue "$work/server.csr" expired.pem "$san_ok" -startdate 20000101000000Z -enddate 20010101000000Z

# Self-signed certificate for the same names: valid dates, unknown issuer.
openssl req -x509 -key server.key -days 36500 -out self_signed.pem \
  -subj "/CN=localhost" -addext "$san_ok"

chmod 644 ./*.pem ./*.key
