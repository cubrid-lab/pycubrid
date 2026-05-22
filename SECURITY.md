# Security Policy

## Supported Versions

The following versions of pycubrid are currently supported for security updates:

| Version | Status |
|---------|--------|
| 1.3.x   | ✅ Supported |
| < 1.3   | ❌ Not Supported |

Security patches will be applied to supported versions only. Users are strongly encouraged to upgrade to the latest version.

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue in pycubrid, please report it responsibly by emailing:

**Email:** paikend@gmail.com

**Do not** open a public GitHub issue for security vulnerabilities. Responsible disclosure allows us to address the issue before public disclosure.

### Response Timeline

- **48 hours:** Initial acknowledgment of your report
- **7 days:** Security assessment and initial response with remediation plan
- **Ongoing:** Regular updates on progress until resolution

## What Qualifies as a Security Issue

A security issue is any vulnerability that could:

- Allow unauthorized access to data
- Enable authentication bypass or privilege escalation
- Permit SQL injection or other code execution attacks
- Compromise confidentiality, integrity, or availability of the system
- Allow denial of service (DoS) attacks
- Expose sensitive information (credentials, tokens, private data)
- Bypass security controls or safety mechanisms
- Affect the security posture of applications using pycubrid

Examples include:
- SQL injection vulnerabilities in query construction
- Authentication/authorization flaws in the CAS protocol implementation
- Insecure credential handling during connection setup
- Cryptographic weaknesses in wire protocol communication
- Buffer overflow or memory corruption in packet parsing
- Input validation bypass

## What to Include in Your Report

Please provide the following information with your vulnerability report:

1. **Description:** Clear explanation of the vulnerability and its impact
2. **Affected Versions:** Which version(s) of pycubrid are vulnerable
3. **Steps to Reproduce:** Detailed instructions to reproduce the issue
4. **Proof of Concept:** Code sample, script, or test case demonstrating the vulnerability
5. **Impact Assessment:** Severity assessment (Critical, High, Medium, Low) and potential consequences
6. **Suggested Fix:** If you have a proposed patch or remediation strategy (optional but helpful)
7. **Your Contact Information:** Name, email, and PGP key (if applicable)

## Security Best Practices for Users

When using pycubrid, follow these security best practices:

- Always use parameterized queries (`?` placeholders) to prevent SQL injection
- Keep pycubrid updated to the latest version
- Use secure connection parameters when connecting to CUBRID databases
- Follow the principle of least privilege for database credentials
- Regularly audit and monitor database access logs
- Never hardcode credentials in your application code
- Use environment variables or secure credential management systems

## Transport Security (TLS)

pycubrid supports TLS for both sync and async broker connections via the `ssl`
parameter on `pycubrid.connect()` and `pycubrid.aio.connect()`. The broker
must be configured with `SSL=ON` in `cubrid_broker.conf` for TLS to succeed.

Recommended configurations, in order of preference:

1. **`ssl=True` (verified default)** — uses `ssl.create_default_context()`
   with the system trust store and `minimum_version = TLSv1_2` enforced for
   both sync and async paths. Use this when the broker presents a certificate
   chained to a publicly-trusted CA.
2. **Custom `ssl.SSLContext` with pinned CA bundle** — for self-signed or
   private-CA brokers, load the CA explicitly:

   ```python
   import ssl
   ctx = ssl.create_default_context(cafile="/etc/ssl/cubrid-ca.pem")
   ctx.minimum_version = ssl.TLSVersion.TLSv1_2
   pycubrid.connect(..., ssl=ctx)
   ```

3. **Never disable hostname/certificate verification** (`check_hostname=False`,
   `verify_mode=CERT_NONE`) in production — that defeats the purpose of TLS
   and is treated as a security issue under this policy.

### Known Limitation

On Python 3.10, `asyncio.loop.start_tls()` can hang on certificate-verify
failures (CPython [gh-142352](https://github.com/python/cpython/issues/142352),
fixed in 3.13/3.14). Tracked as
[#156](https://github.com/cubrid-lab/pycubrid/issues/156). For production
async TLS on Python 3.10, validate the certificate chain out-of-band or use
the sync path. This is a CPython bug, not a pycubrid security issue, and is
documented in detail in
[`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md#async-tls-handshake-hangs-on-python-310).

## Disclosure Policy

Once a security vulnerability is fixed:

1. A security patch will be released
2. The vulnerability will be disclosed in release notes
3. An advisory may be published on GitHub Security Advisories
4. Credit will be given to the reporter (if requested)

We appreciate your responsible disclosure and help in keeping pycubrid secure.
