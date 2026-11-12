# Support

## Getting Help

Start with the documentation; most problems are covered there.

- **Documentation site**: [cubrid-lab.github.io/pycubrid](https://cubrid-lab.github.io/pycubrid/)
- **First connection**: [Your First CUBRID Connection](https://cubrid-lab.github.io/pycubrid/quickstart/)
  and its section [If the first connection fails](https://cubrid-lab.github.io/pycubrid/quickstart/#if-the-first-connection-fails)
- **Errors and symptoms**: [Troubleshooting](https://cubrid-lab.github.io/pycubrid/TROUBLESHOOTING/)
- **Common questions**: [FAQ](https://cubrid-lab.github.io/pycubrid/faq/)

If the documentation does not answer it:

- **A question about using pycubrid**: ask in [GitHub Discussions](https://github.com/cubrid-lab/pycubrid/discussions).
- **A reproducible defect in pycubrid**: open a
  [bug report](https://github.com/cubrid-lab/pycubrid/issues/new?template=bug_report.yml).
- **A security problem**: do not post it publicly; see [Security](#security) below.

## Reporting Bugs

Use the [bug report template](https://github.com/cubrid-lab/pycubrid/issues/new?template=bug_report.yml).
When you ask a question or report a bug, include what you can of the following:

- pycubrid version (`python -c "import pycubrid; print(pycubrid.__version__)"`)
- Python version and operating system
- CUBRID server version (`cubrid_rel` on the server)
- How you connect: a local Docker container, a remote server, or another setup
- The full traceback, not only the last line
- A minimal script that reproduces the problem

Replace passwords, hostnames, user names and other credentials, and any real data, before
you post.

## Feature Requests

Please use the [feature request template](https://github.com/cubrid-lab/pycubrid/issues/new?template=feature_request.yml) to request features.

## Security

For security vulnerabilities, please see [SECURITY.md](SECURITY.md).
