# CI PyYAML wheel

Python 3.15 uses `pyyaml-6.0.3-1-py3-none-any.whl` because PyYAML 6.0.3
does not publish a compatible wheel. Other Python versions use the registry
distribution selected by `uv.lock`.

This wheel contains the upstream Python modules, metadata and MIT licence.
The optional CPython 3.14 extension was removed. `WHEEL` declares a pure Python
build, and the standard `wheel` tool regenerated `RECORD`. No compiled extension
was retagged. PyYAML uses its pure Python implementation on Python 3.15.

| File | SHA-256 |
| --- | --- |
| `pyyaml-6.0.3-cp314-cp314-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl` | `c458b6d084f9b935061bc36216e8a69a7e293a2f1e68bf956dcd9e6cbcd143f5` |
| `pyyaml-6.0.3-1-py3-none-any.whl` | `39f625849937dafa7166aea5625eb69415e16175a9422ef045e2d5685399393e` |

Reproduce with the locked `wheel==0.48.0` tool in an empty temporary directory:

```sh
python -m pip download --only-binary :all: --no-deps \
  --platform manylinux_2_28_x86_64 --python-version 3.14 --implementation cp \
  --abi cp314 --index-url https://pypi.org/simple PyYAML==6.0.3
echo 'c458b6d084f9b935061bc36216e8a69a7e293a2f1e68bf956dcd9e6cbcd143f5  pyyaml-6.0.3-cp314-cp314-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl' | sha256sum -c -
python -m wheel unpack pyyaml-6.0.3-cp314-cp314-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl
rm pyyaml-6.0.3/yaml/_yaml.cpython-314-x86_64-linux-gnu.so
sed -i 's/Root-Is-Purelib: false/Root-Is-Purelib: true/; s/^Tag: .*/Tag: py3-none-any/' \
  pyyaml-6.0.3/pyyaml-6.0.3.dist-info/WHEEL
SOURCE_DATE_EPOCH=315532800 python -m wheel pack --build-number 1 pyyaml-6.0.3
sha256sum pyyaml-6.0.3-1-py3-none-any.whl
```

After a dependency update, regenerate `uv.lock` with uv 0.12.17 and qualify
the affected groups on supported Python versions. Remove this local source
override when an upstream wheel supports Python 3.15 and passes those checks.
The CI wheel installs reject source distributions; a missing compatible wheel
must fail the job instead of executing a registry build backend.
