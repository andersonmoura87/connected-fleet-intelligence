# linux/amd64 runtime. Digests were read from the public registries on 2026-10-19.
# python:3.12-slim index sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
# The amd64 manifest below is the image this file pins. It is a glibc Debian image, not Alpine.
# uv 0.13.1 linux/amd64 sha256:9a853e40f2eb00f980d265a84fba919dc38ff17750197e7f13c055e4e2f12edd

FROM --platform=linux/amd64 python:3.12-slim@sha256:2b4f19dae3a777dfc3b76730bda1e82e1f66ab2a2686fa93ca78edbfb4f04ffe AS builder

COPY --from=ghcr.io/astral-sh/uv:0.13.1@sha256:9a853e40f2eb00f980d265a84fba919dc38ff17750197e7f13c055e4e2f12edd /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /opt/cfi
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
# --no-editable installs the package into the venv. An editable install would
# point at /opt/cfi/src, which the runtime stage does not copy.
RUN uv sync --frozen --no-dev --no-editable --python /usr/local/bin/python

FROM --platform=linux/amd64 python:3.12-slim@sha256:2b4f19dae3a777dfc3b76730bda1e82e1f66ab2a2686fa93ca78edbfb4f04ffe

RUN groupadd --gid 10001 cfi \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin cfi \
    && mkdir /data \
    && chown cfi:cfi /data

COPY --from=builder --chown=cfi:cfi /opt/cfi/.venv /opt/cfi/.venv

ENV PATH="/opt/cfi/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    CFI_DATA_ROOT=/data

USER cfi
WORKDIR /opt/cfi
ENTRYPOINT ["cfi"]
CMD ["serve"]
