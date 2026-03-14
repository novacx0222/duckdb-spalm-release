FROM spalm-release

# Inherit the full spalm-release environment (Intel oneAPI MKL, Python venv,
# and the docker-entrypoint.sh that sources setvars.sh on startup).
# The DuckDB binary, extension build, and spalm.conf are bind-mounted at runtime
# so no project files are baked into the image.
