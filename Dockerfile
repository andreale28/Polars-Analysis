FROM python:3.10.10-slim-bullseye

ENV POETRY_VERSION=1.5.0

ENV POETRY_HOME=/opt/poetry
ENV POETRY_VIRTUALENVS_CREATE=false
ENV PATH="$POETRY_HOME/bin:$PATH"

# Nice-to-have optimizations
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=off
ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PIP_DEFAULT_TIMEOUT=100
ENV POETRY_CACHE_DIR=/tmp/poetry_cache

# Install Poetry
RUN apt-get update && apt-get install --no-install-recommends -y curl \
 && curl -sSL https://install.python-poetry.org | python - \
 && apt-get purge --auto-remove -y curl \
 && rm -rf /var/lib/apt/lists/*

# Install Python runtime dependencies
# (Some packages require compilers to be installed - this package doesn't actually need build-essential)
WORKDIR /app
COPY pyproject.toml poetry.lock .env ./
RUN apt-get update && apt-get install --no-install-recommends -y build-essential  \
 && poetry install --no-root --only main \
 && apt-get purge --auto-remove -y build-essential \
 && rm -rf /var/lib/apt/lists/*

# Install application
COPY script ./script
RUN poetry install --only main --without dev && rm -rf $POETRY_CACHE_DIR

# Switch to non-root user
#ENV USER=ah
#ENV UID=12321
#ENV GID=12321
#RUN groupadd --system --gid $GID $USER \
# && useradd \
#    --system \
#    --no-log-init \
#    --uid $UID \
#    --gid $GID \
#    $USER
#USER $USER

ENTRYPOINT ["python", "-m", "script.main"]
