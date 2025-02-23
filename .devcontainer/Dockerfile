FROM mcr.microsoft.com/devcontainers/python:dev-3.13-bookworm

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
WORKDIR /sql-db-utils
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
COPY . .
RUN mkdir -p ./.vscode
CMD ["tail", "-f", "/dev/null"]
