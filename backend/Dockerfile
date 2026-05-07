FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# git is needed by the GitHub-push endpoint to clone, commit, and push
# the generated Dataform project to a user-supplied repo.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Build context is the repo ROOT (set by cloudbuild.yaml), so we copy from
# both `transformation-core/` (sibling lib) and `backend/` (this service).
COPY transformation-core /app/transformation-core
RUN pip install --upgrade pip && pip install /app/transformation-core

COPY backend/pyproject.toml /app/pyproject.toml
RUN pip install /app

COPY backend/app /app/app

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
