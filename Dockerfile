# Small, explicit image. Using a Dockerfile rather than buildpacks so the
# entrypoint is obvious and `--args` (e.g. --dry-run, --start) pass straight
# through to the script.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pull_eagle.py .

ENTRYPOINT ["python", "pull_eagle.py"]
