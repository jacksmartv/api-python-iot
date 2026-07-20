FROM python:3.12-slim

WORKDIR /app

# Copy dependency files
COPY pyproject.toml .

# Install Python dependencies
RUN pip install --no-cache-dir .

# Copy source code and scripts
COPY src/ ./src/
COPY migrations/ ./migrations/
COPY scripts/ ./scripts/
COPY entrypoint.sh .

# Create non-root user and set permissions
# /app/data/floorplans es punto de montaje del volumen; precrearlo con ownership
# de appuser para que el pipeline pueda escribir (el volumen hereda el owner del dir).
RUN useradd -m -u 1000 appuser && \
    mkdir -p /app/data/floorplans && \
    chown -R appuser:appuser /app && \
    chmod +x /app/entrypoint.sh
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Run the application with migrations
CMD ["/app/entrypoint.sh"]
