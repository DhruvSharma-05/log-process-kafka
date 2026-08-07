"""HTTP log-ingestion gateway (FR1.1 — direct producer SDK).

    python -m ingest.main

Accepts logs over HTTP from any language or tool and writes them into
`raw-logs` in exactly the envelope the file replayer produces, so everything
downstream is unchanged.
"""
