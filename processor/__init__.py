"""Stream processor: parses, enriches and routes events out of `raw-logs`.

Run as a module so its `config` does not collide with the producer's:

    python -m processor.main
"""
