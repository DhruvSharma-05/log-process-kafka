"""Log producer: replays log files and synthetic streams into `raw-logs`.

Run as a module so its `config` does not collide with the processor's:

    python -m producer.main --file data/NASA_access_log_Aug95 --rate 2000
"""
