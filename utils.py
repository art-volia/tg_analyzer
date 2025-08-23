import random, time, pathlib
from loguru import logger

def setup_logger(log_path: str):
    import logging
    from datetime import datetime
    from zoneinfo import ZoneInfo
    class TZFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=ZoneInfo("Europe/Bucharest"))
            if datefmt:
                return dt.strftime(datefmt)
            return dt.isoformat()

    pathlib.Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(log_path, rotation="20 MB", retention="15 days", enqueue=True)
    logger.add(lambda msg: print(msg, end=""))
    return logger

def sleep_range(a: float, b: float):
    time.sleep(random.uniform(a, b))

def jitter_ms(a_ms: int, b_ms: int):
    time.sleep(random.uniform(a_ms, b_ms)/1000.0)
