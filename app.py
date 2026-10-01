import argparse
import signal
import sys
from pathlib import Path

from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def main(argv=None):
    parser = argparse.ArgumentParser(description="基因组数据访问治理")
    parser.add_argument("--db", default="./data.db", help="SQLite database path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8304)
    args = parser.parse_args(argv)

    repository = SQLiteRepository(args.db)
    rules = RuleEngine()
    service = DomainService(repository, rules)
    static_dir = Path(__file__).resolve().parent / "static"
    server = create_server(args.host, args.port, service, rules, str(static_dir))

    def stop(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    try:
        print("基因组数据访问治理 listening on http://%s:%s" % (args.host, args.port), flush=True)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
