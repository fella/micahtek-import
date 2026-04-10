import argparse
import json
from dataclasses import asdict

from src.process_file import process_file
from src.settings import load_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a MicahTek import.")
    parser.add_argument("--file", required=True, help="Path to the .CRD file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and log records without calling HubSpot",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Override default dry-run behavior and enable real outbound calls",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = load_settings()

    dry_run = True
    if args.live:
        dry_run = False
    elif args.dry_run:
        dry_run = True
    else:
        dry_run = settings.default_dry_run

    summary = process_file(file_path=args.file, dry_run=dry_run)
    print(json.dumps(asdict(summary), indent=2, default=str))


if __name__ == "__main__":
    main()