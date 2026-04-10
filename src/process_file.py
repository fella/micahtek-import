from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

from src.db import get_db_connection
from src.generate_transaction_key import generate_transaction_key
from src.hubspot_client import HubSpotClient
from src.normalize_record import normalize_record
from src.parse_crd import parse_crd_file
from src.settings import load_settings


@dataclass(frozen=True)
class ProcessSummary:
    run_id: UUID
    source_filename: str
    records_found: int
    records_succeeded: int
    records_failed: int
    duplicates_skipped: int
    dry_run: bool


def _create_import_run(conn, run_id: UUID, source_filename: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO import_runs (run_id, source_filename, status)
            VALUES (%s, %s, %s)
            """,
            (run_id, source_filename, "running"),
        )
    conn.commit()


def _complete_import_run(
    conn,
    run_id: UUID,
    status: str,
    records_found: int,
    records_succeeded: int,
    records_failed: int,
    duplicates_skipped: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE import_runs
            SET status = %s,
                completed_at = NOW(),
                records_found = %s,
                records_succeeded = %s,
                records_failed = %s,
                duplicates_skipped = %s
            WHERE run_id = %s
            """,
            (
                status,
                records_found,
                records_succeeded,
                records_failed,
                duplicates_skipped,
                run_id,
            ),
        )
    conn.commit()


def _has_idempotency_key(conn, transaction_key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM idempotency_keys WHERE transaction_key = %s",
            (transaction_key,),
        )
        return cur.fetchone() is not None


def _save_idempotency_key(conn, transaction_key: str, source_filename: str, run_id: UUID) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO idempotency_keys (transaction_key, source_filename, run_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (transaction_key)
            DO UPDATE SET last_seen_at = NOW()
            """,
            (transaction_key, source_filename, run_id),
        )
    conn.commit()


def _save_import_record(
    conn,
    run_id: UUID,
    source_filename: str,
    source_line: int,
    transaction_key: str | None,
    donor_identifier: str | None,
    amount,
    status: str,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO import_records (
                run_id,
                source_filename,
                source_line,
                transaction_key,
                donor_identifier,
                amount,
                status,
                error_code,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                source_filename,
                source_line,
                transaction_key,
                donor_identifier,
                amount,
                status,
                error_code,
                error_message,
            ),
        )
    conn.commit()


def process_file(file_path: str, dry_run: bool) -> ProcessSummary:
    settings = load_settings()
    source_filename = Path(file_path).name
    run_id = uuid4()

    records_found = 0
    records_succeeded = 0
    records_failed = 0
    duplicates_skipped = 0

    hubspot = HubSpotClient(
        access_token=settings.hubspot_access_token,
        base_url=settings.hubspot_base_url,
    )

    with get_db_connection() as conn:
        _create_import_run(conn, run_id, source_filename)

        try:
            raw_records = parse_crd_file(file_path)
            records_found = len(raw_records)

            for raw_record in raw_records:
                try:
                    normalized = normalize_record(raw_record)
                    transaction_key = generate_transaction_key(source_filename, normalized)

                    if _has_idempotency_key(conn, transaction_key):
                        duplicates_skipped += 1
                        _save_import_record(
                            conn=conn,
                            run_id=run_id,
                            source_filename=source_filename,
                            source_line=raw_record.source_line,
                            transaction_key=transaction_key,
                            donor_identifier=normalized.donor_identifier,
                            amount=normalized.amount,
                            status="duplicate",
                        )
                        continue

                    if not dry_run:
                        hubspot.upsert_donation(
                            donor_identifier=normalized.donor_identifier,
                            amount=str(normalized.amount) if normalized.amount is not None else None,
                            transaction_key=transaction_key,
                        )

                    _save_idempotency_key(conn, transaction_key, source_filename, run_id)
                    _save_import_record(
                        conn=conn,
                        run_id=run_id,
                        source_filename=source_filename,
                        source_line=raw_record.source_line,
                        transaction_key=transaction_key,
                        donor_identifier=normalized.donor_identifier,
                        amount=normalized.amount,
                        status="processed" if not dry_run else "dry-run",
                    )
                    records_succeeded += 1

                except Exception as record_error:
                    records_failed += 1
                    _save_import_record(
                        conn=conn,
                        run_id=run_id,
                        source_filename=source_filename,
                        source_line=raw_record.source_line,
                        transaction_key=None,
                        donor_identifier=None,
                        amount=None,
                        status="failed",
                        error_code="RECORD_ERROR",
                        error_message=str(record_error),
                    )

            final_status = "completed" if records_failed == 0 else "completed_with_errors"
            _complete_import_run(
                conn=conn,
                run_id=run_id,
                status=final_status,
                records_found=records_found,
                records_succeeded=records_succeeded,
                records_failed=records_failed,
                duplicates_skipped=duplicates_skipped,
            )

        except Exception as run_error:
            _complete_import_run(
                conn=conn,
                run_id=run_id,
                status=f"failed: {run_error}",
                records_found=records_found,
                records_succeeded=records_succeeded,
                records_failed=records_failed,
                duplicates_skipped=duplicates_skipped,
            )
            raise

    return ProcessSummary(
        run_id=run_id,
        source_filename=source_filename,
        records_found=records_found,
        records_succeeded=records_succeeded,
        records_failed=records_failed,
        duplicates_skipped=duplicates_skipped,
        dry_run=dry_run,
    )