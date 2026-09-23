"""One-shot mock of a completed session whose candidate drops its final CDR.

This extends the frozen amount mock without changing its historical fingerprints.
The new run fingerprints cover both this module and the billing module it uses,
with CRLF normalized to LF, plus the selected behavior. They are not Git SHAs.
Only Bronze input evidence is used; absent output is represented by zero rows.
"""

from decimal import InvalidOperation, localcontext
import hashlib
from pathlib import Path

if __package__:
    from . import mock_billing
else:
    import mock_billing


MISSING_CDR_RUN_IDS = ("mock-missing-cdr-bad-v1", "mock-missing-cdr-fixed-v1")


def _release_cdrs(parsed, tariff_body, price, *, drop_final_cdr):
    # Deliberate candidate defect: completing the session emits no final record.
    if drop_final_cdr:
        return []
    return [mock_billing._cdr(parsed, tariff_body, price, faulty=False)]


def generate_missing_cdr_runs(events, tariff, manifest):
    """Return deterministic Bronze rows for faulty and corrected missing-CDR runs.

    Both roles see the same validated events, tariff, seed and clock. The bad
    candidate omits its CDR entirely; the baseline and corrected candidate each
    calculate one healthy CDR independently of the Silver expected ledger.
    """
    try:
        with localcontext() as context:
            context.prec = 60
            ordered, parsed, tariff_body, price = mock_billing._validated_inputs(
                events, tariff, manifest
            )
            sources = (
                b"mock_billing.py\0"
                + Path(mock_billing.__file__).read_bytes().replace(b"\r\n", b"\n")
                + b"\0mock_missing_cdr.py\0"
                + Path(__file__).read_bytes().replace(b"\r\n", b"\n")
            )

            def fingerprint(variant):
                return hashlib.sha1(sources + b"\0" + variant.encode("ascii")).hexdigest()

            output = {name: [] for name in (
                "run_manifest", "ocpp_transaction_events_raw", "tariffs_raw", "ocpi_cdrs_raw"
            )}
            for run_id in MISSING_CDR_RUN_IDS:
                faulty_candidate = run_id == MISSING_CDR_RUN_IDS[0]
                output["run_manifest"].append({
                    "run_id": run_id, "scenario_id": "mock-missing-cdr-v1", "seed": manifest["seed"],
                    "baseline_sha": fingerprint("healthy-v1"),
                    "candidate_sha": fingerprint("drop-final-cdr-v1" if faulty_candidate else "healthy-v1"),
                    "tariff_hash": mock_billing._sha256(tariff["payload"]),
                    "created_at": manifest["created_at"],
                })
                output["ocpp_transaction_events_raw"].extend(dict(row, run_id=run_id) for row in ordered)
                output["tariffs_raw"].append(dict(tariff, run_id=run_id))
                for role in ("baseline", "candidate"):
                    output["ocpi_cdrs_raw"].extend(
                        dict(row, run_id=run_id, release_role=role)
                        for row in _release_cdrs(
                            parsed, tariff_body, price,
                            drop_final_cdr=faulty_candidate and role == "candidate",
                        )
                    )
            return output
    except (KeyError, IndexError, TypeError, InvalidOperation) as error:
        raise ValueError(f"Malformed or unsupported missing-CDR input: {error}") from error


def generate_all_mock_runs(events, tariff, manifest):
    """Keep the original amount evidence unchanged and append missing-CDR runs."""
    output = mock_billing.generate_mock_runs(events, tariff, manifest)
    missing = generate_missing_cdr_runs(events, tariff, manifest)
    for table, rows in missing.items():
        output[table].extend(rows)
    return output
