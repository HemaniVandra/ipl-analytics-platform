import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import SparkSession
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GE_ROOT      = PROJECT_ROOT / "expectations"

spark   = SparkSession.builder.getOrCreate()
context = gx.get_context(context_root_dir=str(GE_ROOT))

def run_deliveries_validation() -> bool:
    """
    Runs the deliveries expectation suite against
    ipl_catalog.silver.deliveries.
    Returns True if all critical expectations pass.
    Returns False if any critical expectation fails.
    Called by Airflow DAG after Silver transform completes.
    """
    print(f"\nRunning deliveries validation: "
          f"{datetime.now(timezone.utc).isoformat()}")

    # ── Load Silver table ──────────────────────────────────────
    df_silver = spark.table("ipl_catalog.silver.deliveries")
    print(f"Rows to validate: {df_silver.count():,}")

    # ── Batch request ──────────────────────────────────────────
    batch_request = RuntimeBatchRequest(
        datasource_name     = "databricks_datasource",
        data_connector_name = "runtime_connector",
        data_asset_name     = "silver_deliveries",
        runtime_parameters  = {"batch_data": df_silver},
        batch_identifiers   = {
            "run_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        }
    )

    # ── Run checkpoint ─────────────────────────────────────────
    results = context.run_checkpoint(
        checkpoint_name = "nightly_checkpoint",
        validations = [{
            "batch_request":           batch_request,
            "expectation_suite_name":  "deliveries_suite",
        }]
    )

    # ── Process results ────────────────────────────────────────
    validation_result = results.list_validation_results()[0]
    statistics        = validation_result["statistics"]

    total        = statistics["evaluated_expectations"]
    successful   = statistics["successful_expectations"]
    failed       = statistics["unsuccessful_expectations"]
    success_pct  = statistics["success_percent"]

    print(f"\nValidation Results:")
    print(f"  Total expectations : {total}")
    print(f"  Passed             : {successful}")
    print(f"  Failed             : {failed}")
    print(f"  Success rate       : {success_pct:.1f}%")

    # ── Print failed expectations ──────────────────────────────
    if failed > 0:
        print(f"\nFailed expectations:")
        for result in validation_result.results:
            if not result.success:
                print(f"  ✗ {result.expectation_config.expectation_type}")
                print(f"    Column : "
                      f"{result.expectation_config.kwargs.get('column', 'N/A')}")
                print(f"    Details: {result.result}")

    # ── Build data docs ────────────────────────────────────────
    context.build_data_docs()
    print(f"\nData docs updated at: "
          f"{GE_ROOT}/data_docs/local_site/index.html")

    # ── Determine pass/fail ────────────────────────────────────
    # Check if any CRITICAL expectations failed
    critical_failures = [
        r for r in validation_result.results
        if not r.success
        and r.expectation_config.meta.get("severity") == "critical"
    ]

    if critical_failures:
        print(f"\n✗ CRITICAL FAILURES: {len(critical_failures)}")
        print("Pipeline should be HALTED")
        return False

    if failed > 0:
        print(f"\n⚠ WARNING: {failed} non-critical failures")
        print("Pipeline can continue with warnings")
        return True

    print("\n✓ All expectations passed — Silver data is clean")
    return True


if __name__ == "__main__":
    passed = run_deliveries_validation()
    if not passed:
        raise RuntimeError(
            "Critical GE validation failed — "
            "bad data blocked from Gold layer"
        )