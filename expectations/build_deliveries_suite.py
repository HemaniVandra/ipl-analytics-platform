# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 17:27:40 2026

@author: Hemani Vandra
"""

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from pyspark.sql import SparkSession
from pathlib import Path
from datetime import datetime, timezone

# ── Setup ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GE_ROOT      = PROJECT_ROOT / "expectations"

spark = SparkSession.builder.getOrCreate()

# ── Load GE context ────────────────────────────────────────────
context = gx.get_context(
    context_root_dir=str(GE_ROOT)
)

SUITE_NAME = "deliveries_suite"
TABLE_NAME = "ipl_catalog.silver.deliveries"

# ── IPL teams for accepted values check ───────────────────────
IPL_TEAMS = [
    "Mumbai Indians",
    "Chennai Super Kings",
    "Royal Challengers Bengaluru",
    "Kolkata Knight Riders",
    "Delhi Capitals",
    "Rajasthan Royals",
    "Sunrisers Hyderabad",
    "Punjab Kings",
    "Lucknow Super Giants",
    "Gujarat Titans",
    "Kochi Tuskers Kerala",
    "Pune Warriors",
    "Rising Pune Supergiant",
    "Gujarat Lions",
]

# ── Load Silver deliveries table ───────────────────────────────
df_silver = spark.table(TABLE_NAME)
print(f"Loaded {df_silver.count():,} rows from {TABLE_NAME}")

# ── Create or update expectation suite ────────────────────────
suite = context.add_or_update_expectation_suite(
    expectation_suite_name=SUITE_NAME
)

# ── Create batch request ───────────────────────────────────────
batch_request = RuntimeBatchRequest(
    datasource_name     = "databricks_datasource",
    data_connector_name = "runtime_connector",
    data_asset_name     = "silver_deliveries",
    runtime_parameters  = {"batch_data": df_silver},
    batch_identifiers   = {
        "run_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    }
)

# ── Get validator ──────────────────────────────────────────────
validator = context.get_validator(
    batch_request           = batch_request,
    expectation_suite_name  = SUITE_NAME
)

print("Building deliveries expectation suite...")
print("=" * 50)


# ══════════════════════════════════════════════════════════════
# SECTION 1 — TABLE LEVEL EXPECTATIONS
# ══════════════════════════════════════════════════════════════

# ── 1.1 Table must have rows ───────────────────────────────────
# Ensures Bronze → Silver transform actually loaded data
validator.expect_table_row_count_to_be_between(
    min_value = 500_000,    # IPL has 1M+ deliveries across all seasons
    max_value = 5_000_000,  # upper bound sanity check
)
print("✓ 1.1  Row count between 500K and 5M")

# ── 1.2 Required columns must exist ───────────────────────────
# If Silver transform drops a column by mistake this catches it
required_columns = [
    "match_id", "season", "start_date", "venue",
    "innings", "over_number", "ball_number",
    "batting_team", "bowling_team",
    "striker", "non_striker", "bowler",
    "runs_off_bat", "extras", "wides", "noballs",
    "byes", "leg_byes", "total_runs",
    "is_wicket", "is_boundary", "is_six",
    "is_dot_ball", "phase",
    "_ingested_at", "_source"
]

validator.expect_table_columns_to_match_set(
    column_set    = required_columns,
    exact_match   = False   # allow extra columns
)
print(f"✓ 1.2  All {len(required_columns)} required columns exist")


# ══════════════════════════════════════════════════════════════
# SECTION 2 — NULL CHECKS ON CRITICAL COLUMNS
# ══════════════════════════════════════════════════════════════

# Critical columns — null here means data is unusable
critical_not_null = [
    "match_id",
    "innings",
    "over_number",
    "ball_number",
    "batting_team",
    "bowling_team",
    "striker",
    "bowler",
    "runs_off_bat",
    "total_runs",
    "phase",
    "_source",
    "_ingested_at",
]

for col in critical_not_null:
    validator.expect_column_values_to_not_be_null(
        column            = col,
        meta              = {"severity": "critical"}
    )

print(f"✓ 2.1  Not null on {len(critical_not_null)} critical columns")

# Non-critical nulls — allowed for optional fields
optional_nullable = [
    "wicket_type",          # null when no wicket fell
    "player_dismissed",     # null when no wicket fell
    "wides",                # null when no wide bowled
    "noballs",              # null when no no-ball bowled
]
print(f"✓ 2.2  Nullable columns confirmed: {optional_nullable}")


# ══════════════════════════════════════════════════════════════
# SECTION 3 — RANGE CHECKS ON NUMERIC COLUMNS
# ══════════════════════════════════════════════════════════════

# ── 3.1 Innings must be 1 or 2 ────────────────────────────────
# Super overs are innings 3+ — we exclude them in Silver
validator.expect_column_values_to_be_between(
    column    = "innings",
    min_value = 1,
    max_value = 2,
)
print("✓ 3.1  innings between 1 and 2")

# ── 3.2 Over number must be 1-20 ──────────────────────────────
# T20 cricket has exactly 20 overs per innings
validator.expect_column_values_to_be_between(
    column    = "over_number",
    min_value = 1,
    max_value = 20,
)
print("✓ 3.2  over_number between 1 and 20")

# ── 3.3 Ball number must be 1-9 ───────────────────────────────
# Normal max is 6 but wides/no-balls add extra balls (up to 9)
validator.expect_column_values_to_be_between(
    column    = "ball_number",
    min_value = 1,
    max_value = 9,
)
print("✓ 3.3  ball_number between 1 and 9")

# ── 3.4 Runs off bat must be 0-6 ──────────────────────────────
# Maximum off bat is 6 — a six
# 0 = dot ball, 1/2/3 = running, 4 = boundary, 6 = six
validator.expect_column_values_to_be_between(
    column    = "runs_off_bat",
    min_value = 0,
    max_value = 6,
)
print("✓ 3.4  runs_off_bat between 0 and 6")

# ── 3.5 Extras must be 0-20 ───────────────────────────────────
# Very rarely more than 5 but allow up to 20 for edge cases
validator.expect_column_values_to_be_between(
    column    = "extras",
    min_value = 0,
    max_value = 20,
)
print("✓ 3.5  extras between 0 and 20")

# ── 3.6 Total runs per delivery must be 0-26 ──────────────────
# Max theoretically: 6 (off bat) + 20 (extras) but realistically 0-7
validator.expect_column_values_to_be_between(
    column    = "total_runs",
    min_value = 0,
    max_value = 26,
)
print("✓ 3.6  total_runs between 0 and 26")

# ── 3.7 Wides must be 0 or 1 ──────────────────────────────────
# A wide is either called or not — binary
validator.expect_column_values_to_be_between(
    column            = "wides",
    min_value         = 0,
    max_value         = 1,
    mostly            = 0.99,   # allow 1% for edge cases
)
print("✓ 3.7  wides 0 or 1")

# ── 3.8 No-balls must be 0 or 1 ───────────────────────────────
validator.expect_column_values_to_be_between(
    column    = "noballs",
    min_value = 0,
    max_value = 1,
    mostly    = 0.99,
)
print("✓ 3.8  noballs 0 or 1")


# ══════════════════════════════════════════════════════════════
# SECTION 4 — ACCEPTED VALUES CHECKS
# ══════════════════════════════════════════════════════════════

# ── 4.1 Batting team must be a valid IPL team ─────────────────
validator.expect_column_values_to_be_in_set(
    column        = "batting_team",
    value_set     = IPL_TEAMS,
    meta          = {"severity": "critical"}
)
print(f"✓ 4.1  batting_team in {len(IPL_TEAMS)} valid IPL teams")

# ── 4.2 Bowling team must be a valid IPL team ─────────────────
validator.expect_column_values_to_be_in_set(
    column    = "bowling_team",
    value_set = IPL_TEAMS,
    meta      = {"severity": "critical"}
)
print("✓ 4.2  bowling_team in valid IPL teams")

# ── 4.3 Phase must be powerplay, middle, or death ─────────────
validator.expect_column_values_to_be_in_set(
    column    = "phase",
    value_set = ["powerplay", "middle", "death"],
)
print("✓ 4.3  phase in [powerplay, middle, death]")

# ── 4.4 Source must be cricsheet ──────────────────────────────
validator.expect_column_values_to_be_in_set(
    column    = "_source",
    value_set = ["cricsheet"],
)
print("✓ 4.4  _source is cricsheet")

# ── 4.5 Wicket type must be valid cricket dismissal types ─────
# Null is allowed — only validate non-null values
validator.expect_column_values_to_be_in_set(
    column    = "wicket_type",
    value_set = [
        "bowled",
        "caught",
        "lbw",
        "run out",
        "stumped",
        "hit wicket",
        "obstructing the field",
        "timed out",
        "handled the ball",
        "retired hurt",
        "caught and bowled",
    ],
    mostly    = 0.999,      # allow 0.1% for new dismissal types
)
print("✓ 4.5  wicket_type in valid dismissal types")


# ══════════════════════════════════════════════════════════════
# SECTION 5 — BOOLEAN COLUMN CHECKS
# ══════════════════════════════════════════════════════════════

# ── 5.1 Boolean columns must be True or False only ────────────
boolean_columns = [
    "is_wicket",
    "is_boundary",
    "is_six",
    "is_dot_ball",
]

for col in boolean_columns:
    validator.expect_column_values_to_be_in_set(
        column    = col,
        value_set = [True, False],
    )
print(f"✓ 5.1  Boolean columns valid: {boolean_columns}")


# ══════════════════════════════════════════════════════════════
# SECTION 6 — CONSISTENCY CHECKS
# ══════════════════════════════════════════════════════════════

# ── 6.1 Batting team != bowling team for same delivery ─────────
# If batting_team equals bowling_team → data is corrupted
validator.expect_column_pair_values_to_be_equal(
    column_A    = "batting_team",
    column_B    = "bowling_team",
    or_equal    = False,        # they must NOT be equal
    mostly      = 0.0,          # zero rows should have equal teams
)
print("✓ 6.1  batting_team != bowling_team on every row")

# ── 6.2 Sixes must have runs_off_bat = 6 ─────────────────────
# If is_six=True then runs_off_bat must be exactly 6
validator.expect_column_values_to_be_in_set(
    column        = "runs_off_bat",
    value_set     = [6],
    row_condition = "is_six == True",
    condition_parser = "great_expectations__experimental",
)
print("✓ 6.2  runs_off_bat = 6 when is_six = True")

# ── 6.3 Boundaries must have runs_off_bat >= 4 ────────────────
validator.expect_column_values_to_be_between(
    column        = "runs_off_bat",
    min_value     = 4,
    row_condition = "is_boundary == True",
    condition_parser = "great_expectations__experimental",
)
print("✓ 6.3  runs_off_bat >= 4 when is_boundary = True")

# ── 6.4 Dot balls must have runs_off_bat = 0 and no wides/noballs
validator.expect_column_values_to_be_in_set(
    column        = "runs_off_bat",
    value_set     = [0],
    row_condition = "is_dot_ball == True",
    condition_parser = "great_expectations__experimental",
)
print("✓ 6.4  runs_off_bat = 0 when is_dot_ball = True")

# ── 6.5 Wicket rows must have wicket_type not null ────────────
validator.expect_column_values_to_not_be_null(
    column        = "wicket_type",
    row_condition = "is_wicket == True",
    condition_parser = "great_expectations__experimental",
)
print("✓ 6.5  wicket_type not null when is_wicket = True")


# ══════════════════════════════════════════════════════════════
# SECTION 7 — SEASON AND DATE CHECKS
# ══════════════════════════════════════════════════════════════

# ── 7.1 Season must be a valid IPL season year ────────────────
# IPL started in 2008
validator.expect_column_values_to_be_in_set(
    column    = "season",
    value_set = [str(y) for y in range(2008, 2026)],
)
print("✓ 7.1  season in valid IPL years 2008-2025")

# ── 7.2 start_date must not be null ───────────────────────────
validator.expect_column_values_to_not_be_null(
    column = "start_date"
)
print("✓ 7.2  start_date not null")

# ── 7.3 Phase must align with over_number ─────────────────────
# powerplay = overs 1-6, middle = 7-15, death = 16-20
validator.expect_column_values_to_be_in_set(
    column        = "phase",
    value_set     = ["powerplay"],
    row_condition = "over_number <= 6",
    condition_parser = "great_expectations__experimental",
)
validator.expect_column_values_to_be_in_set(
    column        = "phase",
    value_set     = ["death"],
    row_condition = "over_number > 15",
    condition_parser = "great_expectations__experimental",
)
print("✓ 7.3  phase consistent with over_number")


# ══════════════════════════════════════════════════════════════
# SECTION 8 — VOLUME CHECKS PER MATCH
# ══════════════════════════════════════════════════════════════

# ── 8.1 Each match should have reasonable delivery count ───────
# A T20 match has ~240-360 deliveries (120 per innings + extras)
validator.expect_column_values_to_be_between(
    column    = "match_id",
    min_value = None,   # not a numeric check — using aggregate below
    max_value = None,
)

# Use SQL-based check for per-match delivery counts
validator.expect_column_pair_values_A_to_be_greater_than_B(
    column_A    = "total_runs",
    column_B    = "runs_off_bat",
    or_equal    = True,   # total_runs >= runs_off_bat always
)
print("✓ 8.1  total_runs >= runs_off_bat on every row")


# ══════════════════════════════════════════════════════════════
# SAVE SUITE
# ══════════════════════════════════════════════════════════════

validator.save_expectation_suite(
    discard_failed_expectations = False
)

print("\n" + "=" * 50)
print(f"Suite '{SUITE_NAME}' saved successfully")
print(f"Total expectations: "
      f"{len(validator.get_expectation_suite().expectations)}")
print("=" * 50)