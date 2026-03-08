"""CloudWatch alarm configuration — pseudo-code.

Two categories of alarms:
1. Lambda failure alarms: one per Lambda, fires after all retries exhausted
2. Quarantine alarms: one per source, fires on non-zero object count in that
   source's quarantine prefix — per-source granularity so the team knows
   immediately which source is producing bad data.

In production, these would be defined via AWS CDK.
"""

SNS_TOPIC = "arn:aws:sns:us-east-1:ACCOUNT_ID:bluealpha-pipeline-alerts"

# ---------------------------------------------------------------------------
# Lambda failure alarms
# ---------------------------------------------------------------------------
# One alarm per Lambda function. Triggers when the Errors metric exceeds 0,
# meaning at least one invocation failed after all retries were exhausted
# and the event was sent to the dead-letter queue.

# ---------------------------------------------------------------------------
# Quarantine alarms
# ---------------------------------------------------------------------------
# One alarm per source. Uses a custom CloudWatch metric published by a
# scheduled Lambda (or S3 inventory) that counts objects under each
# quarantine prefix. Fires when count > 0 so the team investigates
# which rows failed and why.
#
# Per-source granularity matters: when an alarm fires, on-call immediately
# knows which source is producing bad data without digging through logs.