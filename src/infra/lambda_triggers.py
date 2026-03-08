"""Lambda trigger configuration — pseudo-code.

Defines the S3 event notifications that trigger each transformation Lambda,
and the EventBridge cron rule for the Google Ads ingestion Lambda.

In production these would be defined via AWS CDK.
"""

LAMBDA_TRIGGERS = {
    # Google Ads ingestion: cron-triggered, no S3 event. Timing depends on business needs.

    # Google Ads transformation: fires when ingestion Lambda writes to bronze

    # Facebook transformation: fires when vendor uploads CSV to bronze

    # CRM transformation: fires when vendor uploads CSV to bronze
}

RETRY_CONFIG = {
    # All Lambdas use asynchronous invocation with built-in retry:
    #   MaximumRetryAttempts: 2 (3 total attempts including the initial invocation)
    #   MaximumEventAgeInSeconds: 3600 (discard events older than 1 hour)
    #
    # After all retries are exhausted, failed events are sent to quarantine S3. 
    # CloudWatch alarms fire on quarantine depth > 0.
}
