from prometheus_client import Counter, Histogram

REQUEST_TIME = Histogram(
    "urban_api_request_processing_seconds",
    "Processing time histogram",
    ["method", "path"],
    buckets=[0.05, 0.2, 0.3, 0.7, 1.0, 1.5, 2.5, 5.0, 10.0, 20.0, 40.0, 60.0, 120.0],
)
"""Processing time histogram in seconds"""

REQUESTS_COUNTER = Counter("urban_api_requests_total", "Total number of requests", ["method", "path", "is_user_set"])
"""Total requests counter"""

SUCCESS_COUNTER = Counter(
    "urban_api_success_total",
    "Total number of processed requests without exceptions (including non-2xx status codes)",
    ["method", "path", "status_code"],
)
"""Total successful requests (including non-2xx status codes) counter"""

ERRORS_COUNTER = Counter(
    "urban_api_errors_total", "Total number of errors in requests", ["method", "path", "error_type", "status_code"]
)
"""Total errors (caused by exceptions) counter"""
