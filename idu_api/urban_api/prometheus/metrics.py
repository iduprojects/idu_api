from prometheus_client import Counter, Histogram

REQUEST_TIME = Histogram("urban_api_request_processing_seconds", "Processing time histogram")
"""Processing time histogram in seconds"""

REQUESTS_COUNTER = Counter("urban_api_requests_total", "Total number of requests", ["method", "path"])
"""Total requests counter"""

SUCCESS_COUNTER = Counter("urban_api_success_total", "Total number of requests without errors", ["status_code"])
"""Total successful requests counter"""

ERRORS_COUNTER = Counter("urban_api_errors_total", "Total number of errors in requests", ["error_type"])
"""Total errors counter"""
