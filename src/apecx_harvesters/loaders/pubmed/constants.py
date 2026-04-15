# NCBI allows 3 req/s without an API key; 10 req/s with one.
# Apply small fudge factor because we still get 429s if too close to the max
_margin = 0.9
rate_limit: float = 3 * _margin
rate_limit_with_key: float = 10.0 * _margin