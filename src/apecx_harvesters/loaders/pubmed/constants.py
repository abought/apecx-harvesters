# NCBI allows 3 req/s without an API key; 10 req/s with one.
# 2.75 req/s keeps 4 consecutive requests >1s apart (otherwise shared rate limiters request too much)
rate_limit: float = 2.75