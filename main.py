"""
================================================================
 main.py — spark-submit entrypoint wrapper
================================================================
 spark-submit needs a Python FILE path, not a module name.
 This wrapper sits at /app/main.py so spark-submit can find it,
 then delegates to app.index.main() which has the real CLI logic.
================================================================
"""
import sys

from app.index import main

if __name__ == "__main__":
    sys.exit(main())
