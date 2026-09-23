#!/usr/bin/env python3
"""
scripts/verify_env.py — Environment verification script for LNTA.

Checks Python, Java, PySpark, dependencies, and Spark session connectivity.
"""

import os
import shutil
import subprocess
import sys

# Ensure repo root is on sys.path
REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)


def check_python():
    v = sys.version_info
    print(
        f"[*] Python version: {v.major}.{v.minor}.{v.micro} ({'OK' if v.major == 3 and v.minor >= 10 else 'WARN'})"
    )


def check_java():
    java_path = shutil.which("java")
    if not java_path:
        print("[!] Java: NOT FOUND in PATH")
        return False
    try:
        out = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode()
        first_line = out.splitlines()[0] if out else "Unknown"
        print(f"[*] Java version: {first_line} (OK)")
        return True
    except (subprocess.SubprocessError, OSError) as e:
        print(f"[!] Java check error: {e}")
        return False


def check_pyspark():
    try:
        import pyspark

        print(f"[*] PySpark version: {pyspark.__version__} (OK)")
        from streaming.common.session import get_spark

        spark = get_spark("LNTA-VerifyEnv")
        cnt = spark.range(5).count()
        tz = spark.conf.get("spark.sql.session.timeZone")
        print(f"[*] SparkSession test: range(5).count() = {cnt}, timeZone = {tz} (OK)")
        spark.stop()
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[!] PySpark session error: {e}")
        return False


def check_modules():
    modules = ["pandas", "numpy", "yaml", "networkx", "streamlit", "plotly", "pytest"]
    for m in modules:
        try:
            __import__(m)
            print(f"[*] Module '{m}': OK")
        except ImportError:
            print(f"[!] Module '{m}': NOT INSTALLED")


def main():
    print("=" * 60)
    print(" LNTA Environment Verification")
    print("=" * 60)
    check_python()
    check_java()
    check_modules()
    pyspark_ok = check_pyspark()
    print("=" * 60)
    if pyspark_ok:
        print("[SUCCESS] All core environment requirements are operational.")
        sys.exit(0)
    else:
        print("[FAILURE] Some environment requirements failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
