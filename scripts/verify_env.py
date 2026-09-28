#!/usr/bin/env python3
"""
scripts/verify_env.py — Environment verification script for LNTA.

Checks Python, Java, PySpark, dependencies, Hadoop, Flume, TShark,
permissions, and Spark session connectivity.
"""

import os
import shutil
import subprocess
import sys

# Ensure repo root is on sys.path
REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)


def check_python() -> tuple[bool, str, str]:
    v = sys.version_info
    ver_str = f"{v.major}.{v.minor}.{v.micro}"
    if v.major == 3 and v.minor >= 10:
        return True, f"Python {ver_str}", ""
    return False, f"Python {ver_str}", "Install Python >= 3.10"


def check_java() -> tuple[bool, str, str]:
    java_path = shutil.which("java")
    if not java_path:
        return False, "Not Found", "Install OpenJDK 11 or 17 (sudo apt install openjdk-11-jdk)"
    try:
        out = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode()
        first_line = out.splitlines()[0] if out else "Unknown"
        return True, first_line, ""
    except Exception as e:
        return False, str(e), "Check JAVA_HOME and PATH"


def check_tshark() -> tuple[bool, str, str]:
    path = shutil.which("tshark")
    if path:
        try:
            out = subprocess.check_output(
                ["tshark", "--version"], stderr=subprocess.STDOUT
            ).decode()
            ver = out.splitlines()[0] if out else "Installed"
            return True, ver, ""
        except Exception:
            return True, "Installed", ""
    return False, "Not Found", "Install Wireshark/TShark (sudo apt install tshark)"


def check_hadoop() -> tuple[bool, str, str]:
    path = shutil.which("hadoop") or shutil.which("hdfs")
    if path:
        return True, "Hadoop CLI present", ""
    if os.environ.get("HADOOP_HOME"):
        return True, f"HADOOP_HOME={os.environ['HADOOP_HOME']}", ""
    return (
        False,
        "Not Found (Optional for local mode)",
        "Install Apache Hadoop or use local file fallback",
    )


def check_flume() -> tuple[bool, str, str]:
    path = shutil.which("flume-ng")
    if path:
        return True, "Flume CLI present", ""
    return False, "Not Found (Optional for local replay)", "Install Apache Flume or use replay.py"


def check_pyspark() -> tuple[bool, str, str]:
    try:
        import pyspark

        ver = pyspark.__version__
        from streaming.common.session import get_spark

        spark = get_spark("LNTA-VerifyEnv")
        cnt = spark.range(5).count()
        tz = spark.conf.get("spark.sql.session.timeZone")
        spark.stop()
        return True, f"PySpark {ver} (count={cnt}, tz={tz})", ""
    except Exception as e:
        return False, f"Error: {e}", "pip install pyspark==3.5.0 and ensure Java 11/17"


def check_python_packages() -> tuple[bool, str, str]:
    required = ["pandas", "numpy", "yaml", "networkx", "streamlit", "plotly", "pytest", "scipy"]
    missing = []
    for pkg in required:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    if not missing:
        return True, "All required packages installed", ""
    return (
        False,
        f"Missing: {', '.join(missing)}",
        f"pip install -r requirements.txt ({' '.join(missing)})",
    )


def check_directories_and_permissions() -> tuple[bool, str, str]:
    dirs = ["data", "data/sample", "logs", "serving", "config"]
    for d in dirs:
        full_d = os.path.join(REPO_DIR, d)
        os.makedirs(full_d, exist_ok=True)
        if not os.access(full_d, os.R_OK | os.W_OK):
            return False, f"Permission error on {d}", f"chmod u+rwx {full_d}"
    return True, "All project directories writable", ""


def main():
    print("=" * 80)
    print(" LNTA Environment Verification")
    print("=" * 80)

    checks = [
        ("Python Version", check_python, True),
        ("Java (JDK)", check_java, True),
        ("Python Dependencies", check_python_packages, True),
        ("PySpark & Session", check_pyspark, True),
        ("Project Dirs & Perms", check_directories_and_permissions, True),
        ("TShark (Packet Capture)", check_tshark, False),
        ("Hadoop / HDFS", check_hadoop, False),
        ("Apache Flume", check_flume, False),
    ]

    results = []
    has_critical_failure = False

    for name, func, required in checks:
        ok, details, fix_hint = func()
        status_str = "PASS" if ok else ("FAIL" if required else "OPTIONAL")
        if not ok and required:
            has_critical_failure = True
        results.append((name, status_str, details, fix_hint))

    print(f"{'Component':<24} | {'Status':<8} | {'Details / Fix Hint':<45}")
    print("-" * 80)
    for name, status_str, details, fix_hint in results:
        status_display = f"[{status_str}]"
        info = details if status_str == "PASS" else f"{details} -> {fix_hint}"
        print(f"{name:<24} | {status_display:<8} | {info[:45]}")

    print("=" * 80)
    if has_critical_failure:
        print("[FAILURE] Critical environment requirements failed. Please apply fixes above.")
        sys.exit(1)
    else:
        print("[SUCCESS] Core environment requirements are operational.")
        sys.exit(0)


if __name__ == "__main__":
    main()
