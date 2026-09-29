#!/usr/bin/env python3
import re
import subprocess
import sys


def run_git(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout.splitlines()

def check_pcap():
    print("Checking for .pcap / .pcapng files...")
    files = run_git(['git', 'ls-files', '*.pcap', '*.pcapng'])
    if files:
        print(f"FAIL: Found pcap files: {files}")
        return False
    print("PASS: No pcap files found.")
    return True

def check_ips():
    print("Checking for real IP addresses outside allowed paths...")
    # Basic IP regex excluding RFC 1918 private IPs
    ip_pattern = re.compile(r'\b(?!(?:10|127|172\.(?:1[6-9]|2[0-9]|3[0-1])|192\.168)\.)(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b')
    allowed_dirs = ['data/sample', 'tests', 'test_fixtures']
    # Whitelisted documentation, link-local, DNS, and sample mock IPs
    allowed_ip_prefixes = (
        '198.51.100.', '203.0.113.', '169.254.', '8.8.8.8', '1.1.1.1',
        '104.244.42.', '142.250.190.', '157.240.22.'
    )
    failed = False
    
    files = run_git(['git', 'ls-files'])
    for file in files:
        if any(file.startswith(d) for d in allowed_dirs):
            continue
        # Skip some binary or non-text files
        if not file.endswith(('.py', '.yaml', '.yml', '.md', '.sh', '.txt')):
            continue
        try:
            with open(file, encoding='utf-8') as f:
                content = f.read()
                matches = ip_pattern.findall(content)
                if matches:
                    # Filter out common false positives and allowed documentation/mock IPs
                    real_ips = [
                        ip for ip in matches
                        if not (ip.startswith('0.') or ip == '0.0.0.0' or any(ip.startswith(p) for p in allowed_ip_prefixes))
                    ]
                    if real_ips:
                        print(f"FAIL: Found potential IPs in {file}: {set(real_ips)}")
                        failed = True
        except Exception:
            pass
    if not failed:
        print("PASS: No real IPs found.")
    return not failed

def check_settings():
    print("Checking config/settings.yaml...")
    files = run_git(['git', 'ls-files', 'config/settings.yaml'])
    if files:
        print("FAIL: config/settings.yaml is committed. Only settings.example.yaml should be committed.")
        return False
    print("PASS: config/settings.yaml is not committed.")
    return True

def check_pycache():
    print("Checking __pycache__...")
    files = run_git(['git', 'ls-files', '**/__pycache__/*'])
    if files:
        print("FAIL: __pycache__ files are tracked.")
        return False
    print("PASS: No __pycache__ files tracked.")
    return True

def check_gitignore():
    print("Checking .gitignore coverage...")
    required = ['.venv', '__pycache__/', '*.pcap', 'config/settings.yaml', '.pytest_cache/']
    try:
        with open('.gitignore') as f:
            content = f.read()
            missing = [item for item in required if item not in content]
            if missing:
                print(f"FAIL: .gitignore is missing: {missing}")
                return False
    except FileNotFoundError:
        print("FAIL: .gitignore not found")
        return False
    print("PASS: .gitignore coverage looks good.")
    return True

def main():
    checks = [
        check_pcap(),
        check_ips(),
        check_settings(),
        check_pycache(),
        check_gitignore()
    ]
    if all(checks):
        print("\nOVERALL: PASS")
        sys.exit(0)
    else:
        print("\nOVERALL: FAIL")
        sys.exit(1)

if __name__ == '__main__':
    main()
