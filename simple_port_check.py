#!/usr/bin/env python3
"""
Simple port connectivity checker for IBKR
"""

import socket


def check_port(host, port):
    """Check if a specific port is open."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False


def main():
    host = "127.0.0.1"
    ports = [7497, 7496, 4002, 4001]

    print("Checking IBKR ports...")
    for port in ports:
        status = "OPEN" if check_port(host, port) else "CLOSED"
        print(f"Port {port}: {status}")


if __name__ == "__main__":
    main()
