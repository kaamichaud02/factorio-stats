#!/usr/bin/env python3
"""Test rapide de connexion RCON — aucune dépendance externe."""
import socket
import struct
import sys

HOST = "127.0.0.1"  # <-- IP de ton serveur Factorio
PORT = 27015
PASSWORD = "TON_MOT_DE_PASSE_RCON"  # <-- remplace ici


def _send_packet(sock, request_id, packet_type, body):
    payload = struct.pack("<ii", request_id, packet_type) + body.encode("utf-8") + b"\x00\x00"
    sock.sendall(struct.pack("<i", len(payload)) + payload)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise RuntimeError("Connexion fermée de manière inattendue")
        buf += chunk
    return buf


def _read_packet(sock):
    (length,) = struct.unpack("<i", _recv_exact(sock, 4))
    data = _recv_exact(sock, length)
    request_id, packet_type = struct.unpack("<ii", data[:8])
    body = data[8:-2].decode("utf-8", errors="replace")
    return request_id, packet_type, body


with socket.create_connection((HOST, PORT), timeout=5) as sock:
    _send_packet(sock, 1, 3, PASSWORD)  # SERVERDATA_AUTH
    req_id, _, _ = _read_packet(sock)
    if req_id == -1:
        print("ECHEC: mot de passe RCON refusé")
        sys.exit(1)

    _send_packet(sock, 2, 2, '/silent-command rcon.print("RCON OK")')

    # Factorio peut renvoyer la sortie de rcon.print() dans un paquet séparé
    # du paquet de réponse immédiat (souvent vide). On lit tout ce qui arrive
    # pendant une courte fenêtre de temps.
    sock.settimeout(1.5)
    collected = []
    try:
        while True:
            _, _, body = _read_packet(sock)
            if body:
                collected.append(body)
    except (socket.timeout, RuntimeError):
        pass

    if collected:
        print("Reponse du serveur:", " | ".join(collected))
    else:
        print("Aucune reponse recue (auth OK mais commande sans sortie captee)")
