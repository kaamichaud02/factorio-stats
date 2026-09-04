#!/usr/bin/env python3
"""
Log shipper — tourne SUR le serveur Factorio (là où le fichier console.log
existe, via --console-log). Lit les nouvelles lignes du fichier au fil de
l'eau (façon tail -f), parse les messages de chat/join/leave, et les
insère dans une table PostgreSQL. Le poller principal (sur le serveur
Docker/Arcane) relit ensuite cette table pour les afficher sur le
dashboard, sans avoir besoin d'accéder directement au fichier distant.
"""

import os
import re
import sys
import time

import psycopg2

LOG_PATH = os.environ.get("LOG_PATH", "/factorio-logs/console.log")
OFFSET_PATH = os.environ.get("OFFSET_PATH", "/data/offset.txt")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "2"))

PG_HOST = os.environ.get("POSTGRES_HOST", "")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB = os.environ.get("POSTGRES_DB", "")
PG_USER = os.environ.get("POSTGRES_USER", "")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
PG_TABLE = os.environ.get("POSTGRES_TABLE", "chat_messages")

# Formats standards du fichier --console-log de Factorio. Le timestamp en
# début de ligne peut varier légèrement selon la version — si aucun message
# n'apparaît côté dashboard, vérifie le format réel avec `tail -f` et ajuste
# ces expressions régulières.
RE_CHAT = re.compile(r"\[CHAT\]\s+(?P<player>[^:]+):\s+(?P<message>.*)$")
RE_JOIN = re.compile(r"\[JOIN\]\s+(?P<player>.+?)\s+joined the game$")
RE_LEAVE = re.compile(r"\[LEAVE\]\s+(?P<player>.+?)\s+left the game$")


def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )


def ensure_table():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {PG_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    msg_type TEXT NOT NULL,
                    player TEXT,
                    message TEXT NOT NULL
                );
                """
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{PG_TABLE}_occurred_at "
                f"ON {PG_TABLE} (occurred_at DESC);"
            )
        conn.commit()


def parse_line(line):
    """Retourne (msg_type, player, message) ou None si la ligne n'est pas
    un événement de chat/join/leave."""
    m = RE_CHAT.search(line)
    if m:
        return "chat", m.group("player").strip(), m.group("message").strip()
    m = RE_JOIN.search(line)
    if m:
        player = m.group("player").strip()
        return "join", player, f"{player} a rejoint la partie"
    m = RE_LEAVE.search(line)
    if m:
        player = m.group("player").strip()
        return "leave", player, f"{player} a quitté la partie"
    return None


def read_offset():
    try:
        with open(OFFSET_PATH) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0


def write_offset(offset):
    tmp_path = OFFSET_PATH + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(str(offset))
    os.replace(tmp_path, OFFSET_PATH)


def insert_events(events):
    if not events:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {PG_TABLE} (msg_type, player, message) "
                f"VALUES (%s, %s, %s)",
                events,
            )
        conn.commit()


def tail_once(offset):
    """Lit les nouvelles lignes depuis `offset`. Gère la rotation du
    fichier (si le fichier a été tronqué/recréé, on repart de 0)."""
    try:
        size = os.path.getsize(LOG_PATH)
    except FileNotFoundError:
        print(f"[log-shipper] Fichier introuvable : {LOG_PATH}", file=sys.stderr, flush=True)
        return offset, []

    if size < offset:
        print("[log-shipper] Rotation du fichier détectée, reprise à 0", flush=True)
        offset = 0

    events = []
    with open(LOG_PATH, "r", errors="replace") as f:
        f.seek(offset)
        for line in f:
            parsed = parse_line(line)
            if parsed:
                events.append(parsed)
        offset = f.tell()

    return offset, events


def main():
    print(f"[log-shipper] Démarrage — fichier {LOG_PATH}, intervalle {POLL_INTERVAL}s", flush=True)
    os.makedirs(os.path.dirname(OFFSET_PATH), exist_ok=True)

    try:
        ensure_table()
    except Exception as e:
        print(f"[log-shipper] Erreur création table : {e}", file=sys.stderr, flush=True)

    offset = read_offset()

    while True:
        try:
            new_offset, events = tail_once(offset)
            if events:
                insert_events(events)
                print(f"[log-shipper] {len(events)} événement(s) inséré(s)", flush=True)
            if new_offset != offset:
                offset = new_offset
                write_offset(offset)
        except Exception as e:
            print(f"[log-shipper] Erreur : {e}", file=sys.stderr, flush=True)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
