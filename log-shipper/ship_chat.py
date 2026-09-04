#!/usr/bin/env python3
"""
Log shipper — tourne SUR le serveur Factorio (là où le fichier console.log
existe, via --console-log). Lit les nouvelles lignes du fichier au fil de
l'eau (façon tail -f), parse les messages de chat/join/leave, et les publie
sur un topic MQTT. Le poller principal (sur le serveur Docker/Arcane)
s'abonne à ce topic pour les afficher sur le dashboard, sans avoir besoin
d'accéder directement au fichier distant.
"""

import json
import os
import re
import sys
import time

import paho.mqtt.client as mqtt

LOG_PATH = os.environ.get("LOG_PATH", "/factorio-logs/console.log")
OFFSET_PATH = os.environ.get("OFFSET_PATH", "/data/offset.txt")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "2"))

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "factorio/chat")

# Formats standards du fichier --console-log de Factorio. Le timestamp en
# début de ligne peut varier légèrement selon la version — si aucun message
# n'apparaît côté dashboard, vérifie le format réel avec `tail -f` et ajuste
# ces expressions régulières.
RE_CHAT = re.compile(r"\[CHAT\]\s+(?P<player>[^:]+):\s+(?P<message>.*)$")
RE_JOIN = re.compile(r"\[JOIN\]\s+(?P<player>.+?)\s+joined the game$")
RE_LEAVE = re.compile(r"\[LEAVE\]\s+(?P<player>.+?)\s+left the game$")


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
    print(f"[log-shipper] Démarrage — fichier {LOG_PATH}, MQTT {MQTT_HOST}:{MQTT_PORT} topic={MQTT_TOPIC}", flush=True)
    os.makedirs(os.path.dirname(OFFSET_PATH), exist_ok=True)

    client = mqtt.Client()
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()

    offset = read_offset()

    while True:
        try:
            new_offset, events = tail_once(offset)
            for msg_type, player, message in events:
                payload = json.dumps({
                    "type": msg_type,
                    "player": player,
                    "message": message,
                    "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
                })
                client.publish(MQTT_TOPIC, payload, qos=1)
            if events:
                print(f"[log-shipper] {len(events)} événement(s) publié(s)", flush=True)
            if new_offset != offset:
                offset = new_offset
                write_offset(offset)
        except Exception as e:
            print(f"[log-shipper] Erreur : {e}", file=sys.stderr, flush=True)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
