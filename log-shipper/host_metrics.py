#!/usr/bin/env python3
"""
Host metrics — tourne SUR le serveur Factorio (comme log-shipper), lit les
vraies métriques système (CPU, RAM, charge, I/O disque) via /proc de
l'hôte (monté en lecture seule dans ce conteneur), et les publie sur MQTT.
Le poller principal (sur le serveur Docker/Arcane) s'y abonne pour les
afficher à côté de l'UPS dans l'onglet Performance du dashboard.

RCON ne peut PAS fournir ces métriques (l'API Lua de Factorio est en bac à
sable, sans accès au système d'exploitation) — d'où ce service séparé.
"""

import json
import os
import time

import paho.mqtt.client as mqtt

# /proc de l'HÔTE, monté en lecture seule dans ce conteneur (voir
# docker-compose.yml : "- /proc:/host/proc:ro"). Ne pas confondre avec le
# /proc du conteneur lui-même, qui ne reflète pas forcément les vraies
# ressources matérielles selon la configuration Docker.
PROC_PATH = os.environ.get("PROC_PATH", "/host/proc")

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("HOST_METRICS_TOPIC", "factorio/host-metrics")
INTERVAL = float(os.environ.get("INTERVAL", "5"))

# Préfixes de périphériques à exclure du calcul d'I/O disque (partitions,
# périphériques loop, device-mapper — on ne veut que les disques physiques
# pour éviter de compter deux fois les mêmes octets).
_EXCLUDED_DISK_PREFIXES = ("loop", "ram", "dm-")


def read_cpu_times():
    with open(f"{PROC_PATH}/stat") as f:
        line = f.readline()
    parts = line.split()
    # cpu user nice system idle iowait irq softirq steal guest guest_nice
    values = list(map(int, parts[1:11]))
    idle = values[3] + values[4]  # idle + iowait
    total = sum(values)
    return idle, total


def read_mem_percent():
    info = {}
    with open(f"{PROC_PATH}/meminfo") as f:
        for line in f:
            key, _, rest = line.partition(":")
            val = rest.strip().split()[0]
            info[key] = int(val)  # kB
    total = info.get("MemTotal", 0)
    avail = info.get("MemAvailable", 0)
    used = total - avail
    return {
        "mem_total_gb": round(total / 1024 / 1024, 2),
        "mem_used_gb": round(used / 1024 / 1024, 2),
        "mem_percent": round((used / total) * 100, 1) if total else None,
    }


def read_load_avg():
    with open(f"{PROC_PATH}/loadavg") as f:
        parts = f.read().split()
    return {
        "load1": float(parts[0]),
        "load5": float(parts[1]),
        "load15": float(parts[2]),
    }


def read_disk_sectors():
    """Somme des secteurs lus/écrits sur tous les disques physiques
    (secteurs de 512 octets, convention standard du noyau Linux)."""
    reads = 0
    writes = 0
    with open(f"{PROC_PATH}/diskstats") as f:
        for line in f:
            parts = line.split()
            name = parts[2]
            if any(name.startswith(p) for p in _EXCLUDED_DISK_PREFIXES):
                continue
            reads += int(parts[5])
            writes += int(parts[9])
    return reads, writes


def main():
    print(
        f"[host-metrics] Démarrage — MQTT {MQTT_HOST}:{MQTT_PORT} "
        f"topic={MQTT_TOPIC}, intervalle {INTERVAL}s",
        flush=True,
    )
    client = mqtt.Client()
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()

    prev_idle, prev_total = read_cpu_times()
    prev_reads, prev_writes = read_disk_sectors()
    prev_ts = time.time()

    while True:
        time.sleep(INTERVAL)
        try:
            idle, total = read_cpu_times()
            d_idle = idle - prev_idle
            d_total = total - prev_total
            cpu_percent = round((1 - d_idle / d_total) * 100, 1) if d_total > 0 else None
            prev_idle, prev_total = idle, total

            now = time.time()
            dt = now - prev_ts
            reads, writes = read_disk_sectors()
            d_reads = reads - prev_reads
            d_writes = writes - prev_writes
            disk_read_kbps = round((d_reads * 512 / 1024) / dt, 1) if dt > 0 else None
            disk_write_kbps = round((d_writes * 512 / 1024) / dt, 1) if dt > 0 else None
            prev_reads, prev_writes = reads, writes
            prev_ts = now

            payload = {
                "cpu_percent": cpu_percent,
                "disk_read_kbps": disk_read_kbps,
                "disk_write_kbps": disk_write_kbps,
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
            }
            payload.update(read_mem_percent())
            payload.update(read_load_avg())

            client.publish(MQTT_TOPIC, json.dumps(payload), qos=0)
        except Exception as e:
            print(f"[host-metrics] Erreur : {e}", flush=True)


if __name__ == "__main__":
    main()
