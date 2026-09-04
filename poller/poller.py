#!/usr/bin/env python3
"""
Poller RCON pour serveur Factorio.
Interroge le serveur toutes les POLL_INTERVAL secondes et écrit
un fichier JSON avec les stats (joueurs, recherche, évolution, production).
"""

import json
import os
import socket
import struct
import time
import sys
from datetime import datetime, timezone

RCON_HOST = os.environ.get("FACTORIO_RCON_HOST", "127.0.0.1")
RCON_PORT = int(os.environ.get("FACTORIO_RCON_PORT", "27015"))
RCON_PASSWORD = os.environ.get("FACTORIO_RCON_PASSWORD", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "15"))
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/data/stats.json")

SERVERDATA_AUTH = 3
SERVERDATA_AUTH_RESPONSE = 2
SERVERDATA_EXECCOMMAND = 2
SERVERDATA_RESPONSE_VALUE = 0

# Commande Lua unique envoyée via /silent-command, packagée en JSON par
# helpers.table_to_json (Factorio 2.0+ / Space Age).
LUA_QUERY = (
    "local function build() "
    "local players={} "
    "for _,p in pairs(game.connected_players) do "
    "table.insert(players,{name=p.name,admin=p.admin,online_time=p.online_time}) end "
    "local force=game.forces.player "
    "local research=nil "
    "if force.current_research then "
    "research={name=force.current_research.name,progress=force.research_progress} end "
    "local techs_done=0 local techs_total=0 "
    "for _,t in pairs(force.technologies) do techs_total=techs_total+1 "
    "if t.researched then techs_done=techs_done+1 end end "
    "local evolution=0 "
    "if game.forces.enemy then "
    "local ok,result=pcall(function() return game.forces.enemy.get_evolution_factor(game.surfaces[1]) end) "
    "if ok and result then evolution=result end end "
    "local electricity_produced=0 local electricity_consumed=0 "
    "local seen_networks={} "
    "for _,surface in pairs(game.surfaces) do "
    "for _,pole in pairs(surface.find_entities_filtered{type='electric-pole'}) do "
    "local nid=pole.electric_network_id "
    "if nid and not seen_networks[nid] then "
    "seen_networks[nid]=true "
    "local ok2,stats=pcall(function() return pole.electric_network_statistics end) "
    "if ok2 and stats then "
    "for _,v in pairs(stats.output_counts) do electricity_produced=electricity_produced+v end "
    "for _,v in pairs(stats.input_counts) do electricity_consumed=electricity_consumed+v end "
    "end end end end "
    "local production={} "
    "local stats=force.get_item_production_statistics(game.surfaces[1].name) "
    # ATTENTION: pour item_production_statistics, l'API Factorio inverse
    # input/output par rapport à l'intuition : input_counts = items PRODUITS,
    # output_counts = items CONSOMMÉS. (Différent des stats du réseau
    # électrique ci-dessus, où output = produit comme on s'y attend.)
    "for name,count in pairs(stats.input_counts) do "
    "table.insert(production,{name=name,count=count}) end "
    "table.sort(production,function(a,b) return a.count>b.count end) "
    "local top={} "
    "for i=1,math.min(15,#production) do top[i]=production[i] end "
    "local data={tick=game.tick,players=players,online_count=#game.connected_players,"
    "research=research,techs_done=techs_done,techs_total=techs_total,"
    "evolution=evolution,electricity_produced=electricity_produced,"
    "electricity_consumed=electricity_consumed,top_production=top} "
    "rcon.print(helpers.table_to_json(data)) end build()"
)


class RconError(Exception):
    pass


def _send_packet(sock, request_id, packet_type, body):
    payload = struct.pack("<ii", request_id, packet_type) + body.encode("utf-8") + b"\x00\x00"
    sock.sendall(struct.pack("<i", len(payload)) + payload)


def _read_packet(sock):
    raw_len = _recv_exact(sock, 4)
    (length,) = struct.unpack("<i", raw_len)
    data = _recv_exact(sock, length)
    request_id, packet_type = struct.unpack("<ii", data[:8])
    body = data[8:-2].decode("utf-8", errors="replace")
    return request_id, packet_type, body


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise RconError("Connexion RCON fermée de manière inattendue")
        buf += chunk
    return buf


def rcon_command(command):
    with socket.create_connection((RCON_HOST, RCON_PORT), timeout=10) as sock:
        # Auth
        _send_packet(sock, 1, SERVERDATA_AUTH, RCON_PASSWORD)
        req_id, ptype, _ = _read_packet(sock)
        if req_id == -1:
            raise RconError("Authentification RCON refusée (mot de passe invalide ?)")

        # Commande — la sortie de rcon.print() peut arriver dans un paquet
        # séparé du paquet de réponse immédiat (souvent vide). On lit tout
        # ce qui arrive pendant une courte fenêtre de temps.
        _send_packet(sock, 2, SERVERDATA_EXECCOMMAND, command)
        sock.settimeout(3)
        collected = []
        try:
            while True:
                _, _, body = _read_packet(sock)
                if body:
                    collected.append(body)
        except (socket.timeout, RconError):
            pass

        if not collected:
            raise RconError("Aucune réponse reçue du serveur (commande sans sortie)")
        return "".join(collected)


# État du relevé précédent, utilisé pour calculer un taux instantané
# (Watts) à partir de deux relevés cumulés successifs.
_previous_reading = {"tick": None, "produced": None, "consumed": None}


def poll_once():
    body = rcon_command(f"/silent-command {LUA_QUERY}")
    data = json.loads(body)
    data["last_updated"] = datetime.now(timezone.utc).isoformat()

    # Factorio's helpers.table_to_json sérialise une table Lua vide comme un
    # objet JSON {} plutôt qu'un tableau [] (ambiguïté classique des tables
    # Lua). On force ici un tableau vide si aucune donnée de production
    # n'existe encore, pour que le JS côté dashboard puisse toujours faire
    # .forEach() sans erreur.
    if not isinstance(data.get("top_production"), list):
        data["top_production"] = []
    if not isinstance(data.get("players"), list):
        data["players"] = []

    cur_tick = data.get("tick")
    cur_produced = data.get("electricity_produced")
    cur_consumed = data.get("electricity_consumed")

    prev = _previous_reading
    if (
        prev["tick"] is not None
        and cur_tick is not None
        and cur_tick > prev["tick"]
    ):
        dt_seconds = (cur_tick - prev["tick"]) / 60.0  # 60 ticks/seconde
        if dt_seconds > 0:
            data["electricity_produced_watts"] = max(
                0, (cur_produced - prev["produced"]) / dt_seconds
            )
            data["electricity_consumed_watts"] = max(
                0, (cur_consumed - prev["consumed"]) / dt_seconds
            )
    else:
        data["electricity_produced_watts"] = None
        data["electricity_consumed_watts"] = None

    prev["tick"] = cur_tick
    prev["produced"] = cur_produced
    prev["consumed"] = cur_consumed

    return data


def main():
    print(f"[poller] Démarrage — RCON {RCON_HOST}:{RCON_PORT}, intervalle {POLL_INTERVAL}s", flush=True)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    while True:
        try:
            data = poll_once()
            tmp_path = OUTPUT_PATH + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(data, f)
            os.replace(tmp_path, OUTPUT_PATH)
            print(f"[poller] OK — {data['online_count']} joueur(s) en ligne, tick {data['tick']}", flush=True)
        except Exception as e:
            print(f"[poller] Erreur: {e}", file=sys.stderr, flush=True)
            # On écrit un statut d'erreur pour que le dashboard puisse l'afficher
            error_data = {
                "error": str(e),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            try:
                tmp_path = OUTPUT_PATH + ".tmp"
                with open(tmp_path, "w") as f:
                    json.dump(error_data, f)
                os.replace(tmp_path, OUTPUT_PATH)
            except Exception:
                pass

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
