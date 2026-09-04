#!/usr/bin/env python3
"""
Poller RCON pour serveur Factorio.
Interroge le serveur toutes les POLL_INTERVAL secondes et écrit
un fichier JSON avec les stats (joueurs, recherche, évolution, production).
S'abonne aussi en tâche de fond à un topic MQTT (alimenté par le service
log-shipper) pour inclure les derniers messages de chat/join/leave.
"""

import json
import os
import socket
import struct
import sys
import threading
import time
import urllib.request
import urllib.parse
from collections import deque
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

RCON_HOST = os.environ.get("FACTORIO_RCON_HOST", "127.0.0.1")
RCON_PORT = int(os.environ.get("FACTORIO_RCON_PORT", "27015"))
RCON_PASSWORD = os.environ.get("FACTORIO_RCON_PASSWORD", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "15"))
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/data/stats.json")

# MQTT optionnel — si MQTT_HOST n'est pas défini, le dashboard fonctionne
# normalement mais sans le panneau de chat (dégradation gracieuse, utile
# tant que log-shipper/mosquitto n'est pas déployé côté serveur Factorio).
MQTT_HOST = os.environ.get("MQTT_HOST", "")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "factorio/chat")
CHAT_LIMIT = int(os.environ.get("CHAT_LIMIT", "50"))
CHAT_HISTORY_PATH = os.environ.get("CHAT_HISTORY_PATH", "/data/chat_history.json")

# Historique décimé pour les graphiques du dashboard (joueurs, évolution
# biters, production). On n'enregistre un point que toutes les
# HISTORY_INTERVAL secondes, avec une taille maximale HISTORY_MAX_POINTS,
# pour garder un fichier léger tout en couvrant une longue période (par
# défaut : 1 point / 5 min, 288 points = 24h).
HISTORY_PATH = os.environ.get("HISTORY_PATH", "/data/history.json")
HISTORY_INTERVAL = int(os.environ.get("HISTORY_INTERVAL", "300"))
HISTORY_MAX_POINTS = int(os.environ.get("HISTORY_MAX_POINTS", "288"))

# Notifications Telegram (optionnel) — envoyées quand TELEGRAM_BOT_TOKEN et
# TELEGRAM_CHAT_ID sont configurés. Couvre : recherche terminée, fusée/
# satellite lancé (détectés via RCON) + tout événement "notable" reçu par
# MQTT (join/leave déjà présents, et mort de joueur/attaque de biters si le
# hook optionnel côté scénario est ajouté — voir scenario-hooks.lua).
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
NOTIFY_MQTT_TYPES = {"join", "leave", "death", "biter_attack"}

# Alertes proactives publiées sur MQTT (topic factorio/alerts/...) pour
# être consommées par une automatisation Home Assistant (notif mobile,
# etc.). Publiées sur le même broker que le chat. Le principe : la mort
# d'un joueur est retransmise ; l'électricité est surveillée avec un
# cooldown pour éviter le spam d'alertes répétées.
MQTT_ALERT_PREFIX = os.environ.get("MQTT_ALERT_PREFIX", "factorio/alerts")
ELECTRICITY_ALERT_MARGIN_PCT = float(os.environ.get("ELECTRICITY_ALERT_MARGIN_PCT", "100"))
ELECTRICITY_ALERT_COOLDOWN = int(os.environ.get("ELECTRICITY_ALERT_COOLDOWN", "300"))

# Lu depuis le fichier VERSION à la racine du projet (bind-mounté avec le
# reste du repo). Permet de confirmer visuellement, dans le dashboard, que
# le conteneur en cours d'exécution a bien récupéré le dernier code après
# un déploiement/sync — et pas une version mise en cache.
def _read_version():
    for candidate in ("/app/VERSION", "/VERSION"):
        try:
            with open(candidate) as f:
                return f.read().strip()
        except FileNotFoundError:
            continue
    return "inconnue"

POLLER_VERSION = _read_version()

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
    "local production_totals={} "
    # On agrège la production sur TOUTES les surfaces (pas seulement
    # surfaces[1]/Nauvis) — indispensable pour les scénarios custom ou les
    # parties Space Age où la production a lieu ailleurs.
    "for _,surface in pairs(game.surfaces) do "
    "local ok5,stats2=pcall(function() return force.get_item_production_statistics(surface.name) end) "
    "if ok5 and stats2 then "
    # ATTENTION: pour item_production_statistics, l'API Factorio inverse
    # input/output par rapport à l'intuition : input_counts = items PRODUITS,
    # output_counts = items CONSOMMÉS. (Différent des stats du réseau
    # électrique ci-dessus, où output = produit comme on s'y attend.)
    "for name,count in pairs(stats2.input_counts) do "
    "production_totals[name]=(production_totals[name] or 0)+count end "
    "end end "
    "local production={} "
    "for name,count in pairs(production_totals) do "
    "table.insert(production,{name=name,count=count}) end "
    "table.sort(production,function(a,b) return a.count>b.count end) "
    "local top={} "
    "for i=1,math.min(30,#production) do top[i]=production[i] end "
    "local ok6,rockets=pcall(function() return force.rockets_launched end) "
    "if not (ok6 and rockets) then rockets=0 end "
    "local data={tick=game.tick,players=players,online_count=#game.connected_players,"
    "rockets_launched=rockets,"
    "research=research,techs_done=techs_done,techs_total=techs_total,"
    "evolution=evolution,electricity_produced=electricity_produced,"
    "electricity_consumed=electricity_consumed,top_production=top} "
    "rcon.print(helpers.table_to_json(data)) end build()"
)


# --- Historique décimé (pour les graphiques) -------------------------
_history_lock = threading.Lock()
_history_points = deque(maxlen=HISTORY_MAX_POINTS)
_last_history_ts = 0


def _load_history():
    try:
        with open(HISTORY_PATH) as f:
            items = json.load(f)
        with _history_lock:
            _history_points.extend(items[-HISTORY_MAX_POINTS:])
    except (FileNotFoundError, json.JSONDecodeError):
        pass


def _save_history():
    try:
        tmp_path = HISTORY_PATH + ".tmp"
        with _history_lock:
            items = list(_history_points)
        with open(tmp_path, "w") as f:
            json.dump(items, f)
        os.replace(tmp_path, HISTORY_PATH)
    except Exception as e:
        print(f"[poller] Erreur sauvegarde historique : {e}", file=sys.stderr, flush=True)


def maybe_record_history(data):
    """Ajoute un point d'historique décimé (au plus 1 par HISTORY_INTERVAL
    secondes) à partir du relevé courant."""
    global _last_history_ts
    now = time.time()
    if now - _last_history_ts < HISTORY_INTERVAL:
        return
    _last_history_ts = now

    production_total = sum(
        item.get("count", 0) for item in data.get("top_production", [])
    )
    point = {
        "ts": data.get("last_updated"),
        "online_count": data.get("online_count", 0),
        "evolution": data.get("evolution"),
        "production_total": production_total,
    }
    with _history_lock:
        _history_points.append(point)
    _save_history()


def get_history():
    with _history_lock:
        return list(_history_points)


class RconError(Exception):
    pass


def send_telegram(text):
    """Envoie une notification Telegram si configuré. Best-effort : une
    erreur d'envoi ne doit jamais faire planter le poller."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload, method="POST")
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"[poller] Erreur envoi Telegram : {e}", file=sys.stderr, flush=True)


# --- Chat MQTT -------------------------------------------------------
# Un abonné MQTT tourne dans un thread séparé, en continu, et alimente un
# buffer borné (deque) protégé par un verrou. La boucle principale (poll
# RCON) lit simplement l'état courant du buffer à chaque cycle. Le buffer
# est aussi persisté sur disque pour survivre à un redémarrage du
# conteneur (sinon l'historique de chat repartirait de zéro à chaque
# redéploiement).
_chat_lock = threading.Lock()
_chat_buffer = deque(maxlen=CHAT_LIMIT)


def _load_chat_history():
    try:
        with open(CHAT_HISTORY_PATH) as f:
            items = json.load(f)
        with _chat_lock:
            _chat_buffer.extend(items[-CHAT_LIMIT:])
    except (FileNotFoundError, json.JSONDecodeError):
        pass


def _save_chat_history():
    try:
        tmp_path = CHAT_HISTORY_PATH + ".tmp"
        with _chat_lock:
            items = list(_chat_buffer)
        with open(tmp_path, "w") as f:
            json.dump(items, f)
        os.replace(tmp_path, CHAT_HISTORY_PATH)
    except Exception as e:
        print(f"[poller] Erreur sauvegarde historique chat : {e}", file=sys.stderr, flush=True)


# Référence au client MQTT connecté, pour pouvoir aussi PUBLIER des
# alertes (pas seulement s'abonner au chat) depuis la boucle principale.
_mqtt_client_ref = {"client": None}


def publish_mqtt_alert(topic_suffix, payload):
    client = _mqtt_client_ref.get("client")
    if client is None:
        return
    try:
        client.publish(f"{MQTT_ALERT_PREFIX}/{topic_suffix}", json.dumps(payload), qos=1)
    except Exception as e:
        print(f"[poller] Erreur publication alerte MQTT : {e}", file=sys.stderr, flush=True)


def _on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return
    with _chat_lock:
        _chat_buffer.append(payload)
    _save_chat_history()

    msg_type = payload.get("type")
    if msg_type in NOTIFY_MQTT_TYPES:
        icons = {"join": "🟢", "leave": "🔴", "death": "💀", "biter_attack": "⚠️"}
        icon = icons.get(msg_type, "ℹ️")
        send_telegram(f"{icon} {payload.get('message', '')}")

    # Forward vers Home Assistant (topic dédié aux alertes) — la mort d'un
    # joueur est le cas d'usage le plus courant pour déclencher une notif
    # mobile via une automatisation HA.
    if msg_type == "death":
        publish_mqtt_alert("death", payload)


def start_mqtt_subscriber():
    if mqtt is None or not MQTT_HOST:
        print("[poller] MQTT non configuré, panneau de chat désactivé", flush=True)
        return

    _load_chat_history()

    def _run():
        client = mqtt.Client()
        client.on_message = _on_mqtt_message

        def _on_connect(c, userdata, flags, rc):
            c.subscribe(MQTT_TOPIC, qos=1)
            _mqtt_client_ref["client"] = c
            print(f"[poller] Abonné à MQTT {MQTT_HOST}:{MQTT_PORT} topic={MQTT_TOPIC}", flush=True)

        client.on_connect = _on_connect

        while True:
            try:
                client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
                client.loop_forever()
            except Exception as e:
                print(f"[poller] Erreur connexion MQTT, nouvel essai dans 10s : {e}", file=sys.stderr, flush=True)
                time.sleep(10)

    t = threading.Thread(target=_run, daemon=True)
    t.start()


def get_chat_messages():
    """Retourne les messages actuellement en buffer, les plus récents en
    premier (pour affichage type fil d'actualité)."""
    with _chat_lock:
        items = list(_chat_buffer)
    return list(reversed(items))


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
_previous_reading = {
    "tick": None,
    "produced": None,
    "consumed": None,
    "techs_done": None,
    "research_name": None,
    "rockets_launched": None,
}
_last_electricity_alert_ts = 0
_electricity_alert_active = False


def poll_once():
    body = rcon_command(f"/silent-command {LUA_QUERY}")
    data = json.loads(body)
    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    data["poller_version"] = POLLER_VERSION

    # Factorio's helpers.table_to_json sérialise une table Lua vide comme un
    # objet JSON {} plutôt qu'un tableau [] (ambiguïté classique des tables
    # Lua). On force ici un tableau vide si aucune donnée de production
    # n'existe encore, pour que le JS côté dashboard puisse toujours faire
    # .forEach() sans erreur.
    if not isinstance(data.get("top_production"), list):
        data["top_production"] = []
    if not isinstance(data.get("players"), list):
        data["players"] = []

    data["chat_messages"] = get_chat_messages()
    maybe_record_history(data)
    data["history"] = get_history()

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

    # --- Détection d'événements (recherche terminée, fusée lancée) ---
    cur_techs_done = data.get("techs_done")
    if (
        prev["techs_done"] is not None
        and cur_techs_done is not None
        and cur_techs_done > prev["techs_done"]
    ):
        completed_name = prev["research_name"] or "une technologie"
        send_telegram(f"🔬 Recherche terminée : {completed_name}")

    cur_research_name = (data.get("research") or {}).get("name")
    prev["techs_done"] = cur_techs_done
    prev["research_name"] = cur_research_name

    cur_rockets = data.get("rockets_launched")
    if (
        prev["rockets_launched"] is not None
        and cur_rockets is not None
        and cur_rockets > prev["rockets_launched"]
    ):
        send_telegram(f"🚀 Fusée lancée ! (total : {cur_rockets})")
    prev["rockets_launched"] = cur_rockets

    # --- Alerte électricité (déficit de production) -------------------
    global _last_electricity_alert_ts, _electricity_alert_active
    produced_w = data.get("electricity_produced_watts")
    consumed_w = data.get("electricity_consumed_watts")
    if produced_w is not None and consumed_w is not None:
        deficit = consumed_w > produced_w * (ELECTRICITY_ALERT_MARGIN_PCT / 100.0)
        now = time.time()
        if deficit and now - _last_electricity_alert_ts > ELECTRICITY_ALERT_COOLDOWN:
            _last_electricity_alert_ts = now
            _electricity_alert_active = True
            publish_mqtt_alert("electricity", {
                "status": "deficit",
                "produced_watts": produced_w,
                "consumed_watts": consumed_w,
                "occurred_at": data["last_updated"],
            })
        elif not deficit and _electricity_alert_active:
            _electricity_alert_active = False
            publish_mqtt_alert("electricity", {
                "status": "ok",
                "produced_watts": produced_w,
                "consumed_watts": consumed_w,
                "occurred_at": data["last_updated"],
            })

    return data


def main():
    print(f"[poller] Démarrage — RCON {RCON_HOST}:{RCON_PORT}, intervalle {POLL_INTERVAL}s", flush=True)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    start_mqtt_subscriber()
    _load_history()

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
                "poller_version": POLLER_VERSION,
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
