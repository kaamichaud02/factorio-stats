#!/usr/bin/env python3
"""Debug — affiche la réponse brute (non parsée) de la requête de stats."""
import socket
import struct
import sys

HOST = "127.0.0.1"  # <-- IP de ton serveur Factorio
PORT = 27015
PASSWORD = "TON_MOT_DE_PASSE_RCON"  # <-- remplace ici

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
    "for name,_ in pairs(stats.output_counts) do "
    "local ok3,v=pcall(function() return stats.get_flow_count{name=name,input=false,precision_index=defines.flow_precision_index.five_seconds,count=false} end) "
    "if ok3 and v then electricity_produced=electricity_produced+v end end "
    "for name,_ in pairs(stats.input_counts) do "
    "local ok4,v=pcall(function() return stats.get_flow_count{name=name,input=true,precision_index=defines.flow_precision_index.five_seconds,count=false} end) "
    "if ok4 and v then electricity_consumed=electricity_consumed+v end end "
    "end end end end "
    "local production={} "
    "local stats=force.get_item_production_statistics(game.surfaces[1].name) "
    "for name,count in pairs(stats.output_counts) do "
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


def _send_packet(sock, request_id, packet_type, body):
    payload = struct.pack("<ii", request_id, packet_type) + body.encode("utf-8") + b"\x00\x00"
    sock.sendall(struct.pack("<i", len(payload)) + payload)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise RuntimeError("Connexion fermée")
        buf += chunk
    return buf


def _read_packet(sock):
    (length,) = struct.unpack("<i", _recv_exact(sock, 4))
    data = _recv_exact(sock, length)
    request_id, packet_type = struct.unpack("<ii", data[:8])
    body = data[8:-2].decode("utf-8", errors="replace")
    return request_id, packet_type, body


with socket.create_connection((HOST, PORT), timeout=5) as sock:
    _send_packet(sock, 1, 3, PASSWORD)
    req_id, ptype, body = _read_packet(sock)
    print(f"[AUTH] id={req_id} type={ptype} body={body!r}")
    if req_id == -1:
        print("ECHEC AUTH")
        sys.exit(1)

    _send_packet(sock, 2, 2, f"/silent-command {LUA_QUERY}")
    sock.settimeout(2)
    i = 0
    try:
        while True:
            req_id, ptype, body = _read_packet(sock)
            i += 1
            print(f"[PACKET {i}] id={req_id} type={ptype} body={body!r}")
    except (socket.timeout, RuntimeError):
        print("--- fin des paquets reçus ---")
