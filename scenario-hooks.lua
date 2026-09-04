-- scenario-hooks.lua
--
-- Extraits OPTIONNELS à fusionner dans le control.lua de ton scénario
-- (spiral_troopers) pour activer les notifications Telegram de mort de
-- joueur et d'attaque de biters sur le dashboard.
--
-- IMPORTANT : Factorio n'autorise qu'UN SEUL handler par événement via
-- script.on_event. Si ton control.lua a déjà un handler sur
-- on_player_died ou on_entity_damaged, NE COPIE-COLLE PAS ce fichier tel
-- quel — fusionne plutôt le contenu des fonctions ci-dessous à l'intérieur
-- de tes handlers existants.
--
-- Ces lignes utilisent game.print() avec un préfixe [EVENT:xxx] que
-- log-shipper reconnaît automatiquement (via --console-log) et transmet
-- au dashboard/Telegram sans configuration supplémentaire de ton côté.

-- 1. Mort de joueur ----------------------------------------------------
script.on_event(defines.events.on_player_died, function(event)
  local player = game.players[event.player_index]
  if player then
    game.print("[EVENT:death] " .. player.name .. " est mort")
  end
end)

-- 2. Attaque de biters --------------------------------------------------
-- Alerte quand une structure du joueur subit des dégâts d'un ennemi, avec
-- un cooldown pour éviter le spam (une alerte max toutes les 2 minutes).
-- ATTENTION PERFORMANCE : on_entity_damaged se déclenche très souvent en
-- combat. Si tu observes un impact sur les performances du serveur avec
-- une grosse base, il est possible de restreindre cet event avec un
-- filtre (voir la documentation LuaBootstrap.on_event / EventFilter dans
-- l'API Factorio) plutôt que de tout traiter en Lua à chaque appel.
local last_biter_alert_tick = 0

script.on_event(defines.events.on_entity_damaged, function(event)
  if not (event.entity and event.entity.valid and event.entity.force) then
    return
  end
  if event.entity.force.name ~= "player" then
    return
  end
  if not (event.cause and event.cause.valid and event.cause.force and event.cause.force.name == "enemy") then
    return
  end

  local tick = game.tick
  if tick - last_biter_alert_tick > 120 * 60 then -- 2 minutes (60 ticks/s)
    last_biter_alert_tick = tick
    game.print("[EVENT:biter_attack] Attaque de biters détectée sur " .. event.entity.name)
  end
end)
