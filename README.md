# Factorio Stats Dashboard

Poller RCON + page web statique affichant les stats en direct de ton serveur Factorio
(joueurs connectés, recherche en cours, évolution des biters, top production).

## 1. Activer RCON sur le serveur Factorio

Si ce n'est pas déjà fait, ajoute ces options au lancement du serveur (dans le fichier
de service systemd, sur la ligne `ExecStart`) :

```
--rcon-port 27015 --rcon-password "UN_MOT_DE_PASSE_SOLIDE"
```

Puis recharge et redémarre le service :

```bash
sudo systemctl daemon-reload
sudo systemctl restart factorio
```

Le RCON écoute par défaut sur `127.0.0.1` — comme le dashboard tourne sur la même
VM (réseau Docker `host`), pas besoin de l'exposer publiquement.

## 2. Copier les fichiers sur la VM

Le stack utilise des **bind mounts absolus** (pas de build Docker), donc les fichiers
doivent être présents sur la VM à un chemin fixe : `/opt/factorio-stats/`.

Copie tout le dossier depuis ta machine :

```bash
scp -r factorio-stats/ root@<IP_VM>:/opt/
```

Ou clone/pull ton repo Git si tu l'y as poussé. Structure attendue sur la VM :

```
/opt/factorio-stats/
├── poller/poller.py
├── web/index.html
└── data/            (créé automatiquement, laisse vide)
```

## 3. Déployer dans Portainer

1. **Stacks** → **Add stack**
2. Nom : `factorio-stats`
3. Build method : **Web editor**
4. Colle le contenu de `docker-compose.yml`
5. Descends jusqu'à **Environment variables** et ajoute :
   - `RCON_PORT` = `27015`
   - `RCON_PASSWORD` = `<le mot de passe configuré dans le service Factorio>`
6. **Deploy the stack**

Aucun build n'est nécessaire — les deux services utilisent des images standard
(`python:3.12-alpine`, `nginx:alpine`) et montent tes fichiers directement en
lecture (`poller.py`, `index.html`) via bind mount depuis `/opt/factorio-stats`.

Si tu modifies `poller.py` ou `index.html` plus tard, pas besoin de rebuild :
édite le fichier sur la VM, puis redémarre juste le conteneur concerné depuis
Portainer (bouton *Restart* sur `factorio-stats-poller` ou `factorio-stats-web`).

## 4. Vérifier

Dans Portainer, ouvre les logs du conteneur `factorio-stats-poller`. Tu devrais voir :
```
[poller] Démarrage — RCON 127.0.0.1:27015, intervalle 15s
[poller] OK — 2 joueur(s) en ligne, tick 481920
```

Le dashboard est accessible sur `http://<IP_VM>:8090`.

## 4. Publier via Cloudflare Tunnel (optionnel)

Dans la config de ton tunnel `cloudflared` (comme pour n8n.kaa.zone ou pgadmin.kaa.zone),
ajoute une règle d'ingress :

```yaml
- hostname: factorio-stats.kaa.zone
  service: http://localhost:8090
```

Puis crée l'enregistrement DNS correspondant côté Cloudflare et, si tu veux le
protéger, une policy Zero Trust (Entra ID SSO) comme pour tes autres services.

## 5. Chat en jeu (optionnel)

Le dashboard peut aussi afficher les derniers messages de chat/join/leave.
Comme le fichier de log du serveur Factorio n'est pas toujours accessible
directement depuis le serveur qui héberge ce dashboard, ça passe par un
PostgreSQL existant comme relais :

1. **Sur le serveur Factorio** (là où tourne le jeu), active le flag
   `--console-log /chemin/vers/console.log` sur la ligne `ExecStart` du
   service systemd, puis `sudo systemctl daemon-reload && sudo systemctl
   restart factorio`.
2. Toujours sur ce serveur, déploie le stack `log-shipper/` (voir son
   propre `docker-compose.yml`) : ajuste le chemin du bind mount vers ton
   vrai fichier `console.log`, et configure les variables `POSTGRES_*`
   dans son `.env` pour pointer vers ta base PostgreSQL existante.
3. **Sur le serveur Docker/Arcane** (où tourne ce dashboard), configure les
   mêmes variables `POSTGRES_*` dans le `.env` principal — le poller lira
   alors la table `chat_messages` à chaque cycle.

Si `POSTGRES_HOST` n'est pas configuré, le dashboard fonctionne
normalement, simplement sans le panneau de chat.

## Notes

- Le poller interroge le serveur toutes les 15 secondes (modifiable via
  `POLL_INTERVAL` dans `docker-compose.yml`).
- Les stats de production reflètent la production cumulée depuis le début de
  la partie sur `game.surfaces[1]` (généralement Nauvis). Si tu veux inclure
  d'autres planètes (Space Age), je peux étendre le script pour agréger
  plusieurs surfaces.
- Aucune dépendance Python externe : le client RCON est implémenté "from
  scratch" avec `socket` (protocole Source RCON, compatible Factorio).
