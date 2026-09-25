"""
Implémentation d'un Nœud de Stockage Clé-Valeur Distribué
Description : Implémente un stockage clé-valeur distribué avec un stockage sécurisé 
pour la concurrence, une réplication asynchrone, une détection des pannes par battements 
de cœur, un consensus Raft simplifié et une cohérence d'écriture basée sur le quorum.
"""

import argparse
import random
import threading
import time
from fastapi import FastAPI, Header, HTTPException
import httpx
from pydantic import BaseModel

app = FastAPI(
    title="Nœud de Stockage Clé-Valeur Distribué", 
    description="Instance de nœud participant au cluster de consensus de type Raft."
)

# Structures d'état partagées protégées par des verrous de synchronisation
store: dict[str, str] = {}
store_lock = threading.Lock()

peers: list[int] = []
current_port: int = 8000

# Variables d'état du consensus Raft
state_lock = threading.Lock()
current_term: int = 0
voted_for: int | None = None
node_state: str = "Follower"  # Options : "Follower", "Candidate", "Leader"
current_leader: int | None = None
last_heartbeat_time: float = time.time()

class ValueModel(BaseModel):
    value: str

class VoteRequest(BaseModel):
    term: int
    candidate_id: int

class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: int

@app.get("/health", summary="Point de Contrôle de l'État de Santé")
def health_check():
    """Renvoie le statut opérationnel actuel et les métadonnées Raft du nœud."""
    with state_lock:
        return {
            "status": "sain",
            "state": node_state,
            "term": current_term
        }

@app.post("/raft/request-vote", summary="Protocole de Demande de Vote Raft")
def request_vote(data: VoteRequest):
    """
    Traite les demandes de vote entrantes des nœuds candidats lors de l'élection du leader.
    Met en œuvre la comparaison des termes et la contrainte d'un vote unique par terme.
    """
    global current_term, voted_for, node_state
    with state_lock:
        if data.term > current_term:
            current_term = data.term
            node_state = "Follower"
            voted_for = None

        if data.term == current_term and (voted_for is None or voted_for == data.candidate_id):
            voted_for = data.candidate_id
            return {"vote_granted": True, "term": current_term}
        
        return {"vote_granted": False, "term": current_term}

@app.post("/raft/append-entries", summary="Protocole d'Ajout d'Entrées / Battement de Cœur Raft")
def append_entries(data: AppendEntriesRequest):
    """
    Reçoit les battements de cœur ou les entrées de journal du leader élu afin de maintenir l'autorité de leadership.
    """
    global current_term, node_state, current_leader, last_heartbeat_time
    with state_lock:
        if data.term >= current_term:
            current_term = data.term
            node_state = "Follower"
            current_leader = data.leader_id
            last_heartbeat_time = time.time()
            return {"success": True}
        return {"success": False}

def run_raft_consensus():
    """
    Thread démon en arrière-plan gérant les délais d'attente d'élection, les transitions de candidats,
    le vote de quorum et la diffusion des battements de cœur du leader.
    """
    global node_state, current_term, voted_for, current_leader, last_heartbeat_time
    
    while True:
        time.sleep(1)
        with state_lock:
            ns = node_state
            cp = current_port
            pt = peers
            lht = last_heartbeat_time

        if ns == "Leader":
            # Le leader diffuse les battements de cœur à tous les pairs actifs
            for peer in pt:
                try:
                    httpx.post(
                        f"http://127.0.0.1:{peer}/raft/append-entries", 
                        json={"term": current_term, "leader_id": cp}, 
                        timeout=0.5
                    )
                except Exception:
                    pass
            time.sleep(2)
        else:
            # Suiveur (Follower) ou Candidat surveillant le délai d'élection
            timeout = random.uniform(5.0, 8.0)
            if time.time() - lht > timeout:
                with state_lock:
                    node_state = "Candidate"
                    current_term += 1
                    voted_for = cp
                    last_heartbeat_time = time.time()
                    term_to_ask = current_term
                
                print(f"[Consensus Raft] Délai d'élection expiré sur le port {cp}. Passage à l'état CANDIDAT pour le terme {term_to_ask}.")
                
                votes = 1  # Vote pour soi-même
                total_nodes = len(pt) + 1
                
                for peer in pt:
                    try:
                        res = httpx.post(
                            f"http://127.0.0.1:{peer}/raft/request-vote",
                            json={"term": term_to_ask, "candidate_id": cp}, 
                            timeout=1.0
                        )
                        data = res.json()
                        if data.get("vote_granted"):
                            votes += 1
                    except Exception:
                        pass
                
                with state_lock:
                    if node_state == "Candidate" and votes > total_nodes // 2:
                        node_state = "Leader"
                        current_leader = cp
                        print(f"[Consensus Raft] Le nœud sur le port {cp} a atteint le consensus majoritaire et est devenu LEADER.")
                    else:
                        node_state = "Follower"

@app.on_event("startup")
def startup_event():
    """Initialise le thread de consensus en arrière-plan au démarrage du serveur."""
    threading.Thread(target=run_raft_consensus, daemon=True).start()

@app.get("/cluster/status", summary="État des Nœuds du Cluster")
def get_cluster_status():
    """Expose les métriques du nœud local, son état, son terme et l'identification du leader."""
    with state_lock:
        return {
            "node_port": current_port,
            "state": node_state,
            "term": current_term,
            "leader": current_leader
        }

@app.get("/keys/{key}", summary="Lecture d'une Clé")
def get_key(key: str):
    """Récupère une valeur associée à la clé spécifiée depuis le stockage local."""
    with store_lock:
        if key not in store:
            raise HTTPException(status_code=404, detail="Clé introuvable")
        return {"key": key, "value": store[key]}

@app.put("/keys/{key}", summary="Écriture d'une Clé (Quorum Appliqué)")
def set_key(key: str, body: ValueModel, x_replicated: str | None = Header(default=None)):
    """
    Gère l'insertion ou la mise à jour d'une clé. Les écritures directes des clients sont restreintes au nœud leader.
    Applique la validation du quorum parmi les pairs du cluster avant de confirmer le succès.
    """
    with state_lock:
        if node_state != "Leader" and not x_replicated:
            raise HTTPException(
                status_code=400, 
                detail=f"Requête d'écriture rejetée. Le nœud actuel n'est pas le leader. Le port du leader est {current_leader}."
            )

    # Requête de réplication interne provenant du leader
    if x_replicated:
        with store_lock:
            store[key] = body.value
        return {"status": "succès", "key": key, "value": body.value}

    # Le leader traite l'écriture du client avec application du Quorum
    with store_lock:
        store[key] = body.value

    total_nodes = len(peers) + 1
    quorum = (total_nodes // 2) + 1
    successful_acks = 1  # Inclut l'écriture locale du leader

    for peer in peers:
        try:
            res = httpx.put(
                f"http://127.0.0.1:{peer}/keys/{key}", 
                json={"value": body.value}, 
                headers={"X-Replicated": "true"}, 
                timeout=1.0
            )
            if res.status_code == 200:
                successful_acks += 1
        except Exception:
            pass

    if successful_acks >= quorum:
        return {
            "status": "succès", 
            "key": key, 
            "value": body.value, 
            "quorum_reached": True, 
            "acks": f"{successful_acks}/{total_nodes}"
        }
    else:
        raise HTTPException(
            status_code=503, 
            detail=f"Échec de l'écriture par quorum. Reçu {successful_acks}/{total_nodes} accusés de réception (requis : {quorum})."
        )

@app.delete("/keys/{key}", summary="Suppression d'une Clé (Quorum Appliqué)")
def delete_key(key: str, x_replicated: str | None = Header(default=None)):
    """
    Supprime une clé du moteur de stockage. Applique la validation du leader et les contrôles de quorum.
    """
    with state_lock:
        if node_state != "Leader" and not x_replicated:
            raise HTTPException(
                status_code=400, 
                detail=f"Requête de suppression rejetée. Le nœud actuel n'est pas le leader. Le port du leader est {current_leader}."
            )

    if x_replicated:
        with store_lock:
            if key in store:
                del store[key]
        return {"status": "supprimé"}

    with store_lock:
        if key not in store:
            raise HTTPException(status_code=404, detail="Clé introuvable")
        del store[key]

    total_nodes = len(peers) + 1
    quorum = (total_nodes // 2) + 1
    successful_acks = 1

    for peer in peers:
        try:
            res = httpx.delete(
                f"http://127.0.0.1:{peer}/keys/{key}", 
                headers={"X-Replicated": "true"}, 
                timeout=1.0
            )
            if res.status_code == 200:
                successful_acks += 1
        except Exception:
            pass

    if successful_acks >= quorum:
        return {
            "status": "supprimé", 
            "key": key, 
            "quorum_reached": True, 
            "acks": f"{successful_acks}/{total_nodes}"
        }
    else:
        raise HTTPException(
            status_code=503, 
            detail=f"Échec de la suppression par quorum. Reçu {successful_acks}/{total_nodes} accusés de réception."
        )

@app.get("/keys", summary="Lister Toutes les Clés")
def list_keys():
    """Renvoie l'ensemble du dictionnaire de stockage clé-valeur actuellement maintenu en mémoire."""
    with store_lock:
        return store

if __name__ == "__main__":
    import uvicorn
    parser = argparse.ArgumentParser(description="Exécuter le serveur de nœud clé-valeur distribué.")
    parser.add_argument("--port", type=int, default=8000, help="Numéro de port pour l'écoute du nœud.")
    parser.add_argument("--peers", type=int, nargs="*", default=[], help="Liste des ports des nœuds pairs dans le cluster.")
    args = parser.parse_args()
    
    current_port = args.port
    peers = args.peers
    uvicorn.run(app, host="127.0.0.1", port=args.port)