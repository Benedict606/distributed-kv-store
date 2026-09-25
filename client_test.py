"""
Script de Test Automatisé du Client
Description : Valide les fonctionnalités du système distribué en découvrant 
automatiquement le leader, en testant les opérations CRUD, et en vérifiant 
les contraintes d'architecture (rejet des écritures sur les suiveurs).
"""

import httpx
import time

NODE_PORTS = [8001, 8002, 8003]

def discover_leader():
    """Interroge les nœuds du cluster pour identifier l'instance active ayant le rôle de leader."""
    print("[Test Client] Recherche du nœud leader dans le cluster...")
    for port in NODE_PORTS:
        try:
            response = httpx.get(f"http://127.0.0.1:{port}/cluster/status", timeout=1.0)
            if response.status_code == 200:
                data = response.json()
                if data.get("state") == "Leader":
                    print(f"[Test Client] Leader identifié avec succès sur le port {port}.")
                    return port
        except Exception:
            pass
    print("[Test Client] Erreur : Aucun leader n'a pu être identifié dans le cluster.")
    return None

def run_tests():
    """Exécute une suite de tests séquentiels pour valider les piliers fonctionnels du projet."""
    print("=== DEBUT DES TESTS AUTOMATISES DU SYSTEME DISTRIBUE ===")
    
    # Pause pour laisser le temps au cluster de stabiliser l'élection de leader
    time.sleep(2)
    
    leader_port = discover_leader()
    if not leader_port:
        print("Veuillez vous assurer que le cluster est en cours d'exécution avant de lancer le test.")
        return

    # Identification d'un nœud suiveur (follower) distinct du leader
    follower_port = [p for p in NODE_PORTS if p != leader_port][0]

    test_key = "cle_academique"
    test_value = "systemes_distribues_valeur_reference"

    # Test 1 : Écriture directe sur un suiveur (Doit être rejetée avec le code HTTP 400)
    print(f"\n[Test 1] Tentative d'écriture sur un nœud suiveur (Port {follower_port})...")
    try:
        res = httpx.put(
            f"http://127.0.0.1:{follower_port}/keys/{test_key}",
            json={"value": test_value},
            timeout=2.0
        )
        if res.status_code == 400:
            print(f"[Succès du Test 1] La requête a été correctement rejetée par le suiveur. Détail : {res.json().get('detail')}")
        else:
            print(f"[Échec du Test 1] Comportement inattendu, code de statut reçu : {res.status_code}")
    except Exception as e:
        print(f"[Erreur Test 1] Exception réseau rencontrée : {e}")

    # Test 2 : Écriture sur le leader avec application du Quorum (Doit réussir)
    print(f"\n[Test 2] Écriture de la clé '{test_key}' sur le leader (Port {leader_port})...")
    try:
        res = httpx.put(
            f"http://127.0.0.1:{leader_port}/keys/{test_key}",
            json={"value": test_value},
            timeout=2.0
        )
        if res.status_code == 200:
            print(f"[Succès du Test 2] Écriture validée et répliquée par le quorum : {res.json()}")
        else:
            print(f"[Échec du Test 2] Échec de l'écriture, code : {res.status_code}, réponse : {res.text}")
    except Exception as e:
        print(f"[Erreur Test 2] Exception réseau rencontrée : {e}")

    # Test 3 : Lecture de la clé sur un nœud suiveur pour vérifier la réplication asynchrone
    print(f"\n[Test 3] Lecture de la clé '{test_key}' sur un nœud suiveur (Port {follower_port})...")
    try:
        res = httpx.get(f"http://127.0.0.1:{follower_port}/keys/{test_key}", timeout=2.0)
        if res.status_code == 200:
            print(f"[Succès du Test 3] Donnée répliquée lue avec succès sur le suiveur : {res.json()}")
        else:
            print(f"[Échec du Test 3] Clé non trouvée sur le suiveur, code : {res.status_code}")
    except Exception as e:
        print(f"[Erreur Test 3] Exception réseau rencontrée : {e}")

    # Test 4 : Suppression de la clé via le leader
    print(f"\n[Test 4] Suppression de la clé '{test_key}' via le leader (Port {leader_port})...")
    try:
        res = httpx.delete(f"http://127.0.0.1:{leader_port}/keys/{test_key}", timeout=2.0)
        if res.status_code == 200:
            print(f"[Succès du Test 4] Suppression validée et répliquée par le quorum : {res.json()}")
        else:
            print(f"[Échec du Test 4] Suppression échouée, code : {res.status_code}")
    except Exception as e:
        print(f"[Erreur Test 4] Exception réseau rencontrée : {e}")

    print("\n=== FIN DES TESTS AUTOMATISES ===")

if __name__ == "__main__":
    run_tests()