"""
Script d'Orchestration du Cluster
Description : Automatise l'initialisation et l'exécution simultanées de plusieurs 
instances de nœuds représentant un cluster de stockage clé-valeur distribué.
"""

import subprocess
import sys
import time

def run_cluster():
    """Lance les instances de serveurs sur des ports réseau prédéfinis."""
    print("Initialisation du cluster de stockage distribué (3 nœuds)...")
    
    # Lancement de trois nœuds du cluster avec leurs configurations de pairs correspondantes
    processes = [
        subprocess.Popen([sys.executable, "server.py", "--port", "8001", "--peers", "8002", "8003"]),
        subprocess.Popen([sys.executable, "server.py", "--port", "8002", "--peers", "8001", "8003"]),
        subprocess.Popen([sys.executable, "server.py", "--port", "8003", "--peers", "8001", "8002"]),
    ]
    
    print("Cluster déployé avec succès. Nœuds actifs sur les ports 8001, 8002 et 8003.")
    print("Appuyez sur Ctrl+C dans ce terminal pour terminer proprement tous les processus du cluster.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nArrêt en cours des nœuds du cluster...")
        for p in processes:
            p.terminate()
        print("Arrêt complet du cluster effectué.")

if __name__ == "__main__":
    run_cluster()