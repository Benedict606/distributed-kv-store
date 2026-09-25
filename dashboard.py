"""
Tableau de Bord d'Observabilité
Description : Interface web basée sur Streamlit pour surveiller en temps réel 
les états opérationnels des nœuds, les métriques d'élection de leader, la réplication 
des données et l'intégrité du quorum.
"""

import streamlit as st
import httpx
import time

st.set_page_config(page_title="Tableau de Bord du Cluster KV Distribué", layout="wide")

st.title("Tableau de bord du cluster de stockage Clé-Valeur distribué")
st.markdown("Interface de surveillance en temps réel pour les nœuds du cluster, les états de consensus et la réplication des données.")

NODE_PORTS = [8001, 8002, 8003]

refresh_rate = st.sidebar.slider("Fréquence de rafraîchissement du tableau de bord (secondes)", 1, 10, 2)

st.sidebar.markdown("---")
st.sidebar.subheader("Interface d'Écriture Client")

action_key = st.sidebar.text_input("Clé", "nom")
action_value = st.sidebar.text_input("Valeur", "Benedict")

if st.sidebar.button("Exécuter la Requête PUT (via le Leader)"):
    leader_port = None
    for port in NODE_PORTS:
        try:
            res = httpx.get(f"http://127.0.0.1:{port}/cluster/status", timeout=1.0)
            if res.status_code == 200:
                data = res.json()
                if data.get("state") == "Leader":
                    leader_port = port
                    break
        except Exception:
            pass
    
    if leader_port:
        try:
            res = httpx.put(f"http://127.0.0.1:{leader_port}/keys/{action_key}", json={"value": action_value}, timeout=2.0)
            if res.status_code == 200:
                st.sidebar.success(f"Opération d'écriture validée avec succès via le leader sur le port {leader_port}.")
            else:
                st.sidebar.error(f"Échec de l'écriture avec le code d'état {res.status_code} : {res.text}")
        except Exception as e:
            st.sidebar.error(f"Échec de connexion au nœud leader {leader_port} : {e}")
    else:
        st.sidebar.warning("Aucun leader actif identifié. Le quorum est peut-être perdu ou une élection est en cours.")

st.subheader("États des Nœuds du Cluster")

cols = st.columns(len(NODE_PORTS))
current_leader = None

for i, port in enumerate(NODE_PORTS):
    node_url = f"http://127.0.0.1:{port}"
    status_info = {"port": port, "alive": False, "state": "Inconnu", "term": "-", "keys": {}}
    
    try:
        res_status = httpx.get(f"{node_url}/cluster/status", timeout=0.8)
        res_keys = httpx.get(f"{node_url}/keys", timeout=0.8)
        
        if res_status.status_code == 200:
            status_data = res_status.json()
            status_info["alive"] = True
            status_info["state"] = status_data.get("state", "Inconnu")
            status_info["term"] = status_data.get("term", 0)
            if status_info["state"] == "Leader":
                current_leader = port
                
        if res_keys.status_code == 200:
            status_info["keys"] = res_keys.json()
    except Exception:
        status_info["alive"] = False

    with cols[i]:
        st.markdown(f"### Nœud : Port {port}")
        if status_info["alive"]:
            st.success("Statut : EN LIGNE")
            st.markdown(f"**Rôle :** {status_info['state']}")
            st.markdown(f"**Terme Raft :** `{status_info['term']}`")
            
            st.markdown("**Aperçu du Stockage Local :**")
            if status_info["keys"]:
                st.json(status_info["keys"])
            else:
                st.info("Stockage vide")
        else:
            st.error("Statut : HORS LIGNE / INACCESSIBLE")

st.markdown("---")
st.subheader("Résumé du Cluster")
if current_leader:
    st.info(f"Leader actif du cluster identifié sur le port **{current_leader}**. Toutes les requêtes d'écriture des clients doivent être acheminées par ce nœud.")
else:
    st.warning("Avertissement : Aucun leader actif de cluster détecté. Condition de quorum non satisfaite ou phase d'élection active.")

time.sleep(refresh_rate)
st.rerun()