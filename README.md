# Système de stockage Clé-Valeur distribué tolérant aux pannes (inspiré du protocole Raft)

Auteur : Benedict Lubembela

## 1. Présentation et contexte du Projet

Ce projet consiste en la conception et l'implémentation d'un système de stockage clé-valeur distribué, hautement disponible et tolérant aux pannes. Le système simule les principes fondamentaux des architectures distribuées modernes (type _Dynamo_) et des protocoles de consensus réparti (type _Raft_).

Il assure l'élection autonome d'un nœud leader, la réplication synchrone des mutations de données entre les différents nœuds du cluster, et la validation sécurisée des écritures/suppressions à l'aide de la règle mathématique du quorum.

---

## 2. Piliers Architecturaux et Fonctionnels

Le système repose sur cinq piliers fondamentaux :

1. **Moteur Clé-Valeur et API REST :** Stockage en mémoire protégé contre les conditions de course (_race conditions_) par des verrous de threads, exposé via des endpoints REST performants.
2. **Réplication et Heartbeats :** Propagation synchrone des données vers les nœuds suiveurs (_Followers_) et surveillance de la santé du cluster par battements de cœur réguliers.
3. **Consensus et Élection de Leader :** Implémentation d'une machine à états finis (_Follower_, _Candidate_, _Leader_) gérant les termes de consensus et des délais aléatoires pour éviter les conflits de votes (_split vote_).
4. **Tolérance aux Pannes et Quorum :** Application de la règle mathématique $\text{Quorum} = \lfloor N/2 \rfloor + 1$ pour valider toute modification et garantir la consistance forte.
5. **Observabilité et Validation :** Interface graphique de suivi en temps réel et suite de tests automatisés pour valider le comportement du cluster.

---

## 3. Environnement Technique et Dépendances

- **Langage :** Python (Version 3.10 ou supérieure)
- **Framework Web :** FastAPI & Uvicorn (ASGI)
- **Communication Inter-nœuds :** Httpx (client HTTP asynchrone)
- **Validation des Données :** Pydantic
- **Interface d'Observabilité :** Streamlit
- **Concurrence :** Module natif `threading`

---

## 4. Structure du Projet et Rôle des Fichiers

- **`server.py` :** Cœur du système distribué. Il implémente la logique d'un nœud (stockage, API REST, réplication, gestion des pannes et consensus Raft).
- **`start_cluster.py` :** Script d'orchestration permettant de lancer simultanément les trois instances du cluster sur des ports distincts (`8001`, `8002`, `8003`).
- **`dashboard.py` :** Tableau de bord interactif sous Streamlit pour visualiser en temps réel l'état, le rôle et le contenu du stockage de chaque nœud.
- **`client_test.py` :** Script de test automatisé simulant un client pour valider la découverte dynamique du leader, les opérations CRUD et le respect des contraintes d'architecture.

---

## 5. Guide d'Installation

### Étape 1 : cloner ou accéder au répertoire du projet

cd distributed-key-value-store

### Étape 2 : Créer et activer un environnement virtuel

Sous Windows (CMD / PowerShell) :
DOS
python -m venv venv
venv\Scripts\activate.bat

Sous Linux / macOS :
python3 -m venv venv
source venv/bin/activate

### Étape 3 : Installer les dépendances requises

pip install fastapi uvicorn httpx pydantic streamlit

## 6. Guide d'Exécution et Démonstration

Pour faire tourner et tester le système, vous aurez besoin de **trois terminaux distincts** (avec l'environnement virtuel activé dans chacun d'eux).

### Étape 1 : Lancement du Cluster (Terminal 1)

Exécutez le script d'orchestration pour démarrer les trois nœuds :

```bash
python start_cluster.py

```

### Étape 2 : Lancement du Tableau de Bord d'Observabilité (Terminal 2)

Lancez l'application Streamlit pour superviser le cluster :

```bash
streamlit run dashboard.py

```

Ouvrez l'URL locale indiquée dans votre terminal (généralement `http://localhost:8501`).

### Étape 3 : Exécution des Tests Automatisés (Terminal 3)

Lancez le script simulant les requêtes d'un client :

```bash
python client_test.py

```
