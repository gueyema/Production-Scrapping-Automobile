# Production Scraping Automobile

![CookBook Pro Logo](link_to_your_logo.png)

PSA (Production Scraping Automobile) est un outil de web scraping avancé conçu pour extraire les tarifs d'assurance automobile à partir du site assurland.com. En utilisant une base de profils représentatifs du marché français, PSA récupère des données tarifaires précises et à jour, offrant ainsi une vue d'ensemble complète du marché de l'assurance auto.

## 🌟 Caractéristiques principales

- 🤖 Automatisation avancée avec Playwright pour un scraping fiable et efficace
- 📊 Base de données de profils représentatifs du marché français
- 🏙️ Intégration des données communales pour une précision géographique
- 🚙 Prise en compte des véhicules neufs et d'occasion (données SRA)
- 📈 Traitement des données via Jupyter Notebooks pour une analyse approfondie
- 🔄 Gestion robuste des erreurs et reconnexions automatiques

## 📚 Table des matières

- [Structure du projet](#-structure-du-projet)
- [Prérequis](#-prérequis)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Utilisation](#-utilisation)
- [Traitement des données](#-traitement-des-données)
- [Résultats et analyse](#-résultats-et-analyse)
- [Contribution](#-contribution)
- [Considérations éthiques](#-considérations-éthiques)
- [Licence](#-licence)
- [Contact et support](#-contact-et-support)

## 📁 Structure du projet
```
assurland-auto-scraper/
│
├── src/
│   └── script_ws_prod.py
│
├── notebooks/
│   ├── data_processing.ipynb
│   └── df_communes.csv
|   └── df_sra_occasion.csv 
|   └── df_sra_neuve.csv
│
├── results/
│   └── offres_date_du_jour_start_line_au_end_line.csv
│
├── requirements.txt
├── README.md
└── LICENSE

```
## 🛠 Prérequis

- Python 3.8+
- pip (gestionnaire de paquets Python)
- Navigateur compatible avec Playwright (Chrome, Firefox, ou Edge)
- PyCharm comme IDE pour une gestion des versions avec Git et GitLab
  
# 🚗 Script de Génération de Profils d'Assurance Automobile

## 🌟 Présentation
Bienvenue dans le script de génération de profils d'assurance automobile ! Ce puissant outil Python est conçu pour créer des jeux de données synthétiques réalistes, représentant des utilisateurs, leurs véhicules, et leurs situations d'assurance. Idéal pour les tests, les simulations, et l'analyse de données, ce script vous permet de plonger dans le monde de l'assurance automobile avec des données précises et variées.

## 🔑 Fonctionnalités Clés

### 1. **Génération de Profils Personnels**
- **Données Démographiques** : Âge, sexe, statut marital, et situation professionnelle.
- **Historique de Conduite** : Date d'obtention du permis et expérience de conduite.

### 2. **Simulation de Véhicules**
- **Véhicules Existants et Neufs** : Caractéristiques techniques détaillées (marque, modèle, type de carburant).
- **Informations SRA** : Données sur la sécurité et la réparation automobile.

### 3. **Données d'Assurance**
- **Historique d'Assurance** : Statut d'assurance et besoins futurs.
- **Coefficients de Bonus-Malus** : Calculs basés sur l'historique de conduite.

### 4. **Informations Géographiques**
- **Codes Postaux et INSEE** : Répartition régionale pondérée pour des données plus réalistes.

### 5. **Génération de Données Complémentaires**
- **Utilisation du Véhicule** : Kilométrage moyen et fréquence d'utilisation.
- **Type de Logement et Stationnement** : Informations sur le lieu de résidence.

## 🚀 Comment Utiliser le Script

### Étape 1 : Préparation des Données
Assurez-vous d'avoir les fichiers CSV suivants :
- `df_sra_final.csv` : Données des véhicules.
- `df_sra_neuve.csv` : Données des véhicules neufs.
- `df_communes.csv` : Données des communes françaises.

### Étape 2 : Configuration
- **Ajustez les chemins** des fichiers CSV dans le script si nécessaire.
- **Modifiez le nombre de profils** à générer (par défaut : 100).

### Étape 3 : Exécution
Lancez le script avec la commande suivante :
