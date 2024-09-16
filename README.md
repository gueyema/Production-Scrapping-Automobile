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
