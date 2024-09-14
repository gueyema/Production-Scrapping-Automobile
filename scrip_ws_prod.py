
# flake8: noqa: E221, E241
# pylint: disable=C0301
# pylint: disable=C0303, C0114, W0613, R0914, RO915, C0411, W0012, R0915, R1705, C0103, W0621, W0611, C0305, C0412, C0404, W1203, W0404, W0718, R0912, R0913


import pandas as pd

import random
from datetime import datetime, timedelta
from faker import Faker


import csv
import logging
import asyncio
from asyncio import Semaphore
import datetime
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict
from tqdm import tqdm
import aiofiles
import json
from os import environ
from playwright.async_api import async_playwright, Playwright, expect, TimeoutError as PlaywrightTimeoutError


# Initialisation de Faker pour générer des données en France
fake = Faker('fr_FR')

# Liste de domaines d'email valides en France
email_domains = [
  "gmail.com", "yahoo.fr", "orange.fr", "hotmail.fr", "free.fr",
  "sfr.fr", "laposte.net", "outlook.fr", "wanadoo.fr", "bbox.fr"
]

# Poids pour les régions
poids_regions = {
  '11': 20, '24': 10, '27': 10, '28': 15, '32': 15, '44': 15,
  '52': 15, '53': 15, '75': 15, '76': 15, '84': 15, '93': 10,
  '94': 5, '01': 5, '02': 5, '03': 5, '04': 5, '06': 5
}

# Types de trajet et critères correspondants
types_de_trajet = {
  '1': {'carburants': {'EH', 'ELEC', 'GH', 'BIO', 'FH', 'H2'}, 'carrosseries': {'CAB', 'CPE', 'LIM', 'CLE', 'P2C', 'FLE', 'CLB', 'C2C'}},
  '2': {'carburants': {'DIES', 'CARB', 'EE'}, 'carrosseries': {'VTC', 'MSP', 'FTE', 'LSP', 'MMC', 'PHC', 'PKU'}},
  '3': {'carburants': {'DIES', 'CARB', 'EE'}, 'carrosseries': {'BRK', 'FGN', 'VTC', 'FTE', 'CBC', 'PKU', 'PHC'}},
  '4': {'carburants': {'DIES', 'CARB', 'GNV'}, 'carrosseries': {'FGN', 'BRK', 'VTC', 'PKU'}}
}

# Intervalles de kilométrage pour chaque type de trajet
kilometrages = {
  '1': ['1000', '2000', '5000', '7000', '10000', '12500'],
  '2': ['10000', '12500', '20000', '25000'],
  '3': ['20000', '25000', '30000'],
  '4': ['30000', '40000', '41000']
}

# Poids pour l'historique d'assurance
INSURANCE_HISTORY_WEIGHTS = {
  "P": 30,  # Comme, conducteur principal
  "S": 60,  # Comme, conducteur secondaire
  "N": 10   # Non
}

# Fonctions utilitaires
def charger_donnees_vehicules(chemin_fichier):
    """
    Charge les données des véhicules à partir d'un fichier CSV.

    Args:
        chemin_fichier (str): Chemin vers le fichier CSV contenant les données des véhicules.

    Returns:
        pandas.DataFrame: DataFrame contenant les données des véhicules avec les colonnes converties en chaînes de caractères.
    """
    df = pd.read_csv(chemin_fichier, encoding='ISO-8859-1')
    cols_to_convert = [
        'SpecCarPower', 'groupe_tarification_vehicule', 'puissance_reel_vehicule',
        'valeur_a_neuf_vehicule', 'code_type_frequence_rcm', 'code_type_frequence_rcc',
        'code_type_cout_rcm', 'code_type_frequence_dta', 'code_type_cout_dta',
        'code_type_frequence_vol', 'code_type_cout_vol', 'code_type_frequence_bdg',
        'code_type_cout_bdg', 'code_vente_vehicule', 'qualification_vehicule_vert',
        'date_debut_construction', 'date_fin_construction'
    ]
    df[cols_to_convert] = df[cols_to_convert].astype(str)
    return df

def charger_donnees_vehicules_neuve(chemin_fichier):
    """
    Charge les données des véhicules neufs à partir d'un fichier CSV.

    Args:
        chemin_fichier (str): Chemin vers le fichier CSV contenant les données des véhicules neufs.

    Returns:
        pandas.DataFrame: DataFrame contenant les données des véhicules neufs avec les colonnes converties en chaînes de caractères.
    """
    df = pd.read_csv(chemin_fichier, encoding='ISO-8859-1')
    cols_to_convert = [
        'SpecCarMakeNameNeuve', 'SpecCarTypeNeuve', 'SpecCarFuelTypeNeuve',
        'SpecCarBodyTypeNeuve', 'SpecCarPowerNeuve', 'ID_Veh_Neuve',
        'groupe_tarification_vehicule_Neuve', 'date_debut_construction_Neuve',
        'date_fin_construction_Neuve', 'classe_tarification_vehicule_Neuve',
        'puissance_reel_vehicule_Neuve', 'valeur_a_neuf_vehicule_Neuve',
        'categorie_commerciale_vehicule_Neuve', 'qualification_vehicule_vert_Neuve',
        'code_type_frequence_rcm_Neuve', 'code_type_frequence_rcc_Neuve',
        'code_type_cout_rcm_Neuve', 'code_type_frequence_dta_Neuve',
        'code_type_cout_dta_Neuve', 'code_type_frequence_vol_Neuve',
        'code_type_cout_vol_Neuve', 'code_type_frequence_bdg_Neuve',
        'code_type_cout_bdg_Neuve', 'code_vente_vehicule_Neuve'
    ]
    df[cols_to_convert] = df[cols_to_convert].astype(str)
    return df

def charger_donnees_communes(chemin_fichier):
    """
    Charge les données des communes à partir d'un fichier CSV.

    Args:
        chemin_fichier (str): Chemin vers le fichier CSV contenant les données des communes.

    Returns:
        pandas.DataFrame: DataFrame contenant les données des communes avec les colonnes converties en chaînes de caractères et les poids ajoutés.
    """
    df = pd.read_csv(chemin_fichier, encoding='UTF-8')
    cols_to_convert = [
        'HomeParkZipCode', 'HomeParkInseeCode', 'nom_commune_postal', 'latitude', 'longitude',
        'code_departement', 'nom_departement', 'code_region', 'JobParkZipCode', 'JobParkInseeCode'
    ]
    df[cols_to_convert] = df[cols_to_convert].astype(str)
    if 'code_region' not in df.columns:
        raise ValueError("La colonne 'code_region' n'existe pas dans le fichier CSV des communes")
    df['poids'] = df['code_region'].map(poids_regions).fillna(1)
    return df


def calculer_age(date_naissance):
    """
    Calcule l'âge d'une personne à partir de sa date de naissance.

    Args:
        date_naissance (datetime): Date de naissance de la personne.

    Returns:
        int: Âge de la personne.
    """
    aujourd_hui = datetime.now()
    age = aujourd_hui.year - date_naissance.year
    if aujourd_hui.month < date_naissance.month or (
            aujourd_hui.month == date_naissance.month and aujourd_hui.day < date_naissance.day):
        age -= 1
    return age


def choix_pondere(options, poids):
    """
    Sélectionne un élément d'une liste en fonction de poids donnés.

    Args:
        options (list): Liste des options à choisir.
        poids (list): Liste des poids correspondants aux options.

    Returns:
        Any: L'élément choisi de la liste des options.
    """
    return random.choices(options, poids)[0]

def generer_purchase_date(first_car_driving_date):
    """
    Génère une date d'achat aléatoire entre la date de mise en circulation et la date actuelle.

    Args:
        first_car_driving_date (datetime): Date de mise en circulation du véhicule.

    Returns:
        str: Date d'achat au format 'mm/YYYY'.

    Raises:
        TypeError: Si first_car_driving_date n'est pas un objet datetime.
        ValueError: Si first_car_driving_date est dans le futur.
    """
    if not isinstance(first_car_driving_date, datetime):
        raise TypeError("first_car_driving_date doit être un objet datetime")

    max_allowed_date = datetime.now()  # Date actuelle de l'exécution du script

    if first_car_driving_date > max_allowed_date:
        raise ValueError("La date de mise en circulation ne peut pas être dans le futur")

    # Calcul du nombre de jours entre la date de mise en circulation et la date actuelle
    max_days = (max_allowed_date - first_car_driving_date).days

    if max_days == 0:
        # Si la date de mise en circulation est aujourd'hui, on retourne cette date
        return first_car_driving_date.strftime('%m/%Y')

    # Génération d'un nombre aléatoire de jours entre 0 et max_days
    jours_apres = random.randint(0, max_days)
    purchase_date = first_car_driving_date + timedelta(days=jours_apres)
    return purchase_date.strftime('%m/%Y')



def determine_car_usage_code(row):
    """
    Détermine le code d'utilisation du véhicule en fonction des types de carburant et de carrosserie.

    Args:
        row (dict): Dictionnaire contenant les informations du véhicule.

    Returns:
        str: Code d'utilisation du véhicule.
    """
    fuel_types = {row.get('SpecCarFuelType'), row.get('SpecCarFuelTypeNeuve')}
    body_types = {row.get('SpecCarBodyType'), row.get('SpecCarBodyTypeNeuve')}
    for code, criteria in types_de_trajet.items():
        if fuel_types & criteria['carburants'] and body_types & criteria['carrosseries']:
            return code
    return '1'



# Fonction pour choisir aléatoirement un kilométrage en fonction du type de trajet
def generer_avg_km_number(car_usage_code):
    """
    Génère un kilométrage moyen en fonction du code d'utilisation du véhicule.

    Args:
        car_usage_code (str): Code d'utilisation du véhicule.

    Returns:
        str: Kilométrage moyen généré.
    """
    return random.choice(kilometrages.get(car_usage_code, ['1000']))



# Fonction pour attribuer FreqCarUse en fonction du kilométrage
def generer_freq_car_use(avg_km_number):
    """
    Attribue FreqCarUse en fonction du kilométrage.

    Args:
        avg_km_number (str): Kilométrage moyen.

    Returns:
        str: Code de fréquence d'utilisation du véhicule.
    """
    km_to_freq = {'1000': '3', '2000': '2', '5000': '2', '7000': '1'}
    return km_to_freq.get(avg_km_number, None)


# Générer une date d'achat prévue
def generer_purchase_date_prevue():
    """
    Génère une date d'achat prévue aléatoire entre aujourd'hui et le 31/12/2025.

    Returns:
        str: Date d'achat prévue au format 'dd/mm/YYYY'.
    """
    aujourd_hui = datetime.now()
    date_limite = datetime(2025, 12, 31)
    delta = date_limite - aujourd_hui
    jours_aleatoires = random.randint(0, delta.days)
    purchase_date_prevue = aujourd_hui + timedelta(days=jours_aleatoires)
    return purchase_date_prevue.strftime('%d/%m/%Y')



def generer_home_info():
    """
    Génère des informations sur le type de logement et le type de résident.

    Returns:
        tuple: (home_type, home_resident_type)
    """
    home_type = random.choices(["1", "2"], weights=[55, 45])[0]
    if home_type == "1":
        home_resident_type = random.choices(["1", "2"], weights=[30, 70])[0]
    else:
        home_resident_type = random.choices(["1", "2"], weights=[70, 30])[0]
    return home_type, home_resident_type

def generer_parking_code(home_type, home_resident_type):
    """
    Génère un code de stationnement en fonction du type de logement et du type de résident.

    Args:
        home_type (str): Type de logement.
        home_resident_type (str): Type de résident.

    Returns:
        str: Code de stationnement.
    """
    if home_type == "2" and home_resident_type == "2":
        return random.choices(["G", "C", "P", "J", "V"], weights=[40, 5, 5, 40, 10])[0]
    elif home_type == "1" and home_resident_type == "1":
        return random.choices(["G", "C", "P", "J", "V"], weights=[5, 30, 30, 5, 30])[0]
    elif home_type == "1" and home_resident_type == "2":
        return random.choices(["G", "C", "P", "J", "V"], weights=[30, 40, 20, 5, 5])[0]
    else:
        return random.choices(["G", "C", "P", "J", "V"], weights=[10, 30, 30, 5, 25])[0]

def generer_statut_assurance():
    """
    Génère un statut d'assurance basé sur des poids prédéfinis.

    Returns:
        str: Statut d'assurance généré.
    """
    return random.choices(list(INSURANCE_HISTORY_WEIGHTS.keys()), 
                          weights=list(INSURANCE_HISTORY_WEIGHTS.values()))[0]


def convertir_date_permis(date_str):
    """
    Convertit une date de permis de conduire au format 'mm/yyyy' en objet datetime.

    Args:
        date_str (str): Date du permis au format 'mm/yyyy'.

    Returns:
        datetime: Date du permis convertie en objet datetime, ou None si la conversion échoue.
    """
    if date_str and date_str != 'None':
        return datetime.strptime(date_str, '%m/%Y').replace(day=1)
    return None


def calculer_annees_assurance(date_permis_str):
    """
    Calcule le nombre d'années pleines d'assurance à partir de la date d'obtention du permis.

    Args:
        date_permis_str (str): Date d'obtention du permis au format 'mm/yyyy'.

    Returns:
        str: Nombre d'années d'assurance.
    """
    if date_permis_str and date_permis_str != 'None':
        date_permis = datetime.strptime(date_permis_str, '%m/%Y')
        aujourd_hui = datetime.now()
        difference = relativedelta(aujourd_hui, date_permis)
        return str(difference.years)
    return "0"



def generer_profil(vehicules_df, communes_df):
    """
    Génère un profil complet en combinant des données personnelles, de véhicule et d'assurance.

    Args:
        vehicules_df (pandas.DataFrame): DataFrame contenant les données des véhicules.
        communes_df (pandas.DataFrame): DataFrame contenant les données des communes.

    Returns:
        dict: Dictionnaire contenant toutes les informations du profil généré.
    """
    aujourd_hui = datetime.now()

    # Définir les pourcentages pour le sexe
    sexe = choix_pondere(["Un homme", "Une femme"], [50, 50])

    # Définir les pourcentages pour le statut marital
    statut_marital = choix_pondere(
        ["Célibataire", "Marié(e)", "Concubin(e) / vie maritale", "Pacsé(e)", "Veuf(ve)", "Séparé(e)", "Divorcé(e)"],
        [40, 30, 10, 5, 5, 5, 5]
    )

    # Définir les pourcentages pour l'occupation
    occupation = choix_pondere(
        ["Salarié Cadre", "Salarié", "Commerçant", "Fonctionnaire territorial", "Fonctionnaire hospitalier",
         "Fonctionnaire Défense/Intérieur", "Fonctionnaire autre", "Enseignant", "Agriculteur", "Artisan",
         "Chef d'entreprise", "Profession libérale", "VRP", "Etudiant", "Retraité", "Sans profession",
         "Recherche d'emploi", "Prof. du spectacle", "Forain", "Taxi/VTC"],
        [10, 20, 5, 5, 5, 5, 5, 5, 2, 2, 2, 5, 2, 5, 10, 5, 2, 2, 2, 2]
    )

    # Définir les tranches d'âge et leur distribution
    tranches_age = [(18, 25), (26, 35), (36, 50), (51, 65), (66, 80), (81, 100)]
    distribution_age = [10, 20, 30, 20, 15, 5]
    tranche_age = choix_pondere(tranches_age, distribution_age)
    age_min, age_max = tranche_age
    date_naissance = aujourd_hui - timedelta(days=random.randint(age_min * 365, age_max * 365))

    age = calculer_age(date_naissance)

    # Calcul de la date d'obtention du permis de conduire
    age_minimum_permis = 18
    date_minimum_permis = date_naissance + timedelta(days=age_minimum_permis * 365)
    if date_minimum_permis > aujourd_hui:
        date_permis = None
    else:
        jours_depuis_permis_possible = (aujourd_hui - date_minimum_permis).days
        date_permis = date_minimum_permis + timedelta(days=random.randint(0, jours_depuis_permis_possible))

    experience_pre_permis = random.choice(["Oui", "Non", "Non", "Non"])
    suspension_permis = "Non, jamais"

    conjoint_date_naissance = None
    conjoint_permis = None
    conjoint_date_permis = None

    if statut_marital in ["Marié(e)", "Concubin(e) / vie maritale", "Pacsé(e)"]:
        age_conjoint_min, age_conjoint_max = 14, 100
        conjoint_date_naissance = aujourd_hui - timedelta(days=random.randint(age_conjoint_min * 365, age_conjoint_max * 365))
        conjoint_permis = random.choice(["Oui", "Non"])

        if conjoint_permis == "Oui":
            age_minimum_permis_conjoint = 18
            date_minimum_permis_conjoint = conjoint_date_naissance + timedelta(days=age_minimum_permis_conjoint * 365)
            if date_minimum_permis_conjoint <= aujourd_hui:
                jours_depuis_permis_possible_conjoint = (aujourd_hui - date_minimum_permis_conjoint).days
                conjoint_date_permis = date_minimum_permis_conjoint + timedelta(days=random.randint(0, jours_depuis_permis_possible_conjoint))

    has_child = None
    nombre_enfants = 0
    enfant_annee_naissance_1 = "Non"
    enfant_annee_naissance_2 = "Non"
    enfant_annee_naissance_3 = "Non"

    if age > 35:
        has_child = random.choice(["Oui", "Non"])
        if has_child == "Oui":
            nombre_enfants = random.choice([1, 2, 3])
            annees_possible = [str(year) for year in range(1998, 2025)]  # Conversion en str

            if nombre_enfants >= 1:
                enfant_annee_naissance_1 = random.choice(annees_possible)
            if nombre_enfants >= 2:
                enfant_annee_naissance_2 = random.choice(annees_possible)
            if nombre_enfants == 3:
                enfant_annee_naissance_3 = random.choice(annees_possible)
    else:
        has_child = "Non"

    insurance_need = choix_pondere(["Vous le possédez déjà", "Vous comptez l'acheter"], [70, 30])
    insurance_need_detail = "N/A"
    add_car_age = "N/A"

    if insurance_need == "Vous comptez l'acheter":
        insurance_need_detail = random.choice(
            ["D'une voiture en remplacement", "D'une voiture supplémentaire", "D'une première voiture"])
        add_car_age = random.choice(["Neuve", "D'occasion"])


    other_driver = "Non"
    grey_card_owner = "Vous"

    # Ajouter la colonne PurchaseMode
    modes_achat = ["T", "C", "L", "D"]
    poids_modes = [30, 30, 20, 20]  # Ajuster les poids en fonction des besoins
    purchase_mode = choix_pondere(modes_achat, poids_modes)

    commune = communes_df.sample(weights='poids').iloc[0]

    # Générer les informations sur le logement
    home_type, home_resident_type = generer_home_info()

    parking_code = generer_parking_code(home_type, home_resident_type)

    insurance_status = generer_statut_assurance()

    # Générer des informations personnelles
    first_name = fake.first_name()  # Prénom
    last_name = fake.last_name()  # Nom de famille
    email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(email_domains)}" 
    date_scrap = datetime.now()

    



    return {
        'PrimaryApplicantSex': sexe,
        'PrimaryApplicantBirthDate': date_naissance.strftime('%d/%m/%Y'),
        'Age': age,
        'PrimaryApplicantMaritalStatus': statut_marital,
        'PrimaryApplicantOccupationCode': occupation,
        'PrimaryApplicantDrivLicenseDate': date_permis.strftime('%m/%Y') if date_permis else None,
        'PrimaryApplicantIsPreLicenseExper': experience_pre_permis,
        'PrimaryApplicantDrivLicenseSusp': suspension_permis,
        'ConjointNonSouscripteurBirthDate': conjoint_date_naissance.strftime('%d/%m/%Y') if conjoint_date_naissance else None,
        'ConjointNonSouscripteurHasDriveLicense': conjoint_permis if conjoint_permis else None,
        'ConjointNonSouscripteurDriveLicenseDate': conjoint_date_permis.strftime('%m/%Y') if conjoint_date_permis else None,
        'HasChild': has_child,
        'NombreEnfants': nombre_enfants if nombre_enfants else None,
        'ChildBirthDateYear1': enfant_annee_naissance_1 if nombre_enfants and nombre_enfants >= 1 else "non",
        'ChildBirthDateYear2': enfant_annee_naissance_2 if nombre_enfants and nombre_enfants >= 2 else "non",
        'ChildBirthDateYear3': enfant_annee_naissance_3 if nombre_enfants and nombre_enfants == 3 else "non",
        'InsuranceNeed': insurance_need,
        'InsuranceNeedDetail': insurance_need_detail,
        'AddCarAge': add_car_age,
        'OtherDriver': other_driver,
        'GreyCardOwner': grey_card_owner,
        'PurchaseMode': purchase_mode,
        'HomeParkZipCode': commune['HomeParkZipCode'],
        'HomeParkInseeCode': commune['HomeParkInseeCode'],
        'code_departement': commune['code_departement'],
        'nom_departement': commune['nom_departement'],
        'code_region': commune['code_region'],
        'JobParkZipCode': commune['JobParkZipCode'],
        'JobParkInseeCode': commune['JobParkInseeCode'],
        'HomeType': home_type,
        'HomeResidentType': home_resident_type,
        'ParkingCode': parking_code,
        'PrimaryApplicantHasBeenInsured': insurance_status,
        'Id': str(random.randint(5, 10)),  # Id aléatoire entre 5 et 10 (as a string)
        'TitleAddress': random.choice(["MONSIEUR", "MADAME"]), 
        'LastName': fake.last_name(),  # Nom de famille
        'FirstName': fake.first_name(),  # Prénom
        'Address': f"{fake.building_number()} {fake.street_name()}",  # Adresse sans ville ni code postal
        'Email': email,  # Email généré
        'Phone': f"0{random.randint(6, 7)}{random.randint(10000000, 99999999)}",  # Numéro de téléphone valide en France
        'DateScraping': date_scrap.strftime('%d/%m/%Y')
        
    }

# Charger les données de véhicules
chemin_fichier_vehicules = 'notebook/df_sra_final.csv'  # Remplacez par le chemin de votre fichier CSV
vehicules_df = charger_donnees_vehicules(chemin_fichier_vehicules)

#Charger les données de véhicules neufs
chemin_fichier_vehicules_neuves = 'notebook/df_sra_neuve.csv'  # Remplacez par le chemin de votre fichier CSV
vehicules_neuves_df = charger_donnees_vehicules_neuve(chemin_fichier_vehicules_neuves)

# Charger les données des communes
chemin_fichier_communes = 'notebook/df_communes.csv'  # Remplacez par le chemin réel
communes_df = charger_donnees_communes(chemin_fichier_communes)

# Générer les profils avec les informations de véhicules
profils = []
for _ in range(10):  # Par exemple, générer 1000 profils
    profil = generer_profil(vehicules_df, communes_df)
    vehicule = vehicules_df.sample().iloc[0]
    profil.update({
        'ID_Veh': vehicule['ID_Veh'],
        'SpecCarMakeName': vehicule['SpecCarMakeName'],
        'SpecCarType': vehicule['SpecCarType'],
        'SpecCarFuelType': vehicule['SpecCarFuelType'],
        'SpecCarBodyType': vehicule['SpecCarBodyType'],
        'FirstCarDrivingDate': vehicule['FirstCarDrivingDate'],
        'SpecCarPower': vehicule['SpecCarPower'],
        'groupe_tarification_vehicule': vehicule['groupe_tarification_vehicule'],
        'date_debut_construction': vehicule['date_debut_construction'],
        'date_fin_construction': vehicule['date_fin_construction'],
        'classe_tarification_vehicule': vehicule['classe_tarification_vehicule'],
        'puissance_reel_vehicule': vehicule['puissance_reel_vehicule'],
        'valeur_a_neuf_vehicule': vehicule['valeur_a_neuf_vehicule'],
        'categorie_commerciale_vehicule': vehicule['categorie_commerciale_vehicule'],
        'qualification_vehicule_vert': vehicule['qualification_vehicule_vert'],
        'code_type_frequence_rcm': vehicule['code_type_frequence_rcm'],
        'code_type_frequence_rcc': vehicule['code_type_frequence_rcc'],
        'code_type_cout_rcm': vehicule['code_type_cout_rcm'],
        'code_type_frequence_dta': vehicule['code_type_frequence_dta'],
        'code_type_cout_dta': vehicule['code_type_cout_dta'],
        'code_type_frequence_vol': vehicule['code_type_frequence_vol'],
        'code_type_cout_vol': vehicule['code_type_cout_vol'],
        'code_type_frequence_bdg': vehicule['code_type_frequence_bdg'],
        'code_type_cout_bdg': vehicule['code_type_cout_bdg'],
        'code_vente_vehicule': vehicule['code_vente_vehicule'],
        
        
    })

    # Ajouter la colonne `PurchaseDate` si `InsuranceNeed` est "Vous le possédez déjà"
    if profil['InsuranceNeed'] == "Vous le possédez déjà":
        first_car_driving_date = datetime.strptime(vehicule['FirstCarDrivingDate'], '%m/%Y')
        profil['PurchaseDate'] = generer_purchase_date(first_car_driving_date)
    else:
        profil['PurchaseDate'] = 'N/A'

    # Ajouter la colonne `PurchaseDatePrev` si `InsuranceNeed` est "Vous comptez l'acheter" et `AddCarAge` est "D'occasion"
    if profil['InsuranceNeed'] == "Vous comptez l'acheter" and profil['AddCarAge'] == "D'occasion":
        profil['PurchaseDatePrev'] = generer_purchase_date_prevue()
    elif profil['InsuranceNeed'] == "Vous comptez l'acheter" and profil['AddCarAge'] == "Neuve":
        profil['PurchaseDatePrev'] = generer_purchase_date_prevue()
    else:
        profil['PurchaseDatePrev'] = None

    profil['PrimaryApplicantInsuranceYearNb'] = calculer_annees_assurance(profil['PrimaryApplicantDrivLicenseDate'])
    profils.append(profil)


# Répartition des lignes de véhicules neufs sur les profils
# Filtrer les profils pour lesquels `InsuranceNeed` est "Vous comptez l'acheter" et `AddCarAge` est "Neuve"
profils_neufs = [profil for profil in profils if profil['InsuranceNeed'] == "Vous comptez l'acheter" and profil['AddCarAge'] == "Neuve"]

# Assigner les lignes de `vehicules_neuves_df` aux profils filtrés
if len(profils_neufs) > 0 and len(vehicules_neuves_df) > 0:
    # Répartir les véhicules neufs entre les profils
    vehicules_neuves_df = vehicules_neuves_df.sample(n=len(profils_neufs), replace=True)  # Assurer que nous avons suffisamment de véhicules
    for i, profil in enumerate(profils_neufs):
        vehicule = vehicules_neuves_df.iloc[i]
        profil.update({
            'ID_Veh_Neuve': vehicule['ID_Veh_Neuve'],
            'SpecCarMakeNameNeuve': vehicule['SpecCarMakeNameNeuve'],
            'SpecCarTypeNeuve': vehicule['SpecCarTypeNeuve'],
            'SpecCarFuelTypeNeuve': vehicule['SpecCarFuelTypeNeuve'],
            'SpecCarBodyTypeNeuve': vehicule['SpecCarBodyTypeNeuve'],
            'SpecCarPowerNeuve': vehicule['SpecCarPowerNeuve'],
            'groupe_tarification_vehicule_Neuve': vehicule['groupe_tarification_vehicule_Neuve'],
            'date_debut_construction_Neuve': vehicule['date_debut_construction_Neuve'],
            'date_fin_construction_Neuve': vehicule['date_fin_construction_Neuve'],
            'classe_tarification_vehicule_Neuve': vehicule['classe_tarification_vehicule_Neuve'],
            'puissance_reel_vehicule_Neuve': vehicule['puissance_reel_vehicule_Neuve'],
            'valeur_a_neuf_vehicule_Neuve': vehicule['valeur_a_neuf_vehicule_Neuve'],
            'categorie_commerciale_vehicule_Neuve': vehicule['categorie_commerciale_vehicule_Neuve'],
            'qualification_vehicule_vert_Neuve': vehicule['qualification_vehicule_vert_Neuve'],
            'code_type_frequence_rcm_Neuve': vehicule['code_type_frequence_rcm_Neuve'],
            'code_type_frequence_rcc_Neuve': vehicule['code_type_frequence_rcc_Neuve'],
            'code_type_cout_rcm_Neuve': vehicule['code_type_cout_rcm_Neuve'],
            'code_type_frequence_dta_Neuve': vehicule['code_type_frequence_dta_Neuve'],
            'code_type_cout_dta_Neuve': vehicule['code_type_cout_dta_Neuve'],
            'code_type_frequence_vol_Neuve': vehicule['code_type_frequence_vol_Neuve'],
            'code_type_cout_vol_Neuve': vehicule['code_type_cout_vol_Neuve'],
            'code_type_frequence_bdg_Neuve': vehicule['code_type_frequence_bdg_Neuve'],
            'code_type_cout_bdg_Neuve': vehicule['code_type_cout_bdg_Neuve'],
            'code_vente_vehicule_Neuve': vehicule['code_vente_vehicule_Neuve']
        })

# Mettre à jour les profils non-neufs avec des valeurs None pour les colonnes liées aux véhicules neufs
for profil in profils:
    if profil['InsuranceNeed'] != "Vous comptez l'acheter" or profil['AddCarAge'] != "Neuve":
        profil.update({
            'ID_Veh_Neuve': None,
            'SpecCarMakeNameNeuve': None,
            'SpecCarTypeNeuve': None,
            'SpecCarFuelTypeNeuve': None,
            'SpecCarBodyTypeNeuve': None,
            'SpecCarPowerNeuve': None,
            'groupe_tarification_vehicule_Neuve': None,
            'date_debut_construction_Neuve': None,
            'date_fin_construction_Neuve': None,
            'classe_tarification_vehicule_Neuve': None,
            'puissance_reel_vehicule_Neuve': None,
            'valeur_a_neuf_vehicule_Neuve': None,
            'categorie_commerciale_vehicule_Neuve': None,
            'qualification_vehicule_vert_Neuve': None,
            'code_type_frequence_rcm_Neuve': None,
            'code_type_frequence_rcc_Neuve': None,
            'code_type_cout_rcm_Neuve': None,
            'code_type_frequence_dta_Neuve': None,
            'code_type_cout_dta_Neuve': None,
            'code_type_frequence_vol_Neuve': None,
            'code_type_cout_vol_Neuve': None,
            'code_type_frequence_bdg_Neuve': None,
            'code_type_cout_bdg_Neuve': None,
            'code_vente_vehicule_Neuve': None
        })

    profil['CarUsageCode'] = determine_car_usage_code(profil)
    profil['AvgKmNumber'] = generer_avg_km_number(profil['CarUsageCode'])
    profil['FreqCarUse'] = generer_freq_car_use(profil['AvgKmNumber'])
    profil['PrimaryApplicantIsFirstDrivOtherCar']= "Non"  # Nouvelle colonne avec valeur "Non"
    profil['PrimaryApplicantContrCancell']= "N"
    # Déterminer le coefficient de bonus
    bonus_coeff_mapping = {
        "0": "1",
        "1": "0.95",
        "2": "0.9",
        "3": "0.85",
        "4": "0.8",
        "5": "0.76",
        "6": "0.72",
        "7": "0.68",
        "8": "0.64",
        "9": "0.6",
        "10": "0.57",
        "11": "0.54",
        "12": "0.51",
        "13": "0.5",
        "14": "10",
        "15": "20",
        "16": "30",
        "17": "40",
        "18": "50",
        "19": "60",
        "20": "70",
        "21": "80",
        "22": "90",
        "23": "100",
        "24": "110",
        "25": "120"
    }

    # Si l'année d'assurance est supérieure à 25, le coefficient est "120"
    profil['PrimaryApplicantBonusCoeff'] = bonus_coeff_mapping.get(profil['PrimaryApplicantInsuranceYearNb'], "120")
    profil['PrimaryApplicantDisasterLast3year']= "0"
     # Calculer CarOwningTime
    actuel = datetime.now()
    # Calculer CarOwningTime
    if profil['InsuranceNeed'] == "Vous le possédez déjà":
        # Convertir PurchaseDate en objet datetime
        month, year = map(int, profil['PurchaseDate'].split('/'))
        purchase_date = datetime(year, month, 1)  # On prend le premier jour du mois pour simplifier le calcul

        difference = (actuel - purchase_date).days // 365  # Calculer la différence en années
        if difference < 1:
            profil['CarOwningTime'] = "0"
        elif difference == 1:
            profil['CarOwningTime'] = "1"
        elif difference == 2:
            profil['CarOwningTime'] = "2"
        elif difference == 3:
            profil['CarOwningTime'] = "3"
        elif difference == 4:
            profil['CarOwningTime'] = "4"
        elif difference >= 5:
            profil['CarOwningTime'] = "6"
    else:
        profil['CarOwningTime'] = "0"  # Valeur par défaut si l'assurance n'est pas "Vous le possédez déjà"

    # Créer la colonne CurrentGuaranteeCode avec des poids
    guarantee_codes = ["A", "E", "N"]
    weights = [0.5, 0.4, 0.1]  # Exemple de poids pour chaque code
    profil['CurrentGuaranteeCode'] = random.choices(guarantee_codes, weights=weights, k=1)[0]

    # Créer la colonne PrimaryApplicantContrNotRunningSince
    if profil['CurrentGuaranteeCode'] == "N":
        not_running_values = ["6", "7", "8", "1", "2", "3"]
        profil['PrimaryApplicantContrNotRunningSince'] = random.choice(not_running_values)
    else:
        profil['PrimaryApplicantContrNotRunningSince'] = None  # Ou une autre valeur par défaut si nécessaire

    # Créer la colonne CurrentCarrier
    current_carrier_values = ['71', '73', '1', '68', '74', '72', '75', '76', '77', '4', '33', '34', '78', '37', '79', '3', '8', '80', '81', '9', '13', '82', '44', '14', '83', '45', '89', '84', '90', '17', '18', '48', '19', '20', '22', '21', '85', '86', '49', '50', '70', '69', '56', '87', '26', '88', '0', '59', '60']
    profil['CurrentCarrier'] = random.choice(current_carrier_values)

    # Créer la colonne ContractAnniverMth
    contract_anniver_mth_values = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    profil['ContractAnniverMth'] = random.choice(contract_anniver_mth_values)

    # Créer la colonne EffectiveDate
    effective_date = actuel + timedelta(days=random.randint(1, 30))  # Date dans les 30 prochains jours
    profil['EffectiveDate'] = effective_date.strftime('%d/%m/%Y')  # Format jour/mois/année

    # Créer la colonne ContrGuaranteeCode avec des poids égaux
    contr_guarantee_codes = ["A", "C", "D", "E"]
    profil['ContrGuaranteeCode'] = random.choice(contr_guarantee_codes)  # Choix aléatoire parmi les valeurs

    # Créer la colonne UserOptIn
    profil['UserOptIn'] = random.choice(["0", "1"])  # Choix aléatoire entre "0" et "1"


# Convertir les profils en DataFrame et sauvegarder
profils_df = pd.DataFrame(profils)
# Afficher les 10 premiers profils

# Paramètres
start_line = 1  # Commencer à la première ligne
end_line = 10   # Finir à la dixième ligne

for profile in profils[:2]:

    print(profile)




# Statistiques supplémentaires
print("\nStatistiques supplémentaires:")
print(f"Nombre total de profils générés : {len(profils_df)}")
print("\nTop 5 des régions les plus représentées:")
print(profils_df['code_region'].value_counts().head())
print("\nÂge moyen des profils:", profils_df['Age'].mean())
print("\nDistribution des niveaux de garantie:")
print(profils_df['ContrGuaranteeCode'].value_counts(normalize=True))
print("\nTop 10 des professions les plus fréquentes:")
print(profils_df['PrimaryApplicantOccupationCode'].value_counts().head(10))

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

TIMEOUT = 2 * 60000
# SBR_WS_CDP = 'wss://brd-customer-hl_e9a5f52e-zone-scraping_browser1:jpuci55coo47@brd.superproxy.io:9222'

SBR_WS_CDP = 'wss://brd-customer-hl_538b14f9-zone-scraping_browser1:zk8riomp0pt9@brd.superproxy.io:9222'
TARGET_URL = environ.get('TARGET_URL', default='https://www.assurland.com/')

# Liste d'agents utilisateurs
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/113.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/127.0.2651.105",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/113.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; Xbox; Xbox One) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edge/44.18363.8131",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0"]

# Liste de langues
LANGUAGES = ["fr-FR", "en-GB", "de-DE", "es-ES"]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def display_profiles(profils: List[Dict[str, str]], num_lines: int = 5):
    """
    Affiche les premières lignes des profils dans le journal de logs.

    Cette fonction prend une liste de profils (chacun représenté par un dictionnaire)
    et affiche les détails des premiers profils dans le journal de logs. Elle est utile
    pour avoir un aperçu rapide des données de profil au début de l'exécution du programme.

    Args:
        profiles (List[Dict[str, str]]): Une liste de dictionnaires, où chaque dictionnaire
                                        représente un profil avec des paires clé-valeur.
        num_lines (int, optional): Le nombre de profils à afficher. Par défaut à 5.

    Fonctionnement:
        1. Affiche un message indiquant le nombre de profils qui vont être affichés.
        2. Itère sur les 'num_lines' premiers profils de la liste.
        3. Pour chaque profil, affiche toutes ses paires clé-valeur.
        4. Ajoute un séparateur entre chaque profil pour une meilleure lisibilité.

    Note:
        - Si la liste de profils contient moins d'éléments que 'num_lines',
        tous les profils disponibles seront affichés.
        - L'affichage se fait via le logger, assurez-vous que celui-ci est correctement configuré.

    Exemple d'utilisation:
        profiles = [
            {"Id": "1", "Nom": "Dupont", "Age": "30"},
            {"Id": "2", "Nom": "Martin", "Age": "25"},
            {"Id": "3", "Nom": "Durand", "Age": "35"}
        ]
        display_profiles(profils, num_lines=2)
    """
    logger.info(f"Affichage des {num_lines} premiers profils :")
    for i, profile in enumerate(profils[:num_lines], start=1):
        logger.info(f"Profil {i}:")
        for key, value in profile.items():
            logger.info(f"  {key}: {value}")
        logger.info("-" * 50)  # Séparateur entre les profils

async def exponential_backoff(page, url, max_retries=5, initial_timeout=30000):
    """
    Effectue une navigation vers une URL avec une stratégie de backoff exponentiel en cas d'échec.

    Cette fonction tente de naviguer vers l'URL spécifiée, en augmentant le temps d'attente
    et le délai d'expiration de manière exponentielle à chaque tentative échouée.

    Args:
        page (Page): L'objet Page de Playwright sur lequel effectuer la navigation.
        url (str): L'URL vers laquelle naviguer.
        max_retries (int, optional): Le nombre maximal de tentatives. Par défaut à 5.
        initial_timeout (int, optional): Le délai d'expiration initial en millisecondes. Par défaut à 30000 (30 secondes).

    Fonctionnement:
        1. Tente de naviguer vers l'URL avec un délai d'expiration initial.
        2. En cas d'échec (timeout), augmente le délai d'expiration de manière exponentielle.
        3. Attend un temps aléatoire entre les tentatives, également augmenté de manière exponentielle.
        4. Répète jusqu'à ce que la navigation réussisse ou que le nombre maximal de tentatives soit atteint.

    Raises:
        PlaywrightTimeoutError: Si la navigation échoue après le nombre maximal de tentatives.

    Logs:
        - Avertissement à chaque échec de tentative, indiquant le temps d'attente avant la prochaine tentative.
        - Erreur si toutes les tentatives échouent.

    Note:
        - Le temps d'attente entre les tentatives est calculé de manière aléatoire pour éviter les motifs prévisibles.
        - Le délai d'expiration augmente de manière exponentielle à chaque tentative.

    Exemple d'utilisation:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await exponential_backoff(page, "https://example.com")
    """
    for attempt in range(max_retries):
        try:
            timeout = initial_timeout * (2 ** attempt)
            await page.goto(url, timeout=timeout)
            return
        except PlaywrightTimeoutError:
            if attempt < max_retries - 1:
                wait_time = random.uniform(1, 2 ** attempt)
                logger.warning(
                    f"Timeout lors de la navigation vers {url}. Attente de {wait_time:.2f} secondes avant la prochaine tentative.")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Échec de la navigation vers {url} après {max_retries} tentatives")
                raise

async def get_random_browser(playwright: Playwright, bright_data: bool, headless: bool):
    """
    Crée et retourne de manière asynchrone une instance de navigateur aléatoire et son contexte avec des paramètres aléatoires.

    Cette fonction sélectionne un navigateur aléatoire (soit Chromium, soit Firefox), définit des dimensions de viewport,
    un user agent et une langue aléatoires. Elle peut lancer un navigateur directement ou se connecter à un navigateur Bright Data.

    Args:
        playwright (Playwright): L'instance Playwright à utiliser pour la création du navigateur.
        bright_data (bool): Si True, se connecte à un navigateur Bright Data. Si False, lance un navigateur local.
        headless (bool): Indique si le navigateur doit être exécuté en mode headless.

    Returns:
        tuple: Un tuple contenant :
            - browser: L'instance du navigateur lancé.
            - context: Le contexte du navigateur avec les paramètres appliqués.

    Raises:
        PlaywrightError: En cas d'erreur lors du lancement du navigateur ou de la création du contexte.

    Note:
        - La fonction utilise des listes prédéfinies USER_AGENTS et LANGUAGES pour la randomisation.
        - Lors de l'utilisation de Bright Data, elle utilise toujours Chromium et se connecte via CDP.
        - La fonction enregistre des informations sur le navigateur choisi et ses paramètres.

    Exemple:
        playwright = await async_playwright().start()
        browser, context = await get_random_browser(playwright, bright_data=False, headless=True)
    """
    browser_choice = random.choice(['chromium', 'firefox'])
    slow_mo = random.randint(500, 800)
    viewport = {
    "width": random.randint(1024, 1920),
    "height": random.randint(768, 1080)
    }
    user_agent = random.choice(USER_AGENTS)
    language = random.choice(LANGUAGES)

    launch_options = {
    "headless": headless,
    "slow_mo": slow_mo,
    }

    if bright_data:
        browser = await playwright.chromium.connect_over_cdp(SBR_WS_CDP)
    else:
        browser = await getattr(playwright, browser_choice).launch(**launch_options)

    context = await browser.new_context(
    viewport=viewport
    )

    logger.info(f"{browser_choice.capitalize()} a été choisi avec les options : {launch_options}, viewport: {viewport}, user_agent: {user_agent}, locale: {language}")
    return browser, context


async def simulate_human_behavior(page):
    """
    Simule un comportement humain sur une page web de manière asynchrone.

    Cette fonction effectue une série d'actions pour imiter le comportement d'un utilisateur humain
    sur une page web, incluant le défilement aléatoire, des pauses et des mouvements de souris.

    Args:
        page (Page): L'objet Page de Playwright sur lequel effectuer les actions.

    Actions réalisées:
        1. Défilement aléatoire de la page.
        2. Pause aléatoire entre 1 et 3 secondes.
        3. Mouvement aléatoire du curseur de la souris.
        4. Pause aléatoire supplémentaire entre 0.5 et 1.5 secondes.

    Note:
        - Cette fonction utilise des temps d'attente et des positions aléatoires pour rendre
        le comportement moins prévisible et plus proche d'un utilisateur humain.
        - Les actions sont exécutées de manière séquentielle avec des pauses entre chacune.

    Raises:
        PlaywrightError: En cas d'erreur lors de l'exécution des actions sur la page.

    Exemple d'utilisation:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto("https://example.com")
            await simulate_human_behavior(page)
    """
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight * Math.random());")
    await page.wait_for_timeout(random.randint(1000, 3000))
    await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
    await page.wait_for_timeout(random.randint(500, 1500))

async def fill_form_projet(page, profile):
    """
    Remplit le formulaire concernant le projet d'assurance auto de l'utilisateur.

    Cette fonction asynchrone navigue à travers les différentes sections du formulaire 'PROJET',
    en remplissant les champs selon les informations fournies dans le profil de l'utilisateur.

    Args:
        page (Page): L'objet Page de Playwright représentant la page web actuelle.
        profile (dict): Un dictionnaire contenant les informations du profil de l'utilisateur.

    Fonctionnement:
        1. Attend que le formulaire soit chargé.
        2. Remplit les informations sur le besoin d'assurance (achat prévu ou véhicule déjà possédé).
        3. Si applicable, remplit les détails sur le type d'achat de véhicule.
        4. Gère les informations sur les conducteurs secondaires.
        5. Remplit les informations sur le titulaire de la carte grise.
        6. Navigue vers la page suivante après avoir rempli le formulaire.

    Gestion des erreurs:
        - Utilise des blocs try/except pour gérer les timeouts et les erreurs inattendues.
        - Affiche des messages d'erreur détaillés en cas de problème.

    Logs:
        - Affiche des messages de progression et de confirmation pour chaque étape complétée.
        - Signale les valeurs non reconnues ou manquantes dans le profil.

    Note:
        - Utilise des constantes comme TIMEOUT pour la gestion des attentes.
        - Adapte le comportement en fonction des réponses précédentes (ex: affichage de champs supplémentaires).

    Raises:
        Exception: Capture et affiche toute exception survenant pendant le processus de remplissage.

    Exemple d'utilisation:
        await fill_form_projet(page, user_profile)
    """
    try:
        # Attendre que le formulaire soit chargé
        await page.wait_for_selector('div.al_label span:text("Votre projet")')
        print("Vous avez accés à la page du formulaire 'PROJET' ")
        print(f"Le profil '{profile['Id']}' est lancé....")
        await page.wait_for_selector('.InsuranceNeed', state='visible', timeout=TIMEOUT)
        if profile['InsuranceNeed'] == "Vous comptez l'acheter":
            await page.click('button.list-group-item[value="0"]')
            print(f"----> La valeur '{profile['InsuranceNeed']}' a été choisie...")
            try:
                await page.wait_for_selector('.InsuranceNeedDetail', state='visible', timeout=TIMEOUT)
                print("Le div InsuranceNeedDetail est apparu")
                if profile['InsuranceNeedDetail'] == "D'une voiture en remplacement":
                    await page.click('.InsuranceNeedDetail button.list-group-item[value="2"]')
                    print(f"----> La valeur '{profile['InsuranceNeedDetail']}' a été choisie...")
                elif profile['InsuranceNeedDetail'] == "D'une voiture supplémentaire":
                    await page.click('.InsuranceNeedDetail button.list-group-item[value="3"]')
                    print(f"----> La valeur '{profile['InsuranceNeedDetail']}' a été choisie...")
                elif profile['InsuranceNeedDetail'] == "D'une première voiture":
                    await page.click('.InsuranceNeedDetail button.list-group-item[value="4"]')
                    print(f"----> La valeur '{profile['InsuranceNeedDetail']}' a été choisie...")
                else:
                    print(f"Type d'achat de voiture non reconnu : {profile['InsuranceNeedDetail']}")

                if profile['AddCarAge'] == "Neuve":
                    await page.click('.AddCarAge button.list-group-item[value="1"]')
                    print(f"----> La valeur '{profile['AddCarAge']}' a été choisie...")
                elif profile['AddCarAge'] == "D'occasion":
                    await page.click('.AddCarAge button.list-group-item[value="2"]')
                    print(f"----> La valeur '{profile['AddCarAge']}' a été choisie...")
                else:
                    print(f"Type de détail de voiture non reconnu : {profile['AddCarAge']}")
            except PlaywrightTimeoutError:
                print("Le div InsuranceNeedDetail n'est pas apparu comme prévu")

        elif profile['InsuranceNeed'] == "Vous le possédez déjà":
            await page.click('button.list-group-item[value="1"]')
            print(f"----> La valeur '{profile['InsuranceNeed']}' a été choisie...")
        else:
            print(f"Statut d'achat non reconnu : {profile['InsuranceNeed']}")

        await page.wait_for_selector('.OtherDriver', state='visible', timeout=TIMEOUT)

        if profile['OtherDriver'] == "Oui":
            await page.click('.OtherDriver button.list-group-item[value="3"]')
            print(f"----> La valeur '{profile['OtherDriver']}' a été choisie sur la déclaration d'un conducteur secondaire.")
            try:
                if 'OtherDriverType' in profile:
                    await page.wait_for_selector('#OtherDriverType', state="visible", timeout=TIMEOUT)
                    if profile['OtherDriverType'] == "Votre conjoint ou concubin":
                        await page.select_option('#OtherDriverType', value="1")
                        print(
                            f"----> La valeur '{profile['OtherDriverType']}' a été choisie pour le type du conducteur secondaire.")
                    elif profile['OtherDriverType'] == "Votre enfant":
                        await page.select_option('#OtherDriverType', value="2")
                        print(
                            f"----> La valeur '{profile['OtherDriverType']}' a été choisie pour le type du conducteur secondaire.")
                    elif profile['OtherDriverType'] == "Votre père ou votre mère":
                        await page.select_option('#OtherDriverType', value="3")
                        print(
                            f"----> La valeur '{profile['OtherDriverType']}' a été choisie pour le type du conducteur secondaire.")
                    elif profile['OtherDriverType'] == "Le père ou la mère de votre conjoint ou concubin":
                        await page.select_option('#OtherDriverType', value="4")
                        print(
                            f"----> La valeur '{profile['OtherDriverType']}' a été choisie pour le type du conducteur secondaire.")
                    else:
                        print("Type du conducteur secondaire non reconnu")
                else:
                    print("Type du conducteur secondaire non spécifié dans profile")
            except PlaywrightTimeoutError:
                print("Le div OtherDriverType n'est pas apparu comme prévu")

        elif profile['OtherDriver'] == "Non":
            await page.click('.OtherDriver button.list-group-item[value="1"]')
            print(f"----> La valeur '{profile['OtherDriver']}' a été choisie sur la déclaration d'un conducteur secondaire.")
        else:
            print(f"Déclaration d'un conducteur secondaire : {profile['OtherDriver']}")

        # Titulaire de la carte grise
        if 'GreyCardOwner' in profile:
            await page.wait_for_selector('#GreyCardOwner', state="visible", timeout=TIMEOUT)
            if profile['GreyCardOwner'] == "Vous":
                await page.select_option('#GreyCardOwner', value="1")
                print(
                    f"----> La valeur '{profile['GreyCardOwner']}' a été choisie pour le Titulaire de la carte grise.")
            elif profile['OtherDriverType'] == "Votre conjoint ou concubin":
                await page.select_option('#GreyCardOwner', value="2")
                print(
                    f"----> La valeur '{profile['GreyCardOwner']}' a été choisie pour le Titulaire de la carte grise.")
            elif profile['OtherDriverType'] == "Vous ET votre conjoint ou concubin":
                await page.select_option('#GreyCardOwner', value="3")
                print(
                    f"----> La valeur '{profile['GreyCardOwner']}' a été choisie pour le Titulaire de la carte grise.")
            elif profile['OtherDriverType'] == "Votre père ou votre mère":
                await page.select_option('#GreyCardOwner', value="4")
                print(
                    f"----> La valeur '{profile['GreyCardOwner']}' a été choisie pour le Titulaire de la carte grise.")
            elif profile['OtherDriverType'] == "Le père ou la mère de votre conjoint ou concubin":
                await page.select_option('#GreyCardOwner', value="5")
                print(
                    f"----> La valeur '{profile['GreyCardOwner']}' a été choisie pour le Titulaire de la carte grise.")
            elif profile['OtherDriverType'] == "Il s'agit d'un véhicule de société":
                await page.select_option('#GreyCardOwner', value="6")
                print(
                    f"----> La valeur '{profile['GreyCardOwner']}' a été choisie pour le Titulaire de la carte grise.")
            else:
                print("Type du Titulaire de la carte grise non reconnu")
        else:
            print("Type du Titulaire de la carte grise non spécifié dans profile")

        print("Toutes les étapes de fill_form_projet ont été complétées avec succès.")

        await asyncio.sleep(2)
        await page.get_by_role("button", name="SUIVANT ").click()
        print("Navigation vers la page suivante : Votre profil.")

    except Exception as e:
        print(f"Une erreur s'est produite lors du remplissage du formulaire PROJET : {str(e)}")


async def fill_form_profil(page, profile):
    """
    Remplit le formulaire de profil sur une page web avec les informations fournies.

    Cette fonction automatise le processus de remplissage d'un formulaire de profil
    en utilisant Playwright. Elle gère divers champs tels que le sexe, la date de naissance,
    le statut matrimonial, la situation professionnelle, les informations sur le permis de conduire,
    et les détails sur le conjoint et les enfants si applicable.

    Args:
        page (Page): L'objet Page de Playwright représentant la page web actuelle.
        profile (dict): Un dictionnaire contenant les informations du profil à remplir.
            Clés attendues:
            - PrimaryApplicantSex (str): Le sexe du demandeur principal ("Un homme" ou "Une femme").
            - PrimaryApplicantBirthDate (str): La date de naissance du demandeur principal.
            - PrimaryApplicantMaritalStatus (str): Le statut matrimonial du demandeur.
            - PrimaryApplicantOccupationCode (str): Le code de profession du demandeur.
            - PrimaryApplicantDrivLicenseDate (str): La date d'obtention du permis de conduire.
            - PrimaryApplicantIsPreLicenseExper (str): Si le demandeur a fait de la conduite accompagnée ("Oui" ou "Non").
            - PrimaryApplicantDrivLicenseSusp (str): Le statut de suspension du permis de conduire.
            - ConjointNonSouscripteurBirthDate (str): La date de naissance du conjoint (si applicable).
            - ConjointNonSouscripteurHasDriveLicense (str): Si le conjoint a un permis de conduire ("Oui" ou "Non").
            - ConjointNonSouscripteurDriveLicenseDate (str): La date d'obtention du permis du conjoint (si applicable).
            - HasChild (str): Si le demandeur a des enfants ("Oui" ou "Non").
            - ChildBirthDateYear1, ChildBirthDateYear2, ChildBirthDateYear3 (str): Les années de naissance des enfants (si applicable).

    Raises:
        ValueError: Si une valeur invalide est fournie pour un champ ou si une erreur survient lors du remplissage.
        Exception: Pour toute autre erreur inattendue lors du processus de remplissage.

    Returns:
        None

    Note:
        Cette fonction utilise des sélecteurs spécifiques et des timeouts pour interagir avec les éléments de la page.
        Elle effectue également des vérifications pour s'assurer que les valeurs sont correctement saisies.
    """
    try:
        await page.wait_for_selector('div.al_label span:text("Votre profil")', timeout=TIMEOUT)
        await page.wait_for_selector('.PrimaryApplicantSex', state="visible", timeout=TIMEOUT)
        if profile['PrimaryApplicantSex'] == "Un homme":
            await page.click('.PrimaryApplicantSex button.list-group-item[value="H"]')
            print(f"----> La valeur '{profile['PrimaryApplicantSex']}' a été choisie pour le genre.")
        elif profile['PrimaryApplicantSex'] == "Une femme":
            await page.click('.PrimaryApplicantSex button.list-group-item[value="F"]')
            print(f"----> La valeur '{profile['PrimaryApplicantSex']}' a été choisie pour le genre.")
        else:
            print("Genre non reconnu dans profile")

        await page.wait_for_selector("#PrimaryApplicantBirthDate", state="visible", timeout=60000)
        await page.evaluate('document.getElementById("PrimaryApplicantBirthDate").value = ""')
        await page.fill("#PrimaryApplicantBirthDate", profile['PrimaryApplicantBirthDate'])
        await page.press("#PrimaryApplicantBirthDate", "Enter")
        await page.wait_for_timeout(500)
        entered_value = await page.evaluate('document.getElementById("PrimaryApplicantBirthDate").value')
        if entered_value != profile['PrimaryApplicantBirthDate']:
            raise ValueError(
                f"La date de naissance saisie ({entered_value}) ne correspond pas à la valeur attendue ({profile['PrimaryApplicantBirthDate']})")
        print(f"----> Date de naissance '{profile['PrimaryApplicantBirthDate']}' saisie avec succès.")

        """ Statut matrimonial du profil """
        try:
            await page.wait_for_selector("#PrimaryApplicantMaritalStatus", state="visible", timeout=60000)
            await page.select_option("#PrimaryApplicantMaritalStatus", label=profile['PrimaryApplicantMaritalStatus'])
            print(f"----> Statut marital '{profile['PrimaryApplicantMaritalStatus']}' sélectionné avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de la sélection du statut marital: {str(e)}")
            # await page.screenshot(path="error_marital_status.png")
            raise ValueError(f"Erreur lors de la sélection du statut marital : {str(e)}")

        """ Situation prof du profil """
        try:
            await page.wait_for_selector("#PrimaryApplicantOccupationCode", state="visible", timeout=60000)
            await page.select_option("#PrimaryApplicantOccupationCode", label=profile['PrimaryApplicantOccupationCode'])
            print(f"----> Statut professionnel '{profile['PrimaryApplicantOccupationCode']}' sélectionné avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de la sélection du statut professionnel: {str(e)}")
            # await page.screenshot(path="error_professional_status.png")
            raise ValueError(f"Erreur lors de la sélection du statut professionnel : {str(e)}")

        """ Date d'obtention du permis du profil """
        try:
            await page.wait_for_selector("#PrimaryApplicantDrivLicenseDate", state="visible", timeout=60000)
            await page.evaluate('document.getElementById("PrimaryApplicantDrivLicenseDate").value = ""')
            await page.fill("#PrimaryApplicantDrivLicenseDate", profile['PrimaryApplicantDrivLicenseDate'])
            await page.press("#PrimaryApplicantDrivLicenseDate", "Enter")
            await page.wait_for_timeout(500)
            print(
                f"----> Date d'obtention du permis '{profile['PrimaryApplicantDrivLicenseDate']}' saisie avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de la saisie de la date d'obtention du permis: {str(e)}")
            # await page.screenshot(path="error_driving_license_date.png")
            raise ValueError(f"Erreur lors de la saisie de la date d'obtention du permis : {str(e)}")

        try:
            await page.wait_for_selector(".PrimaryApplicantIsPreLicenseExper", state="visible", timeout=TIMEOUT)
            buttons = await page.query_selector_all(".PrimaryApplicantIsPreLicenseExper button")
            if not buttons:
                raise ValueError("Les boutons de conduite accompagnée n'ont pas été trouvés.")
            if profile['PrimaryApplicantIsPreLicenseExper'] == "Oui":
                await page.click('.PrimaryApplicantIsPreLicenseExper button.list-group-item[value="True"]')
                print(
                    f"----> La valeur '{profile['PrimaryApplicantIsPreLicenseExper']}' a été choisie pour la conduite accompagnée.")
            elif profile['PrimaryApplicantIsPreLicenseExper'] == "Non":
                await page.click('.PrimaryApplicantIsPreLicenseExper button.list-group-item[value="False"]')
                print(
                    f"----> La valeur '{profile['PrimaryApplicantIsPreLicenseExper']}' a été choisie pour la conduite accompagnée.")
            else:
                raise ValueError(
                    f"Valeur non reconnue pour la conduite accompagnée : {profile['PrimaryApplicantIsPreLicenseExper']}")
        except Exception as e:
            logging.error(f"Erreur lors de la sélection de l'option de conduite accompagnée: {str(e)}")
            # await page.screenshot(path="error_accompanied_driving.png")
            raise ValueError(f"Erreur lors de la sélection de l'option de conduite accompagnée : {str(e)}")

        try:
            await page.wait_for_selector("#PrimaryApplicantDrivLicenseSusp", state="visible", timeout=10000)
            await page.select_option("#PrimaryApplicantDrivLicenseSusp",
                                     label=profile['PrimaryApplicantDrivLicenseSusp'])
            selected_value = await page.evaluate('document.getElementById("PrimaryApplicantDrivLicenseSusp").value')
            selected_text = await page.evaluate(
                'document.getElementById("PrimaryApplicantDrivLicenseSusp").options[document.getElementById("PrimaryApplicantDrivLicenseSusp").selectedIndex].text')
            if selected_text != profile['PrimaryApplicantDrivLicenseSusp']:
                raise ValueError(
                    f"Le statut sélectionné ({selected_text}) ne correspond pas au statut attendu ({profile['PrimaryApplicantDrivLicenseSusp']})")
            print(
                f"----> Statut de suspension du permis '{profile['PrimaryApplicantDrivLicenseSusp']}' sélectionné avec succès (valeur: {selected_value}).")
        except Exception as e:
            logging.error(f"Erreur lors de la sélection du statut de suspension du permis: {str(e)}")
            # await page.screenshot(path="error_license_suspension_status.png")
            raise ValueError(f"Erreur lors de la sélection du statut de suspension du permis : {str(e)}")

        """ Votre conjoint ou concubin """
        valid_marital_statuses = ["Marié(e)", "Concubin(e) / vie maritale", "Pacsé(e)"]
        if profile.get('PrimaryApplicantMaritalStatus') in valid_marital_statuses:
            logging.info(f"Le statut marital '{profile.get('PrimaryApplicantMaritalStatus')}' nécessite de remplir l'information sur le permis du conjoint.")
            try:
                await page.wait_for_selector("#ConjointNonSouscripteurBirthDate", state="visible", timeout=60000)
                await page.evaluate('document.getElementById("ConjointNonSouscripteurBirthDate").value = ""')
                await page.fill("#ConjointNonSouscripteurBirthDate", profile['ConjointNonSouscripteurBirthDate'])
                await page.press("#ConjointNonSouscripteurBirthDate", "Enter")
                await page.wait_for_timeout(500)
                print(f"----> Date de naissance '{profile['ConjointNonSouscripteurBirthDate']}' saisie avec succès.")
            except Exception as e:
                logging.error(f"Erreur lors de la saisie de la date de naissance: {str(e)}")
                # await page.screenshot(path="error_birth_date.png")
                raise ValueError(f"Erreur lors de la saisie de la date de naissance : {str(e)}")

            try:
                await page.wait_for_selector('.ConjointNonSouscripteurHasDriveLicense', state='visible', timeout=TIMEOUT)
                if profile['ConjointNonSouscripteurHasDriveLicense'] == "Non":
                    await page.click('.ConjointNonSouscripteurHasDriveLicense button.list-group-item[value="False"]')
                    print(f"----> La valeur '{profile['ConjointNonSouscripteurHasDriveLicense']}' a été choisie pour le conjoint avec un permis.")
                elif profile['ConjointNonSouscripteurHasDriveLicense'] == "Oui":
                    await page.click('.ConjointNonSouscripteurHasDriveLicense button.list-group-item[value="True"]')
                    print(f"----> La valeur '{profile['ConjointNonSouscripteurHasDriveLicense']}' a été choisie pour le conjoint avec un permis.")
                    await page.wait_for_selector("#ConjointNonSouscripteurDriveLicenseDate", state="visible",
                                                 timeout=60000)
                    await page.evaluate('document.getElementById("ConjointNonSouscripteurDriveLicenseDate").value = ""')
                    await page.fill("#ConjointNonSouscripteurDriveLicenseDate", profile['ConjointNonSouscripteurDriveLicenseDate'])
                    await page.press("#ConjointNonSouscripteurDriveLicenseDate", "Enter")
                    await page.wait_for_timeout(500)  # Attendre que le calendrier se ferme
                    print(f"Date d'obtention du permis du conjoint '{profile['ConjointNonSouscripteurDriveLicenseDate']}' saisie avec succès.")
                else:
                    print('Valeur non reconnu pour le permis du conjoint ou concubin')
            except Exception as e:
                print(f"Une erreur soulevé sur les informations du conjoint : {str(e)}")
        else:
            print(f"Le statut marital '{profile.get('PrimaryApplicantMaritalStatus')}' ne nécessite pas de remplir l'information sur le permis du conjoint.")

        """ Vos enfants """
        try:
            await page.wait_for_selector('.HasChild', state='visible', timeout=TIMEOUT)
            if profile['HasChild'] == "Oui":
                await page.click('.HasChild button.list-group-item[value="True"]')
                await page.select_option('#ChildBirthDateYear1', value=profile['ChildBirthDateYear1'])
                print(
                    f"Année de l'enfant 1 '{profile['ChildBirthDateYear1']}' saisie avec succès.")
                await page.select_option('#ChildBirthDateYear2', value=profile['ChildBirthDateYear2'])
                print(
                    f"Année de l'enfant 2'{profile['ChildBirthDateYear2']}' saisie avec succès.")
                await page.select_option('#ChildBirthDateYear3', value=profile['ChildBirthDateYear3'])
                print(
                    f"Année de l'enfant 3 '{profile['ChildBirthDateYear3']}' saisie avec succès.")
            elif profile['HasChild'] == "Non":
                await page.click('.HasChild button.list-group-item[value="False"]')
            else:
                print("Valeur non connue pour les enfants")

        except Exception as e:
            print(f"Une erreur soulevé sur les informations des années de naissance des enfants : {str(e)}")

        await asyncio.sleep(2)
        await page.get_by_role("button", name="SUIVANT ").click()
        print("Navigation vers la page suivante : Votre profil.")
    except Exception as e:
        print(f"Une erreur s'est produite lors du remplissage du formulaire PROFIL : {str(e)}")





async def run_for_profile(playwright: Playwright, profile: dict, headless: bool, bright_data: bool,
                        url=TARGET_URL) -> None:
    """
    Exécute un processus de simulation pour un profil donné sur un site web cible.

    Cette fonction asynchrone gère l'ensemble du processus de simulation pour un profil d'assurance auto,
    depuis l'ouverture du navigateur jusqu'à la récupération des tarifs, en passant par le remplissage
    de plusieurs formulaires.

    Args:
        playwright (Playwright): L'instance Playwright pour la création du navigateur.
        profile (dict): Le dictionnaire contenant les informations du profil à simuler.
        headless (bool): Indique si le navigateur doit être exécuté en mode headless.
        bright_data (bool): Indique si Bright Data doit être utilisé pour la connexion.
        url (str, optional): L'URL cible. Par défaut, utilise TARGET_URL.

    Actions principales:
        1. Initialisation du navigateur et de la page.
        2. Navigation vers l'URL cible avec gestion des retards.
        3. Simulation de comportement humain.
        4. Remplissage de plusieurs formulaires (projet, profil, véhicule, antécédents, contrats).
        5. Récupération des tarifs.

    Gestion des erreurs:
        - En cas d'erreur, les informations du profil sont enregistrées dans un fichier JSON d'échecs.
        - Le fichier d'échecs est nommé avec la date du jour et la plage de lignes traitées.

    Note:
        - La fonction utilise des techniques pour éviter la détection de l'automatisation.
        - Des pauses aléatoires et des simulations de comportement humain sont intégrées.

    Raises:
        Exception: Toute exception survenant pendant l'exécution est capturée, enregistrée,
                    et relancée après avoir sauvegardé les informations d'échec.

    Exemple d'utilisation:
        async with async_playwright() as p:
            await run_for_profile(p, profile_data, headless=True, bright_data=False)
    """
    await asyncio.sleep(random.uniform(1, 2))

    browser, context = await get_random_browser(playwright, bright_data, headless)
    page = await context.new_page()

    # Ajouter un script furtif
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});""")

    try:
        await exponential_backoff(page, url)
        await simulate_human_behavior(page)

        actions = [
            page.get_by_role("button", name="Continuer sans accepter").click,
            page.get_by_role("button", name="Tout accepter").click
        ]
        await random.choice(actions)()
        await simulate_human_behavior(page)

        AUTO_INSURANCE_SELECTOR = "div.al_product.al_car[title='Comparez les assurances auto']"
        await page.wait_for_selector(AUTO_INSURANCE_SELECTOR, state="visible", timeout=30000)
        auto_insurance_div = page.locator(AUTO_INSURANCE_SELECTOR)
        await expect(auto_insurance_div).to_be_enabled(timeout=30000)
        await auto_insurance_div.click()
        await simulate_human_behavior(page)

        logger.info("Cliqué sur le div 'Comparez les assurances auto'")
        print(f"Le profil '{profile['Id']}' est lancé....")
        await fill_form_projet(page, profile)
        #await simulate_human_behavior(page)
        await page.wait_for_load_state("networkidle")
        logger.info("=" * 100)
        await fill_form_profil(page, profile)
        #await simulate_human_behavior(page)
        await page.wait_for_load_state("networkidle")
        logger.info("=" * 100)
        

        

    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du profil: {str(e)}")
        date_du_jour = datetime.now().strftime("%d_%m_%y")

        # Créer le nom du fichier avec la date du jour
        nom_fichier_echecs = f"fichiers_echecs_{date_du_jour}_{start_line}_au_{end_line}.json"
        # Écrire les informations du profil dans le fichier JSON des échecs
        async with aiofiles.open(nom_fichier_echecs, mode='a') as f:
            await f.write(json.dumps({'ID': profile['Id']}))
            await f.write('\n')
            print(f"=====> Le profil {profile['Id']} a été stocké dans le fichier des échecs:", nom_fichier_echecs)
        raise
    finally:
        await context.close()

async def run_for_profile_with_semaphore_and_progress(playwright, profile, headless, bright_data, semaphore,
                                                    progress_bar):
    """
    Fonction wrapper asynchrone qui exécute le traitement d'un profil avec gestion du sémaphore et de la progression.

    Cette fonction encapsule l'appel à run_for_profile en ajoutant la gestion du sémaphore pour
    contrôler la concurrence et la mise à jour de la barre de progression.

    Args:
        playwright (Playwright): L'instance Playwright pour la création du navigateur.
        profile (dict): Le dictionnaire contenant les informations du profil à traiter.
        headless (bool): Indique si le navigateur doit être exécuté en mode headless.
        bright_data (bool): Indique si Bright Data doit être utilisé pour la connexion.
        semaphore (asyncio.Semaphore): Le sémaphore pour contrôler la concurrence.
        progress_bar (tqdm): La barre de progression à mettre à jour.

    Fonctionnement:
        1. Acquiert le sémaphore avant d'exécuter le traitement.
        2. Appelle run_for_profile pour traiter le profil.
        3. Gère les exceptions potentielles lors du traitement.
        4. Met à jour la barre de progression après le traitement.
        5. Libère le sémaphore, que le traitement ait réussi ou échoué.

    Raises:
        Aucune exception n'est levée, mais les erreurs sont enregistrées dans le journal.

    Note:
        - Cette fonction est conçue pour être utilisée avec asyncio.gather dans la fonction main.
        - Elle assure que le sémaphore est toujours libéré et que la progression est mise à jour,
        même en cas d'erreur lors du traitement du profil.
    """
    async with semaphore:
        try:
            await run_for_profile(playwright, profile, headless, bright_data)
        except Exception as e:
            logger.error(f"Erreur lors du traitement du profil {profile['Id']}: {str(e)}")
        finally:
            progress_bar.update(1)


                    

async def main(headless: bool, bright_data: bool, max_concurrent: int = 20):
    """
    Fonction principale asynchrone qui gère le traitement parallèle des profils.

    Cette fonction coordonne le processus global de lecture des profils à partir d'un fichier CSV,
    l'affichage d'un aperçu des profils, et le lancement du traitement parallèle de ces profils
    en utilisant Playwright.

    Args:
        headless (bool): Indique si les navigateurs doivent être exécutés en mode headless.
        bright_data (bool): Indique si Bright Data doit être utilisé pour la connexion.
        max_concurrent (int, optional): Nombre maximal de tâches concurrentes. Par défaut à 20.

    Fonctionnement:
        1. Lecture des profils à partir d'un fichier CSV.
        2. Vérification de la présence de profils.
        3. Initialisation d'un sémaphore pour limiter la concurrence.
        4. Création d'une barre de progression pour suivre l'avancement.
        5. Affichage d'un aperçu des profils.
        6. Lancement du traitement parallèle des profils avec Playwright.
        7. Attente de la fin de tous les traitements.

    Utilise:
        - read_csv_profiles(): Fonction pour lire les profils depuis un fichier CSV.
        - display_profiles(): Fonction pour afficher un aperçu des profils.
        - run_for_profile_with_semaphore_and_progress(): Fonction asynchrone pour traiter un profil.

    Note:
        - La fonction utilise asyncio.gather pour exécuter toutes les tâches en parallèle.
        - Un sémaphore est utilisé pour limiter le nombre de tâches concurrentes.
        - Une barre de progression (tqdm) est utilisée pour suivre l'avancement global.

    Raises:
        Aucune exception n'est explicitement levée, mais des erreurs peuvent se produire
        lors de la lecture des profils ou de leur traitement.

    Exemple d'utilisation:
        asyncio.run(main(headless=True, bright_data=False, max_concurrent=10))
    """
    
    if not profils:
        logger.error("Aucun profil n'a été lu. Fin du programme.")
        return
    semaphore = Semaphore(max_concurrent)
    progress_bar = tqdm(total=len(profils), desc="Traitement des profils")

    display_profiles(profils)

    async with async_playwright() as playwright:
        tasks = [
            run_for_profile_with_semaphore_and_progress(playwright, profile, headless, bright_data, semaphore,
                                                        progress_bar)
            for profile in profils
        ]
        await asyncio.gather(*tasks)

    progress_bar.close()

if __name__ == "__main__":
    """
    Point d'entrée principal du script.

    Ce bloc exécute la fonction main avec des paramètres spécifiques lorsque le script est
    exécuté directement (et non importé comme un module).

    Paramètres utilisés:
        - headless=False : Les navigateurs seront exécutés en mode visible (non headless).
        - bright_data=True : Utilisation de Bright Data pour la connexion.
        - max_concurrent=20 : Limite le nombre de tâches concurrentes à 20.

    Fonctionnement:
        Utilise asyncio.run pour exécuter la coroutine main dans la boucle d'événements asyncio.

    Note:
        - Ces paramètres peuvent être ajustés selon les besoins spécifiques de l'exécution.
        - L'exécution en mode non-headless (headless=False) permet de voir les navigateurs en action,
        ce qui peut être utile pour le débogage mais peut consommer plus de ressources.
        - L'utilisation de Bright Data (bright_data=True) implique que la configuration appropriée
        pour Bright Data est en place.
    """
    asyncio.run(main(headless=False, bright_data=True, max_concurrent=20))
