# flake8: noqa: E221, E241
# pylint: disable=C0301
# pylint: disable=C0303, C0114, W0613, R0914, RO915, C0411, W0012, R0915, R1705, C0103, W0621, W0611, C0305, C0412, C0404, W1203, W0404, W0718, R0912, R0913, W0105, W0707


import pandas as pd

import random
from faker import Faker
import unidecode


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
from playwright.async_api import async_playwright, Playwright, expect, TimeoutError as PlaywrightTimeoutError, Page


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
  "P": 60,  # Comme, conducteur principal
  "S": 40,  # Comme, conducteur secondaire
  "N": 00   # Non
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
    codes_a_formater = ['HomeParkZipCode', 'HomeParkInseeCode', 'JobParkZipCode', 'JobParkInseeCode']
    for col in codes_a_formater:
        df[col] = df[col].str.zfill(5)
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
        age_conjoint_min, age_conjoint_max = 19, 100
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
    first_name = unidecode.unidecode(fake.first_name())  # Prénom sans accents
    last_name = unidecode.unidecode(fake.last_name()) 
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
        'Id': str(random.randint(1001, 1500)),  # Id aléatoire entre 5 et 10 (as a string)
        'TitleAddress': random.choice(["MONSIEUR", "MADAME"]), 
        'LastName': last_name,  # Nom de famille
        'FirstName': first_name,  # Prénom
        'Address': f"{fake.building_number()} {fake.street_name()}",  # Adresse sans ville ni code postal
        'Email': email,  # Email généré
        'Phone': f"0{random.randint(6, 7)}{random.randint(10000000, 99999999)}",  # Numéro de téléphone valide en France
        'DateScraping': date_scrap.strftime('%d/%m/%Y')
        
    }

# Charger les données de véhicules
chemin_fichier_vehicules = 'C:/Users/User/PycharmProjects/Production-Scrapping-Automobile/notebook/df_sra_final.csv'  # Remplacez par le chemin de votre fichier CSV
vehicules_df = charger_donnees_vehicules(chemin_fichier_vehicules)

# Charger les données de véhicules neufs
chemin_fichier_vehicules_neuves = 'C:/Users/User/PycharmProjects/Production-Scrapping-Automobile/notebook/df_sra_neuve.csv'  # Remplacez par le chemin de votre fichier CSV
vehicules_neuves_df = charger_donnees_vehicules_neuve(chemin_fichier_vehicules_neuves)

# Charger les données des communes
chemin_fichier_communes = 'C:/Users/User/PycharmProjects/Production-Scrapping-Automobile/notebook/df_communes.csv'  # Remplacez par le chemin réel
communes_df = charger_donnees_communes(chemin_fichier_communes)

# Générer les profils avec les informations de véhicules
profils = []
for _ in range(250):  # Par exemple, générer 1000 profils
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
    profil['PrimaryApplicantIsFirstDrivOtherCar'] = "Non"  # Nouvelle colonne avec valeur "Non"
    profil['PrimaryApplicantContrCancell'] = "N"
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
    profil['PrimaryApplicantDisasterLast3year'] = "0"
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
    weights = [0.5, 0.5, 0.0]  # Exemple de poids pour chaque code
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
    profil['UserOptIn'] = "1"

    profil['CarSelectMode'] = "2"


# Convertir les profils en DataFrame et sauvegarder
profils_df = pd.DataFrame(profils)
# Afficher les 10 premiers profils

# Paramètres
start_line = 1021  # Commencer à la première ligne
end_line = 1500   # Finir à la dixième ligne

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

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

TIMEOUT = 2 * 60000
SBR_WS_CDP = 'wss://brd-customer-hl_e9a5f52e-zone-scraping_browser1:jpuci55coo47@brd.superproxy.io:9222'
# SBR_WS_CDP = 'wss://brd-customer-hl_538b14f9-zone-scraping_browser1:zk8riomp0pt9@brd.superproxy.io:9222'

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/113.0.0.0"]
 

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
        profils:
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
    slow_mo = random.randint(100, 500)
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


async def select_car_brand_Neuve(page: Page, profile: dict, max_retries=3, retry_delay=2):
    selector = "#SpecCarMakeName"
    brand = profile['SpecCarMakeNameNeuve']

    for attempt in range(max_retries):
        try:
            # Attendre que le sélecteur soit visible
            await page.wait_for_selector(selector, state="visible", timeout=30000)

            # Attendre que le sélecteur soit activé (non désactivé)
            await page.wait_for_function(
                f"!document.querySelector('{selector}').disabled",
                timeout=30000
            )

            # Vérifier si l'option existe avant de la sélectionner
            option_exists = await page.evaluate(f"""
                () => {{
                    const select = document.querySelector('{selector}');
                    return Array.from(select.options).some(option => option.text === '{brand}');
                }}
            """)

            if not option_exists:
                print(f"L'option '{brand}' n'existe pas dans la liste déroulante.")
                return False

            # Sélectionner l'option
            await page.select_option(selector, label=brand)

            # Vérifier si la sélection a réussi
            selected_value = await page.evaluate(f"document.querySelector('{selector}').value")
            if selected_value:
                print(f"----> Marque de la voiture '{brand}' sélectionnée avec succès.")
                return True
            else:
                raise Exception("La sélection n'a pas été appliquée.")

        except (PlaywrightTimeoutError, TimeoutError) as e:
            if attempt < max_retries - 1:
                print(f"Tentative {attempt + 1} échouée. Nouvelle tentative dans {retry_delay} secondes...")
                await asyncio.sleep(retry_delay)
            else:
                print(f"Le sélecteur '{selector}' n'est pas devenu interactif après {max_retries} tentatives.")

        except Exception as e:
            print(f"Erreur lors de la sélection de la marque de voiture: {str(e)}")
            break

    print("Impossible de sélectionner la marque de voiture.")
    return False

async def select_car_brand_Occasion(page: Page, profile: dict, max_retries=3, retry_delay=2):
    selector = "#SpecCarMakeName"
    brand = profile['SpecCarMakeName']

    for attempt in range(max_retries):
        try:
            # Attendre que le sélecteur soit visible
            await page.wait_for_selector(selector, state="visible", timeout=30000)

            # Attendre que le sélecteur soit activé (non désactivé)
            await page.wait_for_function(
                f"!document.querySelector('{selector}').disabled",
                timeout=30000
            )

            # Vérifier si l'option existe avant de la sélectionner
            option_exists = await page.evaluate(f"""
                () => {{
                    const select = document.querySelector('{selector}');
                    return Array.from(select.options).some(option => option.text === '{brand}');
                }}
            """)

            if not option_exists:
                print(f"L'option '{brand}' n'existe pas dans la liste déroulante.")
                return False

            # Sélectionner l'option
            await page.select_option(selector, label=brand)

            # Vérifier si la sélection a réussi
            selected_value = await page.evaluate(f"document.querySelector('{selector}').value")
            if selected_value:
                print(f"----> Marque de la voiture '{brand}' sélectionnée avec succès.")
                return True
            else:
                raise Exception("La sélection n'a pas été appliquée.")

        except (PlaywrightTimeoutError, TimeoutError) as e:
            if attempt < max_retries - 1:
                print(f"Tentative {attempt + 1} échouée. Nouvelle tentative dans {retry_delay} secondes...")
                await asyncio.sleep(retry_delay)
            else:
                print(f"Le sélecteur '{selector}' n'est pas devenu interactif après {max_retries} tentatives.")

        except Exception as e:
            print(f"Erreur lors de la sélection de la marque de voiture: {str(e)}")
            break

    print("Impossible de sélectionner la marque de voiture.")
    return False

async def fill_form_vehicule(page, profile):
    """
    Remplit de manière asynchrone un formulaire de véhicule sur une page web en utilisant les informations de profil fournies.

    Cette fonction automatise le processus de remplissage d'un formulaire détaillé de véhicule, gérant divers
    types d'entrées tels que les listes déroulantes, les champs de texte et les sélecteurs de date. Elle couvre un large éventail
    d'informations liées au véhicule, y compris les détails d'achat, les spécifications du véhicule, les habitudes d'utilisation,
    et les informations de stationnement.

    Paramètres :
    ------------
    page : playwright.async_api.Page
        L'objet Page de Playwright représentant la page web où se trouve le formulaire.
    profile : dict
        Un dictionnaire contenant toutes les informations nécessaires pour remplir le formulaire. Les clés attendues incluent :
        - 'CarSelectMode' : Détermine le mode de sélection du véhicule (par exemple, "1" ou "2")
        - 'InsuranceNeed' : Indique si le véhicule est en cours d'achat
        - 'AddCarAge' : Indique si le véhicule est neuf ou d'occasion
        - Diverses clés de spécification du véhicule : 'car_make_value', 'car_type_value', 'alimentation_value',
          'carosserie_value', 'puissance_value', 'id', etc.
        - 'PurchaseDate' : La date d'achat du véhicule
        - 'FirstCarDrivingDate_1' : La première date de mise en circulation du véhicule
        - 'PurchaseMode' : Le mode de financement du véhicule
        - 'CarUsageCode' : L'usage prévu du véhicule
        - 'AvgKmNumber' : Kilométrage moyen parcouru par an
        - 'FreqCarUse' : Fréquence d'utilisation du véhicule
        - 'HomeParkZipCode' : Code postal du lieu de stationnement la nuit
        - 'HomeParkInseeCode' : Code INSEE de la ville de stationnement la nuit
        - 'HomeType' : Type de résidence principale
        - 'HomeResidentType' : Type de résidence (par exemple, propriétaire, locataire)
        - 'JobParkZipCode' : Code postal du lieu de travail
        - 'JobParkInseeCode' : Code INSEE de la ville du lieu de travail
        - 'ParkingCode' : Type de stationnement utilisé la nuit

    Retourne :
    ----------
    None

    Lève :
    ------
    ValueError
        S'il y a une erreur dans la sélection des options, la saisie des données ou la navigation dans le formulaire.
    PlaywrightTimeoutError
        Si un sélecteur ne devient pas visible dans le délai spécifié.

    Notes :
    -------
    - La fonction utilise divers sélecteurs pour interagir avec les éléments du formulaire, elle est donc sensible aux changements
      dans la structure de la page.
    - Elle inclut plusieurs blocs try-except pour gérer les erreurs potentielles et fournir des retours détaillés.
    - La fonction suppose que la page est déjà naviguée vers le bon formulaire avant d'être appelée.
    - Après avoir rempli tous les champs, elle tente de naviguer vers la page suivante.
    - Des informations de débogage sont imprimées dans la console tout au long du processus.

    Exemple :
    ---------

    """
    try:
        await page.wait_for_selector('div.form-group.has-feedback.CarSelectMode', state='visible', timeout=6000)
        if profile['CarSelectMode'] == "1":
            selectmode = await page.wait_for_selector('div.list-group > button.list-group-item[value="1"]')
            await selectmode.click()
            print(
                f"----> L'option avec la valeur '\033[34m{profile['CarSelectMode']}\033[0m' a été sélectionnée avec succès pour la question : la selection de Modéle / Marque. ")
        elif profile['CarSelectMode'] == "2":
            selectmode = await page.wait_for_selector('div.list-group > button.list-group-item[value="2"]')
            await selectmode.click()
            print(
                f"----> L'option avec la valeur '\033[34m{profile['CarSelectMode']}\033[0m' a été sélectionnée avec succès pour la question : la selection de Modéle / Marque. ")
        else:
            raise ValueError("Erreur sur la valeur prise par CarSelectMode")
    except PlaywrightTimeoutError:
        print("Le bouton '.CarSelectMode' n'est pas visible.")
    except Exception as e:
        raise ValueError(f"Erreur d'exception sur les informations de la selection de Modéle / Marque : {str(e)}")

    """ Condition si le projet consiste à un achat de véhicule neuf """
    try:
        if profile['InsuranceNeed'] == "Vous comptez l'acheter" and profile['AddCarAge'] == "Neuve":
            try:
                await page.wait_for_selector('#PurchaseDatePrev', state='visible', timeout=6000)
                await page.get_by_label("Date d'achat prévue du vé").click()
                await page.get_by_role("cell", name="Aujourd'hui").click()
                print(
                    f"----> L'option avec la valeur Aujourd'hui a été sélectionnée avec succès pour la question : La date d'achat.")
            except PlaywrightTimeoutError:
                print("Le bouton '.PurchaseDatePrev' n'est pas visible.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations de la date d'achat prévue : {str(e)}")

            """ Marque de la voiture """
            success = await select_car_brand_Neuve(page, profile)
            if not success:
                print("La sélection de la marque de voiture a échoué. Gestion de l'erreur...")

            """ Modéle de la marque de voiture """
            await asyncio.sleep(2)
            try:
                await page.wait_for_selector("#SpecCarType", state="visible", timeout=2 * 60000)
                element_SpecCarType = await page.query_selector("#SpecCarType")
                await element_SpecCarType.wait_for_element_state("enabled", timeout=6000)
                await page.select_option("#SpecCarType", label=profile['SpecCarTypeNeuve'], timeout=6000)
                print(f"----> Modéle de la voiture '{profile['SpecCarTypeNeuve']}' sélectionné avec succès.")
            except PlaywrightTimeoutError:
                print("Le bouton '.SpecCarType' n'est pas visible.")
            except Exception as e:
                logging.error(f"Erreur lors de la sélection du modéle de voiture: {str(e)}")
                raise ValueError(f"Erreur lors de la sélection du modéle de voiture : {str(e)}")

            """ Type d'alimentation de la voiture """
            await asyncio.sleep(3)
            try:
                await page.wait_for_selector("#SpecCarFuelType", state="visible", timeout=TIMEOUT)
                element_SpecCarFuelType = await page.query_selector("#SpecCarFuelType")
                await element_SpecCarFuelType.wait_for_element_state("enabled", timeout=60000)
                await page.select_option("#SpecCarFuelType", value=profile['SpecCarFuelTypeNeuve'], timeout=6000)
                print(f"----> Alimentation de la voiture '{profile['SpecCarFuelTypeNeuve']}' sélectionné avec succès.")
            except PlaywrightTimeoutError:
                print("Le bouton '.SpecCarFuelType' n'est pas visible.")
            except Exception as e:
                logging.error(f"Erreur lors de la sélection de Alimentation de voiture: {str(e)}")
                raise ValueError(f"Erreur lors de la sélection de Alimentation de voiture : {str(e)}")

            """ Type de carrosserie de la voiture """
            await asyncio.sleep(3)
            try:
                await page.wait_for_selector("#SpecCarBodyType", state="visible", timeout=TIMEOUT)
                element_SpecCarBodyType = await page.query_selector("#SpecCarBodyType")
                await element_SpecCarBodyType.wait_for_element_state("enabled", timeout=60000)
                await page.select_option("#SpecCarBodyType", value=profile['SpecCarBodyTypeNeuve'], timeout=6000)
                print(f"----> Carosserie de la voiture '{profile['SpecCarBodyTypeNeuve']}' sélectionnée avec succès.")
            except PlaywrightTimeoutError:
                print(f"Le sélecteur SpecCarBodyType n'est pas devenu visible dans le délai imparti.")
            except Exception as e:
                logging.error(f"Erreur lors de la sélection de la carrosserie de la voiture: {str(e)}")
                raise ValueError(f"Erreur lors de la sélection de la carrosserie de voiture : {str(e)}")

            """ Puissance de la voiture """
            await asyncio.sleep(2)
            try:
                await page.wait_for_selector("#SpecCarPower", state="visible", timeout=TIMEOUT)
                await page.select_option("#SpecCarPower", value=profile['SpecCarPowerNeuve'], strict=True)
                print(f"----> Puissance de la voiture '{profile['SpecCarPowerNeuve']}' sélectionnée avec succès.")
            except PlaywrightTimeoutError:
                print(f"Le sélecteur #SpecCarPower n'est pas visible.")
            except Exception as e:
                logging.error(f"Erreur lors de la sélection de la puissance de la voiture: {str(e)}")
                raise ValueError(f"Erreur lors de la sélection de la puissance de voiture : {str(e)}")

            """ ID de la voiture """
            try:
                # Attendre que le modal soit visible
                await page.wait_for_selector('.modal-content', state='visible', timeout=60000)

                select_vehicule = profile['ID_Veh_Neuve']

                # Essayer de cliquer sur l'ID spécifique pendant 6 secondes
                try:
                    await page.click(f'button#{select_vehicule}', timeout=6000)
                    print(
                        f"----> Le bouton avec l'ID '\033[34m{select_vehicule}\033[0m' a été cliqué avec succès pour la sélection du véhicule")
                except PlaywrightTimeoutError:
                    # Si l'ID spécifique n'est pas trouvé, cliquer sur un bouton aléatoire
                    all_buttons = await page.query_selector_all('button.al_vehicule')
                    if not all_buttons:
                        raise ValueError("Aucun bouton de véhicule trouvé dans le modal")

                    random_button = random.choice(all_buttons)
                    random_id = await random_button.get_attribute('id')
                    await random_button.click()

                    # Récupérer le nom du modèle pour l'affichage
                    model_name = await random_button.query_selector('strong')
                    model_text = await model_name.inner_text() if model_name else "Modèle inconnu"

                    print(
                        f"----> ID spécifique non trouvé. Un bouton aléatoire avec l'ID '\033[34m{random_id}\033[0m' et le modèle '\033[34m{model_text}\033[0m' a été cliqué pour la sélection du véhicule")

            except PlaywrightTimeoutError:
                print("Le div '.modal-content' n'est pas visible.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception lors de la sélection du véhicule : {str(e)}")
        else:
            """ Date d'achat prévue de la voiture """
            try:
                await page.wait_for_selector('#PurchaseDatePrev', state='visible', timeout=6000)
                await page.get_by_label("Date d'achat prévue du vé").click()
                await page.get_by_role("cell", name="Aujourd'hui").click()
                print(
                    f"----> L'option avec la valeur Aujourd'hui a été sélectionnée avec succès pour la question : La date d'achat.")
            except PlaywrightTimeoutError:
                print("Le bouton '.PurchaseDatePrev' n'est pas visible.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations de la date d'achat prévue : {str(e)}")

            """ Date d'achat de la voiture """
            try:
                await page.wait_for_selector('#PurchaseDate', state='visible', timeout=6000)
                select_PurchaseDate = profile['PurchaseDate']
                await page.type('#PurchaseDate', select_PurchaseDate, strict=True)
                await page.get_by_label("Date d'achat :").click()
                print(
                    f"----> L'option avec la valeur '\033[34m{select_PurchaseDate}\033[0m' a été sélectionnée avec succès pour la question : Date d'achat du véhicule. ")
            except PlaywrightTimeoutError:
                print("Le bouton '.PurchaseDate' n'est pas visible.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations de la date d'achat : {str(e)}")

            """ Date de mise en circulation """
            try:
                await page.wait_for_selector('#FirstCarDrivingDate', state='visible', timeout=6000)
                await page.type("#FirstCarDrivingDate", profile['FirstCarDrivingDate'], strict=True)
                await page.get_by_label("Date de 1ère mise en").click()
                await page.get_by_label("Date de 1ère mise en").press("Enter")
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['FirstCarDrivingDate']}\033[0m' a été sélectionnée avec succès pour la question : Date de mise en circulation du véhicule. ")
            except PlaywrightTimeoutError:
                print("Le bouton '.FirstCarDrivingDate' n'est pas visible.")
            except Exception as e:
                raise ValueError(
                    f"Erreur d'exception sur les informations de la date de mise en circulation du véhicule : {str(e)}")

            """ Marque de la voiture """
            success = await select_car_brand_Occasion(page, profile)
            if not success:
                print("La sélection de la marque de voiture a échoué. Gestion de l'erreur...")

            """ Modéle de la marque de voiture """
            await asyncio.sleep(2)
            try:
                await page.wait_for_selector("#SpecCarType", state="visible", timeout=2 * 60000)
                element_SpecCarType = await page.query_selector("#SpecCarType")
                await element_SpecCarType.wait_for_element_state("enabled", timeout=6000)
                await page.select_option("#SpecCarType", label=profile['SpecCarType'], timeout=60000)
                print(f"----> Modéle de la voiture '{profile['SpecCarType']}' sélectionné avec succès.")
            except PlaywrightTimeoutError:
                print("Le bouton '.SpecCarType' n'est pas visible.")
            except Exception as e:
                logging.error(f"Erreur lors de la sélection du modéle de voiture: {str(e)}")
                raise ValueError(f"Erreur lors de la sélection du modéle de voiture : {str(e)}")

            """ Type d'alimentation de la voiture """
            await asyncio.sleep(3)
            try:
                await page.wait_for_selector("#SpecCarFuelType", state="visible", timeout=TIMEOUT * 2)
                element_SpecCarFuelType = await page.query_selector("#SpecCarFuelType")
                await element_SpecCarFuelType.wait_for_element_state("enabled", timeout=60000)
                await page.select_option("#SpecCarFuelType", value=profile['SpecCarFuelType'], timeout=60000)
                print(f"----> Alimentation de la voiture '{profile['SpecCarFuelType']}' sélectionné avec succès.")
            except PlaywrightTimeoutError:
                print("Le bouton '.SpecCarFuelType' n'est pas visible.")
            except Exception as e:
                logging.error(f"Erreur lors de la sélection de Alimentation de voiture: {str(e)}")
                raise ValueError(f"Erreur lors de la sélection de Alimentation de voiture : {str(e)}")

            """ Type de carrosserie de la voiture """
            await asyncio.sleep(3)
            try:
                await page.wait_for_selector("#SpecCarBodyType", state="visible", timeout=TIMEOUT)
                element_SpecCarBodyType = await page.query_selector("#SpecCarBodyType")
                await element_SpecCarBodyType.wait_for_element_state("enabled", timeout=60000)
                await page.select_option("#SpecCarBodyType", value=profile['SpecCarBodyType'], timeout=6000)
                print(f"----> Carosserie de la voiture '{profile['SpecCarBodyType']}' sélectionnée avec succès.")
            except PlaywrightTimeoutError:
                print(f"Le sélecteur SpecCarBodyType n'est pas devenu visible dans le délai imparti.")
            except Exception as e:
                logging.error(f"Erreur lors de la sélection de la carrosserie de la voiture: {str(e)}")
                raise ValueError(f"Erreur lors de la sélection de la carrosserie de voiture : {str(e)}")

            """ Puissance de la voiture """
            await asyncio.sleep(2)
            try:
                await page.wait_for_selector("#SpecCarPower", state="visible", timeout=TIMEOUT)
                await page.select_option("#SpecCarPower", value=profile['SpecCarPower'], strict=True)
                print(f"----> Puissance de la voiture '{profile['SpecCarPower']}' sélectionnée avec succès.")
            except PlaywrightTimeoutError:
                print(f"Le sélecteur #SpecCarPower n'est pas visible.")
            except Exception as e:
                logging.error(f"Erreur lors de la sélection de la puissance de la voiture: {str(e)}")
                raise ValueError(f"Erreur lors de la sélection de la puissance de voiture : {str(e)}")

            """ ID de la voiture """
            try:
                # Attendre que le modal soit visible
                await page.wait_for_selector('.modal-content', state='visible', timeout=60000)

                select_vehicule = profile['ID_Veh_Neuve']

                # Essayer de cliquer sur l'ID spécifique pendant 6 secondes
                try:
                    await page.click(f'button#{select_vehicule}', timeout=6000)
                    print(
                        f"----> Le bouton avec l'ID '\033[34m{select_vehicule}\033[0m' a été cliqué avec succès pour la sélection du véhicule")
                except PlaywrightTimeoutError:
                    # Si l'ID spécifique n'est pas trouvé, cliquer sur un bouton aléatoire
                    all_buttons = await page.query_selector_all('button.al_vehicule')
                    if not all_buttons:
                        raise ValueError("Aucun bouton de véhicule trouvé dans le modal")

                    random_button = random.choice(all_buttons)
                    random_id = await random_button.get_attribute('id')
                    await random_button.click()

                    # Récupérer le nom du modèle pour l'affichage
                    model_name = await random_button.query_selector('strong')
                    model_text = await model_name.inner_text() if model_name else "Modèle inconnu"

                    print(
                        f"----> ID spécifique non trouvé. Un bouton aléatoire avec l'ID '\033[34m{random_id}\033[0m' et le modèle '\033[34m{model_text}\033[0m' a été cliqué pour la sélection du véhicule")

            except PlaywrightTimeoutError:
                print("Le div '.modal-content' n'est pas visible.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception lors de la sélection du véhicule : {str(e)}")
    except Exception as e:
        raise ValueError(f"Erreur d'exception sur les informations de la marque du véhicule : {str(e)}")

    """ Mode de financement """
    try:
        await page.wait_for_selector('#PurchaseMode', state='visible', timeout=60000)
        await page.select_option('#PurchaseMode', value=profile['PurchaseMode'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['PurchaseMode']}\033[0m' a été sélectionnée avec succès pour la question : Mode de financement. ")
    except PlaywrightTimeoutError:
        print("Le bouton '.PurchaseMode' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations du mode de financement {str(e)}")

    """ Usage prévu  """
    try:
        await page.wait_for_selector('#CarUsageCode', state='visible', timeout=60000)
        await page.select_option('#CarUsageCode', value=profile['CarUsageCode'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['CarUsageCode']}\033[0m' a été sélectionnée avec succès pour la question : Usage prévu . ")
    except PlaywrightTimeoutError:
        print("Le bouton '.CarUsageCode' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations de l'usage prévu {str(e)}")

    """ Kilomètres parcourus par an  """
    try:
        await page.wait_for_selector('#AvgKmNumber', state='visible', timeout=60000)
        await page.select_option('#AvgKmNumber', value=profile['AvgKmNumber'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['AvgKmNumber']}\033[0m' a été sélectionnée avec succès pour la question : Kilomètres parcourus par an. ")
    except PlaywrightTimeoutError:
        print("Le bouton '.AvgKmNumber' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations du Kilomètres parcourus par an {str(e)}")

    """ Combien de fois en moyenne utilisez-vous votre véhicule  """
    try:

        await page.wait_for_selector('#FreqCarUse', state='visible', timeout=30000)
        await page.select_option('#FreqCarUse', value=profile['FreqCarUse'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['FreqCarUse']}\033[0m' a été sélectionnée avec succès pour la question : Combien de fois en moyenne utilisez-vous votre véhicule. ")

    except PlaywrightTimeoutError:
        print("Le bouton '.FreqCarUse' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations de la fréquence d'usage {str(e)}")

    """ Code postal du lieu de stationnement la nuit  """
    try:
        await page.wait_for_selector('#HomeParkZipCode', state='visible', timeout=60000)
        await page.type('#HomeParkZipCode', profile['HomeParkZipCode'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['HomeParkZipCode']}\033[0m' a été sélectionnée avec succès pour la question : Code postal du lieu de stationnement la nuit. ")
    except PlaywrightTimeoutError:
        print("Le bouton '.HomeParkZipCode' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations du code postal du lieu de stationnement : {str(e)}")

    """ Ville du lieu de stationnement la nuit  """
    try:
        await page.wait_for_selector('#HomeParkInseeCode', state='visible', timeout=60000)
        await page.select_option('#HomeParkInseeCode', value=profile['HomeParkInseeCode'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['HomeParkInseeCode']}\033[0m' a été sélectionnée avec succès pour la question : Ville du lieu de stationnement la nuit. ")
    except PlaywrightTimeoutError:
        print("Le bouton '.HomeParkInseeCode' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations de la ville du lieu de stationnement : {str(e)}")

    """ Votre résidence principale  """
    try:
        await page.wait_for_selector('#HomeType', state='visible', timeout=60000)
        await page.select_option('#HomeType', value=profile['HomeType'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['HomeType']}\033[0m' a été sélectionnée avec succès pour la question : Votre résidence principale. ")
    except PlaywrightTimeoutError:
        print("Le bouton '.HomeType' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations de Votre résidence principale : {str(e)}")

    """ Type de location """
    try:
        await page.wait_for_selector('#HomeResidentType', state='visible', timeout=60000)
        await page.select_option('#HomeResidentType', value=profile['HomeResidentType'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['HomeResidentType']}\033[0m' a été sélectionnée avec succès pour la question : Type de location . ")
    except PlaywrightTimeoutError:
        print("Le bouton '.HomeResidentType' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations de Votre Type de location : {str(e)}")

    """ Code postal du lieu de travail  """
    try:
        await page.wait_for_selector('#JobParkZipCode', state='visible', timeout=6000)
        await page.type('#JobParkZipCode', profile['JobParkZipCode'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['JobParkZipCode']}\033[0m' a été sélectionnée avec succès pour la question : Code postal du lieu de travail. ")
    except PlaywrightTimeoutError:
        print("Le bouton '.JobParkZipCode' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations du Code postal du lieu de travail : {str(e)}")

    """ Ville du lieu de travail   """
    try:
        await page.wait_for_selector('#JobParkInseeCode', state='visible', timeout=6000)
        await page.select_option('#JobParkInseeCode', value=profile['JobParkInseeCode'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['JobParkInseeCode']}\033[0m' a été sélectionnée avec succès pour la question : Ville du lieu de travail. ")
    except PlaywrightTimeoutError:
        print("Le bouton '.JobParkInseeCode' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations de la Ville du lieu de travail : {str(e)}")

    """ Mode de parking la nuit """
    try:
        await page.wait_for_selector('#ParkingCode', state='visible', timeout=60000)
        await page.select_option('#ParkingCode', value=profile['ParkingCode'])
        print(
            f"----> L'option avec la valeur '\033[34m{profile['ParkingCode']}\033[0m' a été sélectionnée avec succès pour la question : Mode de parking la nuit. ")
    except PlaywrightTimeoutError:
        print("Le bouton '.ParkingCode' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur les informations du Mode de parking la nuit : {str(e)}")
    try:
        await asyncio.sleep(2)
        await page.get_by_role("button", name="SUIVANT ").click()
        print("Navigation vers la page suivante : Vos antécédents.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur le click du bouton suivant : {str(e)}")
    
async def fill_antecedents(page, profile):
    """
    Remplit de manière asynchrone la section des antécédents d'un formulaire d'assurance sur une page web.

    Cette fonction automatise le processus de remplissage de la section des antécédents d'un formulaire
    d'assurance, en gérant divers types d'entrées tels que les listes déroulantes et les boutons radio.
    Elle couvre un large éventail d'informations liées aux antécédents d'assurance du demandeur principal.

    Paramètres :
    ------------
    page : playwright.async_api.Page
        L'objet Page de Playwright représentant la page web où se trouve le formulaire.
    profile : dict
        Un dictionnaire contenant toutes les informations nécessaires pour remplir la section des antécédents.
        Les clés attendues incluent :
        - 'PrimaryApplicantHasBeenInsured' : Indique si le demandeur a déjà été assuré ('Y' ou 'N')
        - 'PrimaryApplicantInsuranceYearNb' : Nombre d'années d'assurance sans interruption
        - 'PrimaryApplicantIsFirstDrivOtherCar' : Indique si le demandeur est conducteur principal d'un autre véhicule
        - 'PrimaryApplicantContrCancell' : Indique si le contrat a été résilié par un assureur au cours des 3 dernières années
        - 'PrimaryApplicantBonusCoeff' : Coefficient bonus-malus actuel
        - 'PrimaryApplicantDisasterLast3year' : Nombre de sinistres déclarés au cours des 3 dernières années

    Retourne :
    ----------
    None

    Lève :
    ------
    ValueError
        Si une erreur se produit lors de la sélection des options, de la saisie des données ou de la navigation dans le formulaire.
    PlaywrightTimeoutError
        Si un sélecteur ne devient pas visible dans le délai spécifié.

    Notes :
    -------
    - La fonction vérifie d'abord si la page actuelle est bien celle des antécédents.
    - Elle gère différemment les cas où le demandeur a été assuré ou non.
    - La fonction utilise divers sélecteurs pour interagir avec les éléments du formulaire, elle est donc sensible
      aux changements dans la structure de la page.
    - Elle inclut plusieurs blocs try-except pour gérer les erreurs potentielles et fournir des retours détaillés.
    - Après avoir rempli tous les champs, elle tente de naviguer vers la page suivante.
    - Des informations de débogage sont imprimées dans la console tout au long du processus.

    Exemple :
    ---------

    """
    try:
        await page.wait_for_selector('.al_label span', state='visible', timeout=60000)
        # Récupérer le texte du span
        title_text = await page.locator('.al_label span').text_content()
        if title_text.strip() == "Vos antécédents":
            try:
                await page.wait_for_selector("select[name='PrimaryApplicantHasBeenInsured']", state='visible', timeout=2 * 60000)
                if profile['PrimaryApplicantHasBeenInsured'] == "N":
                    await page.select_option("#PrimaryApplicantHasBeenInsured", value=profile['PrimaryApplicantHasBeenInsured'])
                    print(f"Option sélectionnée pour la question initiale : {profile['PrimaryApplicantHasBeenInsured']}")
                    print("Réponse 'Non' sélectionnée. Pas d'autres champs à remplir.")
                else:
                    try:
                        await page.wait_for_selector("select[name='PrimaryApplicantHasBeenInsured']", state='visible',
                                                     timeout=2 * 60000)
                        await page.select_option("#PrimaryApplicantHasBeenInsured", value=profile['PrimaryApplicantHasBeenInsured'])
                        await page.wait_for_selector('#PrimaryApplicantInsuranceYearNb', state='visible', timeout=60000)
                        await page.select_option('#PrimaryApplicantInsuranceYearNb', value=profile['PrimaryApplicantInsuranceYearNb'])
                        print(
                            f"----> L'option avec la valeur '\033[34m{profile['PrimaryApplicantInsuranceYearNb']}\033[0m' a été sélectionnée avec succès pour la question : Assuré sans interruption depuis ?. ")
                    except PlaywrightTimeoutError:
                        print("Le bouton '.PrimaryApplicantInsuranceYearNb' n'est pas visible.")
                    except Exception as e:
                        raise ValueError(
                            f"Erreur d'exception sur les informations des antécédents de l'assuré : {str(e)}")

                    """ Etes-vous désigné conducteur principal d'un autre véhicule et assuré à ce titre ? """
                    try:
                        await page.wait_for_selector('.PrimaryApplicantIsFirstDrivOtherCar', state='visible', timeout=60000)
                        if profile['PrimaryApplicantIsFirstDrivOtherCar'] == "Oui":
                            await page.click('div.PrimaryApplicantIsFirstDrivOtherCar button[value="True"]')
                        elif profile['PrimaryApplicantIsFirstDrivOtherCar'] == "Non":
                            await page.click('div.PrimaryApplicantIsFirstDrivOtherCar button[value="False"]')
                        else:
                            raise ValueError("Erreur sur la valeur prise par PrimaryApplicantIsFirstDrivOtherCar")
                        print(
                            f"----> L'option avec la valeur '\033[34m{profile['PrimaryApplicantIsFirstDrivOtherCar']}\033[0m' a été sélectionnée avec succès pour la question : Etes-vous désigné conducteur principal d'un autre véhicule et assuré à ce titre ?.")
                    except PlaywrightTimeoutError:
                        print("L'élément '.PrimaryApplicantIsFirstDrivOtherCar' n'est pas visible, passage au champ suivant.")
                    except Exception as e:
                        raise ValueError(f"Erreur d'exception sur les informations des antécédents du conducteur : {str(e)}")

                    """ Avez-vous fait l'objet d'une résiliation par un assureur au cours des 3 dernières années ? """
                    try:
                        await page.wait_for_selector('#PrimaryApplicantContrCancell', state='visible', timeout=60000)
                        await page.select_option('#PrimaryApplicantContrCancell', value=profile['PrimaryApplicantContrCancell'])
                        print(
                            f"----> L'option avec la valeur '\033[34m{profile['PrimaryApplicantContrCancell']}\033[0m' a été sélectionnée avec succès pour la question : Avez-vous fait l'objet d'une résiliation par un assureur au cours des 3 dernières années ? . ")
                    except PlaywrightTimeoutError:
                        print("Le bouton '.PrimaryApplicantContrCancell' n'est pas visible.")
                    except Exception as e:
                        raise ValueError(
                            f"Erreur d'exception sur les informations des antécédents de l'assuré : {str(e)}")

                    """ Quel est votre bonus-malus auto actuel ? """
                    try:
                        await page.wait_for_selector('#PrimaryApplicantBonusCoeff', state='visible', timeout=60000)
                        await page.select_option('#PrimaryApplicantBonusCoeff', value=profile['PrimaryApplicantBonusCoeff'],
                                                 strict=True)
                        print(
                            f"----> L'option avec la valeur '\033[34m{profile['PrimaryApplicantBonusCoeff']}\033[0m' a été sélectionnée avec succès pour la question : Quel est votre bonus-malus auto actuel ?. ")
                    except PlaywrightTimeoutError:
                        print("Le bouton '.PrimaryApplicantBonusCoeff' n'est pas visible.")
                    except Exception as e:
                        raise ValueError(
                            f"Erreur d'exception sur les informations des antécédents de l'assuré : {str(e)}")

                    """ Combien de sinistres avez-vous déclaré (y compris bris de glace) ? """
                    try:
                        await page.wait_for_selector('#PrimaryApplicantDisasterLast3year', state='visible', timeout=60000)
                        await page.select_option('#PrimaryApplicantDisasterLast3year',
                                                 value=profile['PrimaryApplicantDisasterLast3year'])
                        print(
                            f"----> L'option avec la valeur '\033[34m{profile['PrimaryApplicantDisasterLast3year']}\033[0m' a été sélectionnée avec succès pour la question : Combien de sinistres avez-vous déclaré (y compris bris de glace) ? . ")
                    except PlaywrightTimeoutError:
                        print("Le bouton '.PrimaryApplicantDisasterLast3year' n'est pas visible.")
                    except Exception as e:
                        raise ValueError(
                            f"Erreur d'exception sur les informations des antécédents de l'assuré : {str(e)}")
            except PlaywrightTimeoutError:
                print("Le bouton '.PrimaryApplicantHasBeenInsured' n'est pas visible.")
            except Exception as e:
                raise ValueError(
                    f"Erreur d'exception sur les informations des antécédents de l'assuré : {str(e)}")
            try:
                await asyncio.sleep(2)
                await page.get_by_role("button", name="SUIVANT ").click()
                print("Navigation vers la page suivante : Votre contrat.")
            except Exception as e:
                raise ValueError(
                    f"Erreur d'exception sur le click du bouton suivant : {str(e)}")
        else:
            print(f"Le titre trouvé est '{title_text}', ce qui ne correspond pas à 'Vos antécédents'.")

    except PlaywrightTimeoutError:
        print("Le bouton '..al_label span' n'est pas visible.")
    except Exception as e:
        raise ValueError(
            f"Erreur d'exception sur l'affichage de la page des antécédents: {str(e)}")

async def fill_form_contrats(page, profile):
    """
    Remplit de manière asynchrone un formulaire de contrat sur une page web en utilisant les informations de profil fournies.

    Cette fonction parcourt divers champs du formulaire, sélectionne des options et saisit des données
    en fonction du profil donné. Elle gère différents types d'éléments de formulaire tels que les listes déroulantes,
    les boutons, les champs de texte et les cases à cocher.

    Args:
        page (Page): Un objet Page de Playwright représentant la page web.
        profile (dict): Un dictionnaire contenant les informations de profil de l'utilisateur pour remplir le formulaire.

    Lève:
        ValueError: S'il y a une erreur lors du remplissage du formulaire ou si les éléments attendus ne sont pas trouvés.

    Retourne:
        None

    Note:
        Cette fonction suppose que la page est déjà chargée et visible.
        Elle utilise divers sélecteurs pour interagir avec les éléments de la page, donc tout changement dans la structure de la page
        pourrait nécessiter des mises à jour des sélecteurs utilisés dans cette fonction.
    """
    try:
        # Attendre que le titre du formulaire soit visible
        await page.wait_for_selector('.al_label span', state='visible', timeout=60000)
        title_text = await page.locator('.al_label span').text_content()

        if title_text.strip() == "Votre contrat":
            """Remplit le champ du type de résidence."""
            try:
                await page.wait_for_selector('#PrimaryApplicantHomeAddressType', state='visible', timeout=6000)
                await page.select_option('#PrimaryApplicantHomeAddressType', value=profile['PrimaryApplicantHomeAddressType'])
                print(f"----> Type de résidence sélectionné : {profile['PrimaryApplicantHomeAddressType']}")
            except PlaywrightTimeoutError:
                print("Le champ 'PrimaryApplicantHomeAddressType' n'est pas visible.")
            except Exception as e:
                raise ValueError(f"Erreur lors du remplissage du type de résidence : {str(e)}")
            """Remplit le champ de la durée de possession du véhicule."""
            try:
                await page.wait_for_selector('#CarOwningTime', state='visible', timeout=6000)
                await page.select_option('#CarOwningTime', value=profile['CarOwningTime'])
                print(f"----> Durée de possession du véhicule : {profile['CarOwningTime']}")
            except PlaywrightTimeoutError:
                print("Le champ 'CarOwningTime' n'est pas visible.")
            except Exception as e:
                raise ValueError(f"Erreur lors du remplissage de la durée de possession du véhicule : {str(e)}")
            
            """ Comment votre véhicule actuel est-il assuré ? """
            try:
                await page.wait_for_selector('.CurrentGuaranteeCode', state='visible', timeout=6000)
                if profile['CurrentGuaranteeCode'] == "A":
                    await page.click('div.CurrentGuaranteeCode button[value="A"]')
                elif profile['CurrentGuaranteeCode'] == "E":
                    await page.click('div.CurrentGuaranteeCode button[value="E"]')
                elif profile['CurrentGuaranteeCode'] == "N":
                    await page.click('div.CurrentGuaranteeCode button[value="N"]')
                else:
                    raise ValueError("Erreur sur la valeur prise par CurrentGuaranteeCode")
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['CurrentGuaranteeCode']}\033[0m' a été sélectionnée avec succès pour la question : Comment votre véhicule actuel est-il assuré ?.")
            except PlaywrightTimeoutError:
                print("L'élément '.CurrentGuaranteeCode' n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(
                    f"Erreur d'exception sur les informations du contrat : {str(e)}")
            """ Quel est votre dernier assureur auto ? """
            try:
                await page.wait_for_selector('#CurrentCarrier', state='visible', timeout=6000)
                await page.select_option('#CurrentCarrier', value=profile['CurrentCarrier'])
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['CurrentCarrier']}\033[0m' a été sélectionnée avec succès pour la question : Quel est votre dernier assureur auto ?. ")
            except PlaywrightTimeoutError:
                print("Le bouton '.CurrentCarrier' n'est pas visible.")
            except Exception as e:
                raise ValueError(
                    f"Erreur d'exception sur les informations de l'assureur actuel : {str(e)}")
            
            """ Quel est le mois d'échéance de votre contrat actuel ? """
            try:
                await page.wait_for_selector('#ContractAnniverMth', state='visible', timeout=6000)
                await page.wait_for_selector('#ContractAnniverMth', state='attached', timeout=10000)
                await page.select_option('#ContractAnniverMth', value=profile['ContractAnniverMth'])
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['ContractAnniverMth']}\033[0m' a été sélectionnée avec succès pour la question : Quel est le mois d'échéance de votre contrat actuel ?. ")
            except PlaywrightTimeoutError:
                print("Le bouton '.ContractAnniverMth' n'est pas visible.")
            except Exception as e:
                raise ValueError(
                    f"Erreur d'exception sur les informations du contrat : {str(e)}")
            
            """ A quelle date souhaitez-vous que votre nouveau contrat débute ? """
            try:
                await page.wait_for_selector('#EffectiveDate', state='visible', timeout=6000)
                await page.locator(".input-group-addon > .fal").click()
                await page.get_by_role("cell", name="Aujourd'hui").click()
                print(
                    f"----> L'option avec la valeur 'Aujourd'hui' a été sélectionnée avec succès pour la question : A quelle date souhaitez-vous que votre nouveau contrat débute ?.")
            except PlaywrightTimeoutError:
                print("Le bouton '.EffectiveDate' n'est pas visible.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations du contrat : {str(e)}")
            
            """ Quel niveau de protection voulez-vous ? """
            try:
                await page.wait_for_selector('.ContrGuaranteeCode', state='visible', timeout=600)
                if profile['ContrGuaranteeCode'] == "A":
                    await page.click('div.ContrGuaranteeCode button.list-group-item[value="A"]')
                elif profile['ContrGuaranteeCode'] == "E":
                    await page.click('div.ContrGuaranteeCode button.list-group-item[value="E"]')
                elif profile['ContrGuaranteeCode'] == "C":
                    await page.click('div.ContrGuaranteeCode button.list-group-item[value="C"]')
                elif profile['ContrGuaranteeCode'] == "D":
                    await page.click('div.ContrGuaranteeCode button.list-group-item[value="D"]')
                else:
                    raise ValueError("Erreur sur la valeur prise par ContrGuaranteeCode")
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['ContrGuaranteeCode']}\033[0m' a été sélectionnée avec succès pour la question : Quel niveau de protection voulez-vous ?.")
            except PlaywrightTimeoutError:
                print("L'élément '.ContrGuaranteeCode' n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(
                    f"Erreur d'exception sur les informations du contrat : {str(e)}")

            try:
                await page.wait_for_selector('.UserOptIn', state='visible', timeout=6000)
                if profile['UserOptIn'] == '1':
                    await page.click('div.UserOptIn button.list-group-item[value="1"]')
                else:
                    await page.click('div.UserOptIn button.list-group-item[value="0"]')
            except PlaywrightTimeoutError:
                print("Le bouton '.UserOptIn' n'est pas visible.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur l'acceptation de recevoir des offres : {str(e)}")
            
            try:
                """ Les coordonnées de l'assuré """
                await page.wait_for_selector('#TitleAddress', state='visible', timeout=6000)
                if profile['TitleAddress'] == 'MONSIEUR':
                    await page.select_option('#TitleAddress', value="MONSIEUR")
                elif profile['TitleAddress'] == 'MADAME':
                    await page.select_option('#TitleAddress', value="MADAME")
                else:
                    raise ValueError("Erreur sur la valeur prise par TitleAddress ")
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['TitleAddress']}\033[0m' a été sélectionnée avec succès pour la civilité ")
            except PlaywrightTimeoutError:
                print("L'élément '.TitleAddress' n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(
                    f"Erreur d'exception sur les informations du contrat : {str(e)}")

            try:
                await page.wait_for_selector('#LastName', state='visible', timeout=6000)
                await page.type('#LastName', profile['LastName'])
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['LastName']}\033[0m' a été sélectionnée avec succès pour le Nom.")
            except PlaywrightTimeoutError:
                print("L'élément '.LastName' n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations du contrat : {str(e)}")

            try:
                await page.wait_for_selector('#FirstName', state='visible', timeout=6000)
                await page.type('#FirstName', profile['FirstName'])
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['FirstName']}\033[0m' a été sélectionnée avec succès pour le Prénom.")
            except PlaywrightTimeoutError:
                print("L'élément '.FirstName' n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations du contrat : {str(e)}")
            
            try:
                await page.wait_for_selector('#Address', state='visible', timeout=6000)
                await page.type('#Address', profile['Address'])
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['Address']}\033[0m' a été sélectionnée avec succès pour l'adresse.")
            except PlaywrightTimeoutError:
                print("L'élément '.Address' n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations du contrat : {str(e)}")

            try:
                await page.wait_for_selector('#ZipCode', state='visible', timeout=6000)
                for char in profile['HomeParkZipCode']:
                    await page.type('#ZipCode', char)
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['HomeParkZipCode']}\033[0m' a été sélectionnée avec succès pour le Code Postal.")
            except PlaywrightTimeoutError:
                print("L'élément '.ZipCode' n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations du contrat : {str(e)}")

            try:
                await page.wait_for_selector('input#Email', state='visible', timeout=6000)
                for char in profile['Email']:
                    await page.type('input#Email', char)
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['Email']}\033[0m' a été sélectionnée avec succès pour le Mail.")
            except PlaywrightTimeoutError:
                print("L'élément '.Email' n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations du contrat : {str(e)}")
            
            try:
                await page.wait_for_selector('input#Phone', state='visible', timeout=6000)
                await page.fill('input#Phone', profile['Phone'])
                print(
                    f"----> L'option avec la valeur '\033[34m{profile['Phone']}\033[0m' a été sélectionnée avec succès pour le Téléphone")
            except PlaywrightTimeoutError:
                print("L'élément '.Phone' n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception sur les informations du contrat : {str(e)}")
            
            try:
                await page.wait_for_selector('.col-xs-12.no-gutter.text-center', state='visible', timeout=6000)
                if profile['Id'] != '0000':
                    await page.click('text="Oui, je la conserve"')
                else:
                    await page.click('text="Non, je la modifie"')
                print("==> Choix effectué avec succès.")
            except PlaywrightTimeoutError:
                print("Les boutons ne sont pas visibles, passage au champ suivant.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception : {str(e)}")
            
            await page.wait_for_selector('#LegalCGU', state='visible', timeout=6000)
            # Cocher la case en cliquant dessus
            await page.check('#LegalCGU')
            print(f"'\033[34m ==> Case LegalCGU cochée avec succès.\033[0m")

            try:
                # Attendre que la case à cocher soit visible
                await page.wait_for_selector('#LegalRGPD', state='visible', timeout=6000)
                # Cocher la case en cliquant dessus
                await page.check('#LegalRGPD')
                print(f"'\033[34m ==> Case LegalRGPD cochée avec succès.\033[0m")
            except PlaywrightTimeoutError:
                print("La case à cocher n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception : {str(e)}")

            try:
                # Attendre que la case à cocher soit visible
                await page.wait_for_selector('#LegalPartner', state='visible', timeout=6000)
                # Cocher la case
                await page.check('#LegalPartner')
                print(f"'\033[34m ==> Case LegalPartner cochée avec succès. \033[0m")

                await page.get_by_label("J'accepte les conditions géné").check()
                print(f"'\033[34m ===> J'accepte les condions générales \033[0m")
                await page.get_by_label(
                    "Je reconnais avoir reçu les informations relatives à la collecte, le traitement").check()
                print(
                    f"'\033[34m ===> Je reconnais voir reçu les informations relatives à la collecte, le traitement \033[0m")
                await page.get_by_label("J'accepte d'être contacté au").check()
                print(f"'\033[34m ===> J'accepte d'être contacté \033[0m")
            except PlaywrightTimeoutError:
                print("La case à cocher n'est pas visible, passage au champ suivant.")
            except Exception as e:
                raise ValueError(f"Erreur d'exception : {str(e)}")
        else:
            print(f"Le titre trouvé est '{title_text}', ce qui ne correspond pas à 'Vos antécédents'.")
    except Exception as e:
        raise ValueError(f"Erreur lors du remplissage du contrat : {str(e)}")
    

    

async def recup_tarifs(page, profile):
    """
    Récupère les tarifs d'assurance pour un profil donné et les enregistre dans un fichier CSV.

    Cette fonction effectue les opérations suivantes :
    1. Clique sur le bouton pour accéder aux devis.
    2. Gère la résolution du captcha.
    3. Scrape le contenu de la page pour récupérer les offres d'assurance.
    4. Enregistre les offres dans un fichier CSV.
    5. En cas d'absence d'offres, enregistre l'ID du profil dans un fichier CSV séparé.

    Args:
        page (Page): L'objet Page de Playwright représentant la page web actuelle.
        profile (dict): Un dictionnaire contenant les informations du profil d'assurance.

    Returns:
        None

    Raises:
        PlaywrightTimeoutError: Si le div '.al_form' n'est pas visible dans le délai imparti.
        ValueError: Pour toute autre erreur survenant pendant l'exécution de la fonction.

    Note:
        - Les variables globales 'start_line' et 'end_line' sont utilisées pour nommer les fichiers de sortie.
        - Les fichiers CSV sont créés avec la date du jour dans leur nom.
        - Si un fichier CSV existe déjà, les nouvelles données sont ajoutées à la fin sans réécrire l'en-tête.
    """
    try:
        await asyncio.sleep(random.uniform(1, 3))
        await page.get_by_role("button", name="ACCÉDEZ À VOS DEVIS ").click()

        client = await page.context.new_cdp_session(page)
        print('Waiting captcha to solve...')
        solve_res = await client.send('Captcha.waitForSolve', {
            'detectTimeout': 10000,
        })
        print('Captcha solve status:', solve_res['status'])

        print('Navigated! Scraping page content...')
        print(f"'\033[34m ============== ACCÉDEZ À VOS DEVIS pour le profil avec l'identifiant {profile['Id']}....\033[0m")
        await page.wait_for_load_state('load', timeout=60000)
        await page.wait_for_selector('.al_form .al_content .container-fluid', state='visible', timeout=5 * 60000)

        offres = await page.query_selector_all('.al_content .container-fluid')
        profile_details = []

        for offre in offres:
            element_assureur = await offre.query_selector('.al_carrier')
            assureur = await element_assureur.inner_text()

            element_prime = await offre.query_selector('.al_premium')
            prime = await element_prime.inner_text()

            profile_details.append({
                'Compagnie': assureur,
                'Prime': prime,
                'ID': profile['Id'],
                'TypeBesoin': profile['InsuranceNeed'],
                'TypeBesoinDetails': profile['InsuranceNeedDetail'],
                'AgeCar': profile['AddCarAge'],
                'OtherDriver': profile['OtherDriver'],
                'CarteGrise': profile['GreyCardOwner'],
                'Genre': profile['PrimaryApplicantSex'],
                'DateNaissance': profile['PrimaryApplicantBirthDate'],
                'Age': profile['Age'],
                'SituationMatrimoniale': profile['PrimaryApplicantMaritalStatus'],
                'Profession': profile['PrimaryApplicantOccupationCode'],
                'DateObtentionPermis': profile['PrimaryApplicantDrivLicenseDate'],
                'ConduiteAccompagné': profile['PrimaryApplicantIsPreLicenseExper'],
                'DateNaissanceConjoint': profile['ConjointNonSouscripteurBirthDate'],
                'DatePermisConjoint': profile['ConjointNonSouscripteurDriveLicenseDate'],
                'Enfants_a_charge': profile['HasChild'],
                'Annee_Enfant_1': profile['ChildBirthDateYear1'],
                'Annee_Enfant_2': profile['ChildBirthDateYear2'],
                'Annee_Enfant_3': profile['ChildBirthDateYear3'],
                'DateAchat': profile['PurchaseDate'],
                'DateAchat_Prevue': profile['PurchaseDatePrev'],
                'DateCirculation': profile['FirstCarDrivingDate'],
                'ID_vehicule_occasion': profile["ID_Veh"],
                'Marque_occasion': profile['SpecCarMakeName'],
                'Modele_occasion': profile['SpecCarType'],
                'Alimentation_occasion': profile['SpecCarFuelType'],
                'Carrosserie_occasion': profile['SpecCarBodyType'],
                'Puissance_occasion': profile['SpecCarPower'],
                'categorie_commerciale_vehicule_occasion': profile['categorie_commerciale_vehicule'],
                'qualification_vehicule_vert_occasion': profile['qualification_vehicule_vert'],
                'valeur_a_neuf_vehicule_ocassion': profile['valeur_a_neuf_vehicule'],
                'groupe_tarification_vehicule_occasion': profile['groupe_tarification_vehicule'],
                'classe_tarification_vehicule_occasion': profile['classe_tarification_vehicule'],
                'code_type_frequence_bdg_occasion': profile['code_type_frequence_bdg'],
                'code_type_cout_bdg_occasion': profile['code_type_cout_bdg'],
                'code_vente_vehicule_occasion': profile['code_vente_vehicule'],
                'code_type_frequence_rcm_occasion': profile['code_type_frequence_rcm'],
                'code_type_frequence_rcc_occasion': profile['code_type_frequence_rcc'],
                'code_type_frequence_dta_occasion': profile['code_type_frequence_dta'],
                'code_type_frequence_vol_occasion': profile['code_type_frequence_vol'],


                'Marque_vehicule_neuf': profile['SpecCarMakeNameNeuve'],
                'Modele_vehicule_neuf': profile['SpecCarTypeNeuve'],
                'Alimentation_vehicule_neuf': profile['SpecCarFuelTypeNeuve'],
                'Carrosserie_vehicule_neuf': profile['SpecCarBodyTypeNeuve'],
                'Puissance_vehicule_neuf': profile['SpecCarPowerNeuve'],
                'ID_vehicule_neuf': profile["ID_Veh_Neuve"],
                'valeur_a_neuf_vehiculeNeuve': profile['valeur_a_neuf_vehicule_Neuve'],
                'groupe_tarification_vehicule_Neuve': profile['groupe_tarification_vehicule_Neuve'],
                'classe_tarification_vehicule_Neuve': profile['classe_tarification_vehicule_Neuve'],
                'puissance_reel_vehicule_Neuve': profile['puissance_reel_vehicule_Neuve'],
                'categorie_commerciale_vehicule_Neuve': profile['categorie_commerciale_vehicule_Neuve'],
                'code_type_frequence_rcm_Neuve': profile['code_type_frequence_rcm_Neuve'],
                'code_type_frequence_rcc_Neuve': profile['code_type_frequence_rcc_Neuve'],
                'code_type_cout_rcm_Neuve': profile['code_type_cout_rcm_Neuve'],
                'code_type_frequence_dta_Neuve': profile['code_type_frequence_dta_Neuve'],
                'qualification_vehicule_vert_Neuve': profile['qualification_vehicule_vert_Neuve'],
                'code_type_frequence_vol_Neuve': profile['code_type_frequence_vol_Neuve'],
                'code_type_cout_vol_Neuve': profile['code_type_cout_vol_Neuve'],
                'code_type_frequence_bdg_Neuve': profile['code_type_frequence_bdg_Neuve'],
                'code_type_cout_bdg_Neuve': profile['code_type_cout_bdg_Neuve'],
                'code_vente_vehicule_Neuve': profile['code_vente_vehicule_Neuve'],
                'ModeFinancement': profile['PurchaseMode'],
                'Usage': profile['CarUsageCode'],
                'KmParcours': profile['AvgKmNumber'],
                'CP_Stationnement': profile['HomeParkZipCode'],
                'Ville_Stationnement': profile['HomeParkInseeCode'],
                'ResidenceType': profile['HomeType'],
                'TypeLocation': profile['HomeResidentType'],
                'CP_Travail': profile['JobParkZipCode'],
                'Ville_Travail': profile['JobParkInseeCode'],
                'nom_departement': profile['nom_departement'],
                'code_departement': profile['code_departement'],
                'code_region': profile['code_region'],
                'Type_Parking': profile['ParkingCode'],
                'TypeAssure': profile['PrimaryApplicantHasBeenInsured'],
                'NbreAnneeAssure': profile['PrimaryApplicantInsuranceYearNb'],
                'Bonus': profile['PrimaryApplicantBonusCoeff'],
                'NbreAnneePossessionVeh': profile['CarOwningTime'],
                'CtrActuel': profile['CurrentGuaranteeCode'],
                'AssureurActuel': profile['CurrentCarrier'],
                'NiveauProtection': profile['ContrGuaranteeCode'],
                'Date_scraping': profile['DateScraping']
            })

        if profile_details:
            date_du_jour = datetime.now().strftime("%d_%m_%y")
            nom_fichier_csv = f"offres_{date_du_jour}_{start_line}_au_{end_line}.csv"
            
            # Écrire les offres dans le fichier CSV
            async with aiofiles.open(nom_fichier_csv, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=profile_details[0].keys())
                
                # Écrire l'en-tête si le fichier est vide
                file_empty = await f.tell() == 0
                if file_empty:
                    await writer.writeheader()
                
                # Écrire les lignes de données
                for row in profile_details:
                    await writer.writerow(row)
                
                print(f"=====> Les offres du profil {profile['Id']} ont été stockées dans le fichier:", nom_fichier_csv)
        else:
            date_du_jour = datetime.now().strftime("%d_%m_%y")
            nom_fichier_sans_tarif = f"fichiers_ST_{date_du_jour}_{start_line}_au_{end_line}.csv"
            
            # Écrire les informations du profil dans le fichier CSV des échecs
            async with aiofiles.open(nom_fichier_sans_tarif, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['ID'])
                
                # Écrire l'en-tête si le fichier est vide
                file_empty = await f.tell() == 0
                if file_empty:
                    await writer.writeheader()
                
                await writer.writerow({'ID': profile['Id']})
                print(f"=====> Le profil {profile['Id']} a été stocké dans le fichier des échecs:", nom_fichier_sans_tarif)

    except PlaywrightTimeoutError:
        print("Le div '.al_form' n'est pas visible, passage au champ suivant.")
    except Exception as e:
        raise ValueError(f"Erreur d'exception sur la récupération des offres : {str(e)}")




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
        await page.wait_for_load_state("networkidle")
        logger.info("=" * 100)
        await fill_form_profil(page, profile)
        await page.wait_for_load_state("load")
        logger.info("=" * 100)
        await fill_form_vehicule(page, profile)
        await page.wait_for_load_state("networkidle")
        logger.info("=" * 100)
        await fill_antecedents(page, profile)
        await page.wait_for_load_state("networkidle")
        logger.info("=" * 100)
        await fill_form_contrats(page, profile)
        logger.info("=" * 100)
        await recup_tarifs(page, profile)
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
