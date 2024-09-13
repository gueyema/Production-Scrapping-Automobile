import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker
from dateutil.relativedelta import relativedelta


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
    Génère une date d'achat aléatoire entre 1 et 365 jours après la date de mise en circulation.

    Args:
        first_car_driving_date (datetime): Date de mise en circulation du véhicule.

    Returns:
        str: Date d'achat au format 'mm/YYYY'.
    """
    jours_apres = random.randint(1, 365)
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
    age_minimum_permis = 17
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
            age_minimum_permis_conjoint = 17
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
            annees_possible = range(1998, 2025)

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
        'Id': random.randint(5, 10),  # Id aléatoire entre 5 et 10
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
for _ in range(100000):  # Par exemple, générer 1000 profils
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
for profil in profils[:10]:

    print(profil)

print(profils_df.columns)
