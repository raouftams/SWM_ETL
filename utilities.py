import requests
from dateutil.parser import parse
import petl as etl
import time
from table.TownTable import TownTable
from hijri_converter import Hijri, Gregorian

from table.VehicleTable import VehicleTable

#Rotation data column names
rotations_table_header = ["vehicle_mat", "vehicle_id", "town", "town_code", "date", "date_hijri", "time", "net_extra", "net_cet", "tare", "brute", "ticket", "cet"]

#dict of column names and renaming rules
rotations_table_renaming = {
                    "véhicule": "vehicle_mat",
                    "matricule": "vehicle_mat",
                    "mat": "vehicle_mat",
                    "identifiant": "vehicle_id",
                    "t/ véh": "tare",
                    "t/ véhi": "tare",
                    "tare véhicule": "tare",
                    "t/véhicule": "tare",
                    "tare véh": "tare",
                    "tare (kg)": "tare",
                    "commune": "town",
                    "commune d'intervention": "town",
                    "codec": "town_code",
                    "code c": "town_code",
                    "code commune": "town_code",
                    "nom client": "town",
                    "heure": "time",
                    "heure 1": "time",
                    "h/ 1": "time",
                    "n ticket": "ticket", 
                    "n tick": "ticket",
                    "n/ ticket": "ticket",
                    "numéro ticket": "ticket",
                    "bon": "ticket",
                    "n° bon": "ticket",
                    "net cet": "net_cet",
                    'poids net': 'net_cet',
                    "p/ net": "net_cet",
                    'p/net': 'net_cet',
                    "net cet (kg)": "net_cet",
                    "net extra (kg)": "net_extra",
                    "net extr": "net_extra",
                    "net extra": "net_extra",
                    "extra net": "net_extra",
                    "net extranet": "net_extra",
                    "poids extra": "net_extra",
                    "pesée 1 (kg)": "brute",
                    "p/ 1 (kg)": "brute",
                    "brut": "brute",
                    "brut22": "brute",
                    "brut (kg)": "brute",
                    "datehijri": "date_hijri",
}

#dict of column names and types
rotations_table_types = {
        "vehicle_mat" : str,
        "vehicle_id" : str,
        "town_code" : str,
        "town": str,
        "date": str,
        "date_hijri": str,
        "time" : str,
        "ticket" : str,
        "net_cet": float,
        "net_extra": float,
        "tare": float,
        "brute": float,
        "cet": str
    }

#Algiers town names
towns_list = [
     "Alger-Centre",
     "Sidi M'Hamed", 
     "El Madania", 
     "Belouizdad", 
     "Bab El Oued", 
     "Bologhine", 
     "Casbah", 
     "Oued Koriche", 
     "Bir Mourad Raïs", 
     "El Biar", 
     "Bouzareah", 
     "Birkhadem", 
     "El Harrach", 
     "Baraki", 
     "Oued Smar", 
     "Bachdjerrah", 
     "Hussein Dey", 
     "Kouba", 
     "Bourouba", 
     "Dar El Beïda", 
     "Bab Ezzouar", 
     "Ben Aknoun", 
     "Dely Ibrahim", 
     "El Hammamet", 
     "Raïs Hamidou", 
     "Djasr Kasentina", 
     "El Mouradia", 
     "Hydra", 
     "Mohammadia", 
     "Bordj El Kiffan", 
     "El Magharia", 
     "Beni Messous",
     "Les Eucalyptus", 
     "Birtouta", 
     "Tessala El Merdja", 
     "Ouled Chebel", 
     "Sidi Moussa", 
     "Aïn Taya", 
     "Bordj El Bahri", 
     "El Marsa", 
     "Heuraoua", 
     "Rouïba", 
     "Reghaïa", 
     "Aïn Benian", 
     "Staoueli", 
     "Zeralda", 
     "Mahelma", 
     "Rahmania", 
     "Souidania", 
     "Cheraga", 
     "Ouled Fayet", 
     "El Achour",
     "El Hamiz", 
     "Draria",
     "Douera",
     "Baba Hassen",
     "Khraicia",
     "Saoula"
]

#some town names abbreviations
towns_abbreviations = {
    "bez": "Bab Ezzouar",
    "bek": "Bordj El Kiffan",
    "deb": "Dar El Beida",
    "beb": "Bordj El Bahri",
}

#stop words for towns names
town_stop_words = [
    "Apc",
    "Extranet",
    "netcom",
    "net com",
    "EPIC",
    "N IDENTIFIE",
    "ABS CET",
    "CET",
    "ABS",
    "OPERATION AUTOROUTE"
]

#vehicles data columns names
vehicles_table_header = ["code", "ancien_matricule", "nouveau_matricule", "num_chassie", "tare", "capacite", "marque", "genre", "volume", "puissance", "mise_en_marche", "code_commune"]

#dict of column names and renaming rules
vehicles_table_renaming = {
    "identifiant": "code",
    "ancien matricule": "ancien_matricule",
    "nouveau matricule": "nouveau_matricule",
    "matricule": "nouveau_matricule",
    "n° chassie": "num_chassie",
    "codec": "code_commune",
    "code c": "code_commune",
    "année mise en marche": "mise_en_marche",
    "volume (m3)": "volume",
}

#dict of column names and types
vehicles_table_types = {
    "code": str,
    "ancien_matricule": str,
    "nouveau_matricule": str,
    "num_chassie": str,
    "tare": float,
    "capacite": float,
    "marque": str,
    "genre": str,
    "volume": float,
    "puissance": float,
    "mise_en_marche": str,
    "code_commune": str
}


#get hijri date from a date
def gregorian_to_hijri_date(date: str):
    """
    params: date in format "dd-mm-yyyy"
    return: corresponding hijri date in format "dd-mm-yyyy" and array of islamic holidays if they exist
    """
    format_date(date)

    #get the corresponding hijri date
    #url = "https://api.aladhan.com/v1/gToH?date={}".format(date)
    #r = requests.get(url)
    #data = r.json()["data"]
    #return data["hijri"]["date"]

    return Gregorian.fromisoformat(date).to_hijri().isoformat()


#get gregorian date from a hijri date
def hijri_to_gregorian_date(date: str):
    """
    params: date in format "dd-mm-yyyy"
    return: corresponding gregorian date in format "dd-mm-yyyy" 
    """
    date = format_date(date)

    #get the corresponding gregorian date
    #url = "https://api.aladhan.com/v1/hToG?date={}".format(date)
    #r = requests.get(url)
    #data = r.json()["data"]
    #return str(data["gregorian"]["date"])
    
    return Hijri.fromisoformat(date).to_gregorian().isoformat()


#check if a string has any format of a date
def is_date(string, fuzzy=False):

    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    if string == None:
        return False
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

#transform date to "yyyy-mm-dd" format
def format_date(date: str):
    #if date is in format "dd-mm-yyyy hh:mm:ss" we remove the time
    date = date.split(" ")[0]
    #if date in in format "dd/mm/yyyy"
    if not "-" in date:
        date = date.replace("/", "-")
    
    #if date is in format "dd-mm-yyyy" we convert it to "yyyy-mm-dd"
    y, m, d = date.split("-")
    if int(y) < 100:
        date = d + "-" + m + "-" + y

    return date

#check if a string is time format
def is_time(input):
    if input == None or type(input) != str:
        return False
    try:
        time.strptime(input, '%H:%M:%S')
        return True
    except ValueError:
        return False

#Concatenate two petl tables with a fixed set of fields 
def concat_table(table_a, table_b, header):
    """
    Arguments
    table_a: a petl table
    table_b: a petl table
    header: a list of column names
    Purpose
    This function concatenates two petl tables and returns a new petl table
    """
    if table_a != []:
        return etl.cat(table_a, table_b, header=header)
    return table_b

#get towns registred in database
def get_registred_towns_data(db_connection):
    """
    return: an array with all towns registred in database
    """
    instance = TownTable()
    data = instance.get_all(db_connection)
    town_names = []
    town_codes = []
    for r in data:
        town_names.append(r[1]) 
        town_codes.append(r[0])
    
    return town_codes, town_names

#get vehicles registred in db
def get_registred_vehicles_data(db_connection):
    """
    return: an array of vehicles codes
    """

    instance = VehicleTable()
    data = instance.get_all(db_connection)
    codes = []
    matricules = []
    for r in data:
        #append vehicle code or id
        codes.append(r[0])
        #append old matricule value
        matricules.append(r[1])
        #append new matricule value
        matricules.append(r[2])
        
    return codes, matricules