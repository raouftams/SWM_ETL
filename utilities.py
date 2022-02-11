import requests
from dateutil.parser import parse

#Rotation data column names
rotations_table_header = ["vehicle_mat", "vehicle_id", "town", "town_code", "unit", "unit_code", "date", "date_hijri", "time", "net_extra", "gap", "net_cet", "tare", "brute", "ticket", "cet"]

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
                    "commune": "town",
                    "commune d'intervention": "town",
                    "codec": "town_code",
                    "code c": "town_code",
                    "code commune": "town_code",
                    "nom client": "town",
                    "unité": "unit",
                    "code u": "unit_code",
                    "codeu": "unit_code", 
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
                    "ecart": "gap",
                    "écart": "gap",
                    "différence": "gap",
                    "diff": "gap",
                    "datehijri": "date_hijri",
}

#dict of column names and types
rotations_table_types = {
        "vehicle_mat" : str,
        "vehicle_id" : str,
        "town_code" : str,
        "town": str,
        "unit_code" : str,
        "unit": str,
        "gap" : float,
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
     "H'Raoua", 
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
    "bek": "Borj El Kiffan",
    "deb": "Dar El Beïda",
    "beb": "Borj El Bahri",
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


#get hijri date from a date
def gregorian_to_hijri_date(date: str):
    """
    params: date in format "dd-mm-yyyy"
    return: corresponding hijri date in format "dd-mm-yyyy" and array of islamic holidays if they exist
    """
    #if date is in format "dd-mm-yyyy hh:mm:ss" we remove the time
    date = date.split(" ")[0]
    # if date in in format "dd/mm/yyyy" we convert it to "dd-mm-yyyy"
    if not "-" in date:
        date = date.replace("/", "-")
    #if date is in format "yyyy-mm-dd" we convert it to "dd-mm-yyyy"
    y, m, d = date.split("-")
    if int(y) > 100:
        date = d + "-" + m + "-" + y

    #get the corresponding hijri date
    url = "https://api.aladhan.com/v1/gToH?date={}".format(date)
    r = requests.get(url)
    data = r.json()["data"]
    return data["hijri"]["date"]


#get gregorian date from a hijri date
def hijri_to_gregorian_date(date: str):
    """
    params: date in format "dd-mm-yyyy"
    return: corresponding gregorian date in format "dd-mm-yyyy" 
    """
    #if date is in format "dd-mm-yyyy hh:mm:ss" we remove the time
    date = date.split(" ")[0]
    # if date in in format "dd/mm/yyyy" we convert it to "dd-mm-yyyy"
    if not "-" in date:
        date = date.replace("/", "-")
    
    #if date is in format "yyyy-mm-dd" we convert it to "dd-mm-yyyy"
    y, m, d = date.split("-")
    if int(y) > 100:
        date = d + "-" + m + "-" + y

    #get the corresponding gregorian date
    url = "https://api.aladhan.com/v1/hToG?date={}".format(date)
    r = requests.get(url)
    data = r.json()["data"]
    return data["gregorian"]["date"]


#check if a string has any format of a date
def is_date(string, fuzzy=False):

    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

