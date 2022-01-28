from datetime import date
from time import time

#Rotation data column names
rotations_table_header = ["vehicle", "town", "date", "time", "gap", "net_cet", "net_extra", "tare", "brute", "ticket"]

#dict of column names and renaming rules
rotations_table_renaming = {
                    "véhicule": "vehicle", 
                    "commune": "town",
                    "ecart": "gap", 
                    "heure": "time", 
                    "n ticket": "ticket", 
                    "n tick": "ticket", 
                    "net cet": "net_cet",
                    "net extra": "net_extra",
                    "poids net": "net_cet",
                    "poids extra": "net_extra",
}

#dict of column names and types
rotations_table_types = {
        "Véhicule": str, 
        "N ticket": str, 
        "N tick": str, 
        "Date": date, 
        "Net CET": int,
        "Net Extra": int,
        "Poids net": int,
        "Poids extra": int,
        "Tare": int,
        "Brute": int,
        "heure": time,
    }