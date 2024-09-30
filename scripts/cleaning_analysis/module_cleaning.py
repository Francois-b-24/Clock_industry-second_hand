import sqlite3
import pandas as pd
from skimpy import skim
import numpy as np
import seaborn as sns
import missingno as msno
import re
from datetime import datetime
from collections import defaultdict
import math
from fuzzywuzzy import process



################################################################################################    
                            # NETTOYAGE PRÉLIMINAIRE. 
################################################################################################

def suppression_doublons(df, liste_colonnes):
    """
    Supprimme les doublons dans le DataFrame sur la base d'une liste de colonnes. 

    Args:
        df(pd.Dataframe) : Le Dataframe contenant les colonnes. 
        liste_colonne (list) : Liste des variables sur lesquelles va se baser la suppression des doublons. 
    
    Returns: 
        pd.Dataframe : DataFrame sans doublons. 
    """    
    df = df.drop_duplicates(subset=liste_colonnes, keep ='first')
    
    return df

def suppression_lignes_vides(df, liste_colonnes): 
    """
    Fonction pour supprimer les NaN sur la base de certaines colonnes dont il est difficile de retrouver l'information. 

    Args:
        df (pd.DataFrame): Le DataFarme contenant les colonnes.
        liste_colonnes (list): Listes des colonnes sur lesquelles va se baser la suppression des colonnes.
    
    Returns: 
        pd.DataFrame : DataFrame soulagé de quelque lignes.  
    """
    df = df.dropna(subset=liste_colonnes)

    return df

# Fonction pour remplir les valeurs manquantes de certaines variables en fonction de la marque, du modèle et du mouvement
def remplissage(df, variable):
    """
    Il s'avère que certaines informations des montres sont belles et bien existantes mais ne sont pas renseignées par les vendeurs. 
    Cette fonctions a pour but de renseigner les valeurs manquantes de certaines variables sur la base de lignes ayant les caractéristiques similaires à savoir :
        - La marque
        - Le modèle 
        - Le mouvement

    Args:
        df (pd.DataFrame): DataFrame contenant la colonne remplir
        variable (str): Nom de la colonne à remplir. 

    Returns:
        pd.DatFrame: DatFrame final. 
    """
    for index, row in df.iterrows():
        if pd.isna(row[variable]) or row[variable] == '[]':
            similar_rows = df[
                (df['marque'] == row['marque']) &
                (df['modele'] == row['modele']) &
                (df['mouvement'] == row['mouvement']) &
                df[variable].notna()
            ]
            if not similar_rows.empty:
                df.at[index, variable] = similar_rows[variable].iloc[0]
    return df

def remplissage_mouvement(df, variable):
    """
    Fonctions pour renseigner la variable 'mouvement' sur la base des modèles ayant des caractéristiques similaires, à savoir : 
        - La marque 
        - Le modèle 
        - mouvement == '[]'
        

    Args:
        df (pd.DataFrame): DataFrame contenant la variable à traiter
        variable (str): Nom de la colonne à traiter. 

    Returns:
        pd.DataFrame: DataFrame final. 
    """
    for index, row in df.iterrows():
        if pd.isna(row[variable]):
            similar_rows = df[
                (df['marque'] == row['marque']) &
                (df['modele'] == row['modele']) &
                (df['mouvement'] == '[]') & 
                df[variable].notna()
            ]
            if not similar_rows.empty:
                df.at[index, variable] = similar_rows[variable].iloc[0]
    return df

def remplissage_reserve_marche(df, variable):
    """
    Fonction pour remplir la colonne 'Reserve de marche'. 
    Sur la base de la colonne contenant l'information sur le rouage, on peut déduire si la montre possède une réserve de marche. 
    La colonne rouage nous indique si la montre possède un mouveemnt Quartz. Si c'est le cas, alors, on peut déduire qu'il n'y a pas de réserve de marche. 

    Args:
        df (pd.DataFrame): DataFrame contenant la colonne à traiter
        variable (str): Nom de la colonne à traiter. 

    Returns:
        pd.DataFrame: DataFrame
    """
    for index, row in df.iterrows():
        if pd.isna(row[variable]) or row[variable] == '[]':
            # Vérifier si row['rouage'] est une chaîne de caractères
            if isinstance(row['rouage'], str):
                similar_rows = df[
                    (row['rouage'].startswith('Quar') or row['rouage'].startswith('ETA')) &
                    df[variable].notna()
                ]
                if not similar_rows.empty:
                    df.at[index, variable] = 'Pas_de_reserve'
            else:
                # Gérer les cas où row['rouage'] n'est pas une chaîne de caractères (par exemple, NaN ou float)
                # Vous pouvez soit ignorer ces lignes, soit les gérer différemment selon vos besoins
                continue
    return df


def suppression_lignes_vides_suite(df, liste_colonnes, colonnes_a_supp, colonne):
    """
    Fonctions pour supprimer les lignes vides sur la base de certaines colonnes dont on à toujours pas pu remplir les informations.
    Supprime également certaines colonnes initules.  

    Args:
        df (pd.DataFarme): DataFrame contenant les colonnes à traiter. 
        liste_colonnes (list): Liste des colonnes à traiter.
    
    Returns: 
        pd.DataFrame : DataFrame modifié.  
    """
    
    df = df.dropna(subset=liste_colonnes)
    df = df[df[colonne] != '[]']
    df = df.drop(columns=colonnes_a_supp)
    df = df.dropna(subset=liste_colonnes)
    
    return df

def count_functions(fonction_string):
    
    """
    Fonctions pour le comptage du nombre de complications.
    Préférence pour la synthétisation de cette informations.  
    
    Args: 
        Variable (str): Colonne à traiter. 
    Returns:
        pd.DatFrame : DataFrame final.
    """
    if pd.isna(fonction_string):
        return 0
    if 'Fonctions\n' in fonction_string:
        fonctions_part = fonction_string.split('Fonctions\n')[1]
        # Diviser par les virgules et les espaces pour obtenir les fonctions
        fonctions_list = [func.strip() for func in fonctions_part.split(',')]
        return len(fonctions_list)
    elif 'Autres\n' in fonction_string:
        fonctions_part = fonction_string.split('Autres\n')[1]
        # Diviser par les virgules et les espaces pour obtenir les fonctions
        fonctions_list = [func.strip() for func in fonctions_part.split(',')]
        return len(fonctions_list)
    else:
        return 'Non_renseignée'
    

def remplissage_mouvement_bis(df, variable):
    """
    Autre fonction pour remplir la colonne 'Mouvement'

    Args:
        df (pd.DatFrame): DataFrame contenant la colonne à traiter. 
        variable (str): La colonne à traiter. 

    Returns:
        pd.DataFrame: DataFrame final. 
    """
    for index, row in df.iterrows():
        if row[variable] == '[]':
            df.at[index, variable] = 'Quartz'
    return df
    
    
def remplissage_mat_verre(df, variable):
    """
    Fonctions pour remplir la colonne 'matiere_verre'.

    Args:
        df (pd.DataFrame): DafaFrame contenant la colonne à traiter. 
        variable (str): La colonne à traiter. 

    Returns:
        pd.DatFrame: DataFrame final. 
    """
    for index, row in df.iterrows():
        if row[variable] == '[]':
            df.at[index, variable] = 'Inconnue'
    return df

def remplissage_reserve_marche_bis(df, variable):
    """Fonction pour renseigner la colonne 'Reserve de marche'

    Args:
        df (pd.DataFrame): DataFrame contenant la colonne à traiter. 
        variable (str): Colonne à traiter. 

    Returns:
        pd.DataFrame: DataFrame final. 
    """
    # Utiliser des opérations vectorisées pour identifier les lignes à modifier
    masque = (df[variable].isna()) & (df['mouvement'] == 'Quartz')
    
    # Appliquer la valeur 'Pas_de_reserve' aux lignes identifiées
    df.loc[masque, variable] = 'Pas_de_reserve'
    
    return df

################################################################################################    
                            # TRAITEMENT DES COLONNES. 
################################################################################################
def traitement_marque(df, colonne):
    """
    """
    df[colonne] = df[colonne].astype(str)
    marque = [i.replace('&', '') for i in df['colonne']]
    marque = [i.strip("['").strip("']")for i in marque]
    marque = [i.replace(',','') for i in marque]
    marque = [i.replace("''",'').replace(" ",'').replace("''",'_').replace('-','_') for i in marque]
    marque = [i.upper() for i in marque]
    df[colonne] = marque
    df[colonne] = df[colonne].astype('category')
    
    # Liste des catégories uniques nettoyées
    categories_uniques = df[colonne].unique()

    # Fonction de correspondance floue pour les catégories similaires
    def fuzzy_grouping(valeur, categories_reference, seuil=80):
        correspondance, score = process.extractOne(valeur, categories_reference)
        if score >= seuil:
            return correspondance
        return valeur

    # Application de la fonction de correspondance floue
    df[colonne] = df[colonne].apply(fuzzy_grouping, categories_reference=categories_uniques)

    return df

def traitement_modele(df, colonne):
    """
    """
    modele = [''.join(i.split(",")[0:2]) for i in df[colonne]]
    modele = [i.strip("['").strip("']").replace(',','').replace(' ','').replace("''","_") for i in modele]
    modele = [i.upper() for i in modele]
    df[colonne] = modele
    df[colonne] = df[colonne].astype('category')
    
def traitement_mouvement(df, colonne):
    mapping = {"['28000', 'A/h']": "automatique",
            "REMONTAGE''AUTOMATIQUE": "automatique",
            "REMONTAGE''MANUEL" : "manuel"
            } 
            
            
    df[colonne] = df[colonne].replace(mapping)
    mouvement = [i.strip("['").strip("']").replace(',','').replace(' ','') for i in df[colonne]]
    mouvement = [i.upper() for i in mouvement]
    df[colonne] = mouvement
    df[colonne] = df[colonne].astype('category')

    return df
            

def traitement_matiere_bracelet(df, colonne):
    """_summary_

    Args:
        df (_type_): _description_
        colonne (_type_): _description_
    """
    
    matiere_bracelet = [i.replace('mm)','Inconnue').replace("mm",'Inconnue').replace('/',"_") for i in df[colonne]]
    matiere_bracelet =  [i.upper() for i in matiere_bracelet]
    df[colonne] = matiere_bracelet
    df[colonne] = df[colonne].astype('category')

    valeurs_a_remplacer = ['BRACELET','ROSE','NOIR','JAUNE',
                        'BRUN','BLANC','VERT','GRIS','BLEU','BORDEAUX',
                        'BEIGE',"['BRACELET']","['DU', 'BRACELET']",
                        "['DU', 'BRACELET', 'BRUN']","['DU', 'BRACELET', 'BLEU']",
                        "['DU', 'BRACELET', 'NOIR']"]
    df[colonne] = df[colonne].replace(valeurs_a_remplacer, "INCONNUE")

    mapping = {"['ACIER']" : "ACIER",
            "D'AUTRUCHE" : "CUIR_AUTRUCHE",
            "ARGENTÉ" : "ARGENT",
            "VACHE" : "CUIR_VACHE",
            "['OR_ACIER']" : "OR_ACIER",
            "['TEXTILE']" : "TEXTILE",
            "['CUIR']" : "CUIR",
            "['CAOUTCHOUC']" : "CAOUTCHOUC",
            "['TITANE']" : "TITANE",
            "['DU', 'BRACELET', 'CAOUTCHOUC']": 'CAOUTCHOUC',
            "['DU', 'BRACELET', 'ACIER']" : "ACIER",
            "['DU', 'BRACELET', 'CUIR']" :"CUIR",
            "['DU', 'BRACELET', 'OR_ACIER']":"OR_ACIER",
            "['DU', 'BRACELET', 'TEXTILE']":"TEXTILE",
            "['DU', 'BRACELET', 'TITANE']" : "TITANE",
            "['DU', 'BRACELET', 'OR', 'JAUNE']":"OR_JAUNE",
            "['DU', 'BRACELET', 'SILICONE']" :'SILICONE',
            "['DU', 'BRACELET', 'OR', 'ROSE']":"OR_ROSE",
            "['DU', 'BRACELET', 'OR']":"OR",
            "['DU', 'BRACELET', 'MATIÈRE', 'PLASTIQUE']": "PLASTIQUE",
            "['DU', 'BRACELET', 'CÉRAMIQUE']":"CERAMIQUE",
            "['DU', 'BRACELET', 'OR', 'BLANC']":"OR_BLANC",
            "['DU', 'BRACELET', 'CUIR', 'DE', 'VACHE']":"CUIR_VACHE",
            "['DU', 'BRACELET', 'PLATINE']":"PLATINE",
            "CÉRAMIQUE":"CERAMIQUE"          
        
    }
    df[colonne] = df[colonne].replace(mapping)
    
    return df

def traitement_matiere_boitier(df, colonne):
    
    """_summary_
    """
    matiere_boitier = [i.strip(']').strip('[') for i in df[colonne]]
    matiere_boitier = [i.replace("', '","_").replace("'",'').replace('/','_') for i in matiere_boitier]
    matiere_boitier = [i.upper() for i in matiere_boitier]
    df[colonne] = matiere_boitier
    df[colonne] = df[colonne].astype('category')
    
    return df 

def traitement_annee_prod(df, colonne):
    """_summary_

    Args:
        df (_type_): _description_
        colonne (_type_): _description_
    """
    
    annee_prod = [i.strip(']').strip('[') for i in df[colonne]]
    annee_prod = [i.strip("'").strip("',").strip("").strip("'(").strip("',").strip(")") for i in df[colonne]]
    df[colonne] = annee_prod
    
    # Fonction pour extraire les années (format AAAA)
    def extract_year(valeur):
        # Utiliser une regex pour chercher un nombre à 4 chiffres (une année)
        match = re.search(r'\b(19|20)\d{2}\b', str(valeur))
        if match:
            return match.group(0)  # Si une année est trouvée, la retourner
        else:
            return None  # Si aucune année n'est trouvée

    # Appliquer la fonction à la liste de données
    annee_prod = [extract_year(valeur) for valeur in df[colonne]]
    df[colonne] = annee_prod

        
    return df

def traitement_etat(df, colonne):
    """_summary_

    Args:
        df (_type_): _description_
        colonne (_type_): _description_
    """
    
    etat = [i.replace("[", "").replace("]", "").replace("'", "").replace("\"", "").replace("(", "").replace(")", "") for i in df[colonne]]
    etat = [i.split(",")[0:2] for i in etat]
    etat = [''.join(sous_liste).strip() for sous_liste in etat]
    df[colonne] = etat
    df[colonne] = df[colonne].astype('category')
    
    # Dictionnaire de regroupement des modalités
    
    regroupement_modalites = {
        'Neuf/Très bon': [
            'Très bon', 'Neuve Neuve', 'Neuf', 'Aucune trace', 'État neuf', 'dorigine/Pièces originales', 'Jamais portée'
        ],
        'Bon/Satisfaisant': [
            'Bon Traces', 'Satisfaisant Traces', 'Doccasion Bon', 'Doccasion Très', 'Doccasion Satisfaisant', 'Légères traces', 'Traces dusure', 'Doccasion :'
        ],
        'Défectueux': ['Défectueux Fortement'],
        'Incomplet': ['Incomplet Éléments', 'Doccasion Incomplet']
    }

    # Fonction pour obtenir la nouvelle modalité basée sur la codification minimaliste
    def obtenir_nouvelle_modalite(modalite):
        for nouvelle_modalite, modalites_orig in regroupement_modalites.items():
            if modalite in modalites_orig:
                return nouvelle_modalite
        return 'Autre'  # Pour gérer les cas non couverts

    #   Exemple d'utilisation
    modalites_simplifiees = [obtenir_nouvelle_modalite(mod) for mod in df[colonne]]
    df[colonne] = modalites_simplifiees
    
    mapping = {
    'Neuf/Très bon' : 'NEUF',
    'Bon/Satisfaisant' : 'SATISFAISANT',
    'Incomplet' :'INDETERMINE',
    'Autre' :'INDETERMINE',
    'Défectueux' : 'DEFECTUEUX'
    } 


    df[colonne] = df[colonne].replace(mapping)
    return df

def traitement_sexe(df, colonne):
    """
    """
    
    mapping = {"HOMME/UNISEXE":"HOMME",
           "['MONTRE', 'HOMME/UNISEXE']":"HOMME",
           "['MONTRE', 'FEMME']":"FEMME"  
    }
    
    df[colonne] = df[colonne].replace(mapping)
    sexe = [i.upper() for i in df[colonne]]
    df[colonne] = sexe
    df[colonne] = df[colonne].astype('category')
    
    return df