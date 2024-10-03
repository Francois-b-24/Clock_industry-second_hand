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

def suppression(df, liste_colonnes):
    """
    Supprimme les doublons dans le DataFrame sur la base d'une liste de colonnes. 
    Fonction pour supprimer les NaN sur la base de certaines colonnes dont il est difficile de retrouver l'information. 
    
    Args:
        df(pd.Dataframe) : Le Dataframe contenant les colonnes. 
        liste_colonnes (list): Listes des colonnes sur lesquelles va se baser la suppression des lignes vides. 
    
    Returns: 
        pd.Dataframe : DataFrame sans doublons. 
    """    
    df = df.drop_duplicates()
    df = df.dropna(subset=liste_colonnes)
    
    return df


# Fonction pour remplir les valeurs manquantes de certaines variables en fonction de la marque, du modèle et du mouvement
def remplissage(df, variable):
    """
    Il s'avère que certaines caractéristiques des montres sont belles et bien existantes mais ne sont pas renseignées par les vendeurs. 
    Cette fonctions a pour but de renseigner les valeurs manquantes de ces caractéristiques sur la base de lignes ayant les caractéristiques similaires à savoir :
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
    

    df = df[df[colonne] != '[]']
    df = df.drop(columns=colonnes_a_supp)
    df = df.dropna(subset=liste_colonnes)
    
    return df

################################################################################################    
                            # TRAITEMENT DES COLONNES. 
################################################################################################
def traitement_marque(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'Marque'.
    On utilise également la fonction de correspondances floue pour regrouper les catégories similaires et ainsi 
    réduire le nombre de modalité. 
    
    Arg:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    df[colonne] = df[colonne].astype(str)
    marque = [i.replace('&', '') for i in df[colonne]]
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
    Fonctions pour le traitement des caractères spéciaux de la colonne 'Modele'.
    On utilise également la fonction de correspondances floue pour regrouper les catégories similaires et ainsi 
    réduire le nombre de modalité. 
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    modele = [''.join(i.split(",")[0:2]) for i in df[colonne]]
    modele = [i.strip("['").strip("']").replace(',','').replace(' ','').replace("''","_") for i in modele]
    modele = [i.upper() for i in modele]
    df[colonne] = modele
    df[colonne] = df[colonne].astype('category')
    
    return df
    
def traitement_mouvement(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'Mouvement'.
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """        
            
    
    mouvement = [i.strip("['").strip("']").replace(',','').replace(' ','') for i in df[colonne]]
    mouvement = [i.upper() for i in mouvement]
    df[colonne] = mouvement
    df[colonne] = df[colonne].astype('category')

    mapping = {"28000''A/H": "AUTOMATIQUE",
           "REMONTAGE''AUTOMATIQUE": "AUTOMATIQUE",
           "REMONTAGE''MANUEL" : "AUTOMATIQUE"
           } 
    
    df[colonne] = df[colonne].replace(mapping)
           
           
    df['mouvement'] = df['mouvement'].replace(mapping)
    return df
            

def traitement_matiere_bracelet(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'Matiere_bracelet'.
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    
    matiere_bracelet = [i.replace('mm)','Inconnue').replace("mm",'Inconnue').replace('/',"_") for i in df[colonne]]
    matiere_bracelet =  [i.upper() for i in matiere_bracelet]
    df[colonne] = matiere_bracelet
    df[colonne] = df[colonne].astype('category')

    valeurs_a_remplacer = ['BRACELET','ROSE','NOIR','JAUNE',
                        'BRUN','BLANC','VERT','GRIS','BLEU','BORDEAUX',
                        'BEIGE',"['BRACELET']","['DU', 'BRACELET']",
                        "['DU', 'BRACELET', 'BRUN']","['DU', 'BRACELET', 'BLEU']",
                        "['DU', 'BRACELET', 'NOIR']", "['NOIR']"]
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
    df[colonne] = df[colonne].str.replace('[\'DU\', \'BRACELET\', \'CUIR\', "D\'AUTRUCHE"]','CUIR_AUTRUCHE' )
    
    return df

def traitement_matiere_boitier(df, colonne):
    
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'Matiere_boitier'.
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    matiere_boitier = [i.strip(']').strip('[') for i in df[colonne]]
    matiere_boitier = [i.replace("', '","_").replace("'",'').replace('/','_') for i in matiere_boitier]
    matiere_boitier = [i.upper() for i in matiere_boitier]
    df[colonne] = matiere_boitier
    df[colonne] = df[colonne].astype('category')
    
    return df 

def traitement_annee_prod(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'Annee_prod'.
    Utilisation du regex pour extraire uniquement les années. 
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
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
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'etat'.
    
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
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
    Fonctions pour le traitement des caractères spéciaux de la colonne 'sexe'.
    
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    
    
    sexe = [i.upper() for i in df[colonne]]
    df[colonne] = sexe
    df[colonne] = df[colonne].astype('category')
    
    mapping = {"['HOMME/UNISEXE']":"HOMME",
            "HOMME/UNISEXE" : 'HOMME' ,
           "['MONTRE', 'HOMME/UNISEXE']" : "HOMME",
           "['MONTRE', 'FEMME']":"FEMME",
           "femme":"FEMME" 
    }

    df[colonne] = df[colonne].replace(mapping)
    
    return df

def extraire_elements_avant_euro(chaine):
    
    # Convertir la chaîne en liste Python
    liste = eval(chaine)
    
    # Initialiser une liste vide pour les résultats
    sous_liste = []
    
    # Vérifier si '€)' ou '€' est dans la liste
    if '€)' in liste:
        # Trouver l'index de '€)'
        index_fin = liste.index('€)')
        
        # Extraire les deux éléments précédant '€)' s'ils existent
        if index_fin >= 2:
            sous_liste = liste[index_fin-2:index_fin]
        else:
            sous_liste = liste[:index_fin]
    elif '€' in liste:
        # Trouver l'index de '€'
        index_fin = liste.index('€')
        
        # Extraire les deux éléments précédant '€' s'ils existent
        if index_fin >= 2:
            sous_liste = liste[index_fin-2:index_fin]
        else:
            sous_liste = liste[:index_fin]
    
    return sous_liste

def traitement_prix(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'prix'.
    
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    
    prix = [''.join(sous_liste).strip() for sous_liste in df[colonne]]
    prix = [i.replace("(=","") for i in prix]
    df[colonne] = prix
    df = df[df[colonne] != '']
    df[colonne] = df[colonne].astype('float')
    
    return df

# Fonction pour extraire les éléments spéciaux
def extraire_elements_h(chaine):
    # Gérer le cas spécial 'Pas_de_reserve'
    if chaine == 'Pas_de_reserve':
        return 0
    if chaine == '[Ultra, Thin, Réserve, de, Marche]':
        return 0
    
    # Convertir la chaîne en liste Python
    liste = eval(chaine)
    
    # Vérifier si 'h' est dans la liste
    if 'h' in liste:
        # Trouver l'index de 'h'
        index_h = liste.index('h')
        
        # Extraire l'élément précédant 'h' s'il existe
        if index_h >= 1:
            return liste[index_h-1]
        else:
            return liste[:index_h]
    
    # Si aucune des conditions n'est remplie, retourner la liste entière
    return list 

def traitement_diametre(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'diametre'.
    
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    
    diametre = [i.split(',')[0] for i in df[colonne]]
    diametre = [i.replace(" ","") for i in diametre]
    diametre = [i.replace('[',"").replace("]","").replace("'",'').replace("mm","") for i in diametre]
    df[colonne] = diametre
    df[colonne] = df[colonne].astype('float')
    diametre = [math.ceil(i) for i in df[colonne]]
    df[colonne] = diametre
    
    return df

def traitement_etencheite(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'etencheite'.
    
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    etencheite = [i.split(',')[0] for i in df[colonne]]
    etencheite = [i.replace('[',"").replace("]","").replace("'",'').replace('Non', '0').replace('Au-delà','0') for i in etencheite]
    df[colonne] = etencheite
    df[colonne] = df[colonne].astype('float')
    
    return df

def traitement_matiere_lunette(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'matiere_lunette'.
    
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    valeurs_a_remplacer = ['rose','jaune','blanc','rouge']
    df[colonne] = df[colonne].replace(valeurs_a_remplacer, 'Indetermine')
    matiere_lunette = [i.replace("/","_").upper() for i in df[colonne]]
    df[colonne] = matiere_lunette
    df[colonne] = df[colonne].astype('category')
    
    def extract_matter(val):
        # Vérifier si la valeur est une liste sous forme de chaîne
        if isinstance(val, str) and '[' in val and ']' in val:
            # Extraire les éléments de la liste à partir de la chaîne
            elements = re.findall(r"'([^']*)'", val)
            # Chercher le dernier élément de la liste (la matière)
            return elements[-1] if elements else None
        else:
            # Si ce n'est pas une liste, retourner la valeur elle-même
            return val

    matiere_lunette = [extract_matter(val) for val in df[colonne]]
    df[colonne] = matiere_lunette
    
    return df

def traitement_matiere_verre(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'matiere_verre'.
    
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    
    matiere_verre = [i.replace("[","").replace("]","").replace("'","") for i in df[colonne]]
    matiere_verre = [i.upper() for i in matiere_verre]
    df[colonne] = matiere_verre
    df[colonne] = df[colonne].astype('category')
    
    mapping = {
        "VERRE, SAPHIR" : "SAPHIR",
        "VERRE, MINÉRAL" : "MINÉRAL"
    }
    
    df[colonne] = df[colonne].replace(mapping)
        
    return df

def traitement_boucle(df, colonne):
    """Fonction pour le traitement de la colonne boucle.

    Args:
        df (pd.DataFrame): DataFrame contenant la variable
        colonne (str): Nom de la colonne

    Returns:
        pd.DataFrame: Retourne le DataFrame modifié. 
    """
    boucle = [i.strip(",").replace("[","").replace("]","").replace("'","").replace(",","").replace(" ","_") for i in df[colonne]]
    boucle = [i.upper() for i in boucle]
    df[colonne] = boucle
    df[colonne] = df[colonne].astype('category')
    return df

def traitement_matiere_boucle(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'matiere_boucle'.
    
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """
    matiere_boucle = [i.strip(",").replace("[","").replace("]","").replace("'","").replace(",","").replace(" ","_").replace("/","_") for i in df['matiere_boucle']]
    matiere_boucle = [i.upper() for i in matiere_boucle]
    df[colonne] = matiere_boucle
    df[colonne] = df['matiere_boucle'].astype('category')
    df[colonne] = df['matiere_boucle'].str.replace('DE_LA_LUNETTE_','').str.replace('DE_LA_BOUCLE_','').str.replace('MATIÈRE_','')
    
    return df

def traitement_ville(df, colonne):
    """
    Fonctions pour le traitement des caractères spéciaux de la colonne 'ville'.
    
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne.
        colonne (str): Colonne à traiter. 
    
    Returns:
        pd.DataFrame : Le DataFrame contenant la colonne après traitement. 
    """ 
    pays = [i.replace('[','').replace(']','').replace("'","") for i in df[colonne]]
    pays = [i.split(',')[0] for i in pays]
    pays = [i.upper() for i in pays]
    
    df = df.drop(columns=[colonne])
    df['pays'] = pays
    df['pays'] = df['pays'].astype('category')
    
    mapping = {
        'AFRIQUE': 'AFRIQUE_DU_SUD',
        'RÉPUBLIQUE' : 'RÉPUBLIQUE_TCHEQUE',
        'HONG' : 'HONG_KONG',
        'VIÊT' : 'VIETNAM',
        'PORTO' : 'PORTUGAL',
        'E.A.U.' : 'EMIRAT_ARABE_UNIS',
        'SRI': 'SRI_LANKA',
        'ARABIE' : 'ARABIE_SAOUDITE' 
    }

    df['pays'] = df['pays'].replace(mapping)
    
    return df

def traitement_complication_date(df,colonne_1, colonne_2):
    """Fonction pour le traitement des colonnes Complication et Date de récupération

    Args:
        colonne_1 (str): Nom de la première colonne --> 'Complications'
        colonne_2 (str): Nom de la deuxième colonne --> 'Date_recup'
    Returns:
        pd.DataFrame: Le DataFrame modifié. 
    """
    df[colonne_1].astype('category')
    df[colonne_2] = pd.to_datetime(df[colonne_2], errors ='coerce')
    
    return df
    