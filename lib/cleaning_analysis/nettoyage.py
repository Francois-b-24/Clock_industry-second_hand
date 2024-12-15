import pandas as pd
import numpy as np
import re
from datetime import datetime
from collections import defaultdict


class Nettoyage:
    def __init__(self, df):
        self.df = df

    # Nettoyage préliminaire 
    
    def nettoyage_colonnes(self, remplacer_nan=np.nan) -> pd.DataFrame:
        
        # Suppression de la date de récupération :
        if 'Date_recup' in self.df.columns:
            self.df.drop(columns='Date_recup', inplace=True)
       
        for col in self.df.columns:
            # Supprimer les crochets et apostrophes dans les chaînes de caractères
            self.df[col] = self.df[col].apply(lambda x: re.sub(r"[\[\]'\"()]", '', str(x)) if isinstance(x, str) else x)
            
            # Supprimer les espaces multiples
            self.df[col] = self.df[col].apply(lambda x: re.sub(r'\s+', ' ', x).strip() if isinstance(x, str) else x)
            
            # Convertit les colonnes en majuscules : 
            self.df[col] = self.df[col].str.upper()
            
            # Remplacer les valeurs NaN par la valeur spécifiée par défaut
            self.df[col] = self.df[col].fillna(remplacer_nan)
               

            
        # Supprimer les espaces en début et fin de chaîne si c'est une colonne de type chaîne
        if self.df[col].dtype == 'object':
            self.df[col] = self.df[col].str.strip()
        
        # Supprimer les doublons
        self.df.drop_duplicates(inplace = True)
                        

        # Remplacer les chaînes vides et 'None' par NaN
        self.df.replace('', np.nan, inplace=True)
        self.df.replace('None', np.nan, inplace=True)
        
        # Suppression des lignes pour lesquelles on ne connait pas la marque et le modèle
        self.df = self.df.dropna(subset=['marque','modele'])
        
        return self.df
    
    
    
    
    def remplissage(self, variable):
        
        """
        Renseigne l'information manquante pour une colonne donnée, si une ligne possède la marque, le modèle
        et un mouvement similaire. 

        Returns:
            colonne (str): Colonne pour laquelle on doit renseigner les valeurs manquantes. 
        """
        # Grouper par 'marque', 'modele', 'mouvement' pour trouver les valeurs similaires
        groupes_similaires = self.df.groupby(['marque', 'modele', 'mouvement'])[variable]

        # Remplir les valeurs manquantes avec la première valeur non manquante des groupes similaires
        self.df[variable] = self.df[variable].fillna(groupes_similaires.transform('first'))

        return self.df
    
    
    
    
    # Un peu de preprocessing sur les variables
    
    def remplissage_mouvement(self):
        """
        Renseigne la colonne mouvement. 

        Returns:
            pd.DataFrame
        """
        # Remplacer les listes vides par des NaN (si applicable, sinon cette étape peut être ignorée)
        self.df['mouvement'] = self.df['mouvement'].apply(lambda x: np.nan if isinstance(x, list) and not x else x)

        # Grouper par 'marque' et 'modele' pour trouver les valeurs similaires
        groupes_similaires = self.df.groupby(['marque', 'modele'])['mouvement']

        # Remplir les valeurs manquantes avec la première valeur non manquante des groupes similaires
        self.df['mouvement'] = self.df['mouvement'].fillna(groupes_similaires.transform('first'))

        return self.df
    
    
    
    
    def remplissage_reserve_marche(self):
        """
        Renseigne la colonne 'reserve de marche'.

        Returns:
            pd.DataFrame
        """
        
        # Remplir 'Pas_de_reserve' pour les lignes où 'rouage' commence par 'Quar' ou 'ETA' et où 'reserve de marche' est NaN ou vide
        masque_quartz_eta = (
            (self.df['reserve_de_marche'].isna() | (self.df['reserve_de_marche'] == '')) &  # Si 'variable' est manquant ou vide
            self.df['rouage'].apply(lambda x: isinstance(x, str) and (x.startswith('Quar') or x.startswith('ETA')))  # Vérifier 'rouage'
        )
        self.df.loc[masque_quartz_eta, 'reserve_de_marche'] = 'Pas_de_reserve'

        # Remplir 'Pas_de_reserve' pour les lignes où 'mouvement' est 'Quartz' et où 'variable' est NaN
        masque_quartz_mouvement = (self.df['reserve_de_marche'].isna() | (self.df['reserve_de_marche'] == '')) & (self.df['mouvement'] == 'Quartz')
        self.df.loc[masque_quartz_mouvement, 'reserve_de_marche'] = 'Pas_de_reserve'

        return self.df
    
    

    def comptage_fonctions(self, fonction_string):
        """
        Compte le nombre de complications ou fonctions dans une chaîne donnée.

        Args:
            fonction_string (str): La chaîne à analyser.

        Returns:
            int ou str: Le nombre de fonctions trouvées, ou 'Non_renseignée' si aucune information.
        """
        if not isinstance(fonction_string, str) or pd.isna(fonction_string):
            return 0
    
        # Liste des mots-clés à rechercher pour les fonctions
        mots_clefs = ['FONCTIONS', 'AUTRES']
        
        for mots in mots_clefs:
            if mots in fonction_string:
                fonctions_part = fonction_string.split(mots)[-1]
                fonctions_list = [func.strip() for func in fonctions_part.split(',') if func.strip()]
                return len(fonctions_list)
        
        return 'Non_renseignée'


    def compteur_complications(self, column_name):
        """
        Ajoute une colonne au DataFrame contenant le nombre de fonctions pour chaque entrée.

        Args:
            column_name (str): Le nom de la colonne à traiter dans le DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame mis à jour avec une nouvelle colonne de comptage.
        """
        self.df[f'comptage_{column_name}'] = self.df[column_name].apply(self.comptage_fonctions)
        return self.df
        
    

    def suppression_colonnes(self):
        """
        Fonctions pour supprimer les colonnes inutiles
        
        Args:
            df (pd.DataFarme): DataFrame contenant les colonnes à traiter. 
            liste_colonnes (list): Liste des colonnes à traiter.
        
        Returns: 
            pd.DataFrame : DataFrame modifié.  
        """
        colonnes_a_supp = ['rouage', 'fonctions', 'descriptions', 'annee_prod']
        self.df = self.df.drop(columns=colonnes_a_supp)
        
        return self.df
    
    
    # Transformation des colonnes dans un format adéquat :
    
    
    def mise_en_forme(self):
        # Marque 
        marque = [i.replace(', ', '-').replace('.','') for i in self.df['marque']]
        self.df['marque'] = marque
        
        # Modèle
        modele = [i.replace(',','').replace(' ','-') for i in self.df['modele']]
        self.df['modele'] = modele
        
        # Mouvement 
        
        mapping = {"FOND, TRANSPARENT,, INDICATION, DE, LA, RÉSERVE, DE, MARCHE,, ÉTAT, DORIGINE/PIÈCES, ORIGINALES,, COUCHE, PVD/DLC" : "AUTOMATIQUE",
           
        "28000, A/H": "AUTOMATIQUE",
            "REMONTAGE, AUTOMATIQUE": "AUTOMATIQUE",
            "REMONTAGE, MANUEL" : "AUTOMATIQUE",
            "21600, A/H" : "AUTOMATIQUE",
            "REMONTAGE AUTOMATIQUE" : "AUTOMATIQUE",
            "MONTRE, CONNECTÉE" : "BATTERIE",
            "SQUELETTE" : "AUTOMATIQUE",
            "OSCILLATOIRE, 28800, A/H" : "AUTOMATIQUE", 
            "OSCILLATOIRE, 4, HZ" : "AUTOMATIQUE",
            "OSCILLATOIRE, 28800, HZ": "AUTOMATIQUE"
            } 
           
           
        self.df['mouvement'] = self.df['mouvement'].replace(mapping)
        
        # Sexe
        
        mapping_sexe = {"HOMME/UNISEXE":"HOMME",
           "MONTRE HOMME/UNISEXE":"HOMME",
           "MONTRE, FEMME":"FEMME",
           "MONTRE, HOMME/UNISEXE":"HOMME"  
           }

        self.df['sexe'] = self.df['sexe'].replace(mapping_sexe)
        
        # Matière verre et boucle
        mapping_verre = {
        'VERRE SAPHIR' : 'SAPHIR',
        'VERRE, MINÉRAL' : 'MINÉRAL',
        'MATIÈRE, PLASTIQUE':'PLASTIQUE',
        'VERRE, SAPHIR':'SAPHIR'
            }
        
        mapping_boucle = {
            "PLIS,_COUVERT" : "PLIS"
        }
    
        self.df['matiere_verre'] = self.df['matiere_verre'].replace(mapping_verre)
        self.df['boucle'] = self.df['boucle'].str.replace(', ','_')
        self.df['boucle'] = self.df['boucle'].replace(mapping_boucle)
        
        # ville
        
        pays = [i.split(',')[0].strip() if isinstance(i, str) else 'INCONNU' for i in self.df['ville']]
        self.df = self.df.drop(columns=['ville'])
        self.df['pays'] = pays
        
        mapping = {
            "GRANDE-BRETAGNE": 'ROYAUME-UNI',
            'AFRIQUE': 'AFRIQUE_DU_SUD',
            'RÉPUBLIQUE' : 'RÉPUBLIQUE_TCHEQUE',
            'HONG' : 'HONG_KONG',
            'VIÊT' : 'VIETNAM',
            'PORTO' : 'PORTUGAL',
            'E.A.U.' : 'EMIRAT_ARABE_UNIS',
            'SRI': 'SRI_LANKA',
            'ARABIE' : 'ARABIE_SAOUDITE' 
        }

        self.df['pays'] = self.df['pays'].replace(mapping)
        
        return self.df
        
     
    def extraire_matiere(self, chaine):
        matières = [ "oracier", "acier", "cuir", "textile", "titane", "caoutchouc", "bronze",
            "silicone", "vache", "dautruche", "bronze","plastique", "platine", "céramique","or",
            "aluminium", "argent", "requin", "caoutchouc", "plastique", "silicone", 
            "céramique", "satin", "blanc", "requin", "agenté", "rose", "jaune",
            "rouge", "tungstène", "palladium","lisse","carbone", "plaquée"]

        if isinstance(chaine, str): # Vérifier si la chaîne est une chaîne de caractères
            chaine = chaine.replace('/', "").lower()
            for matiere in matières:
                if matiere in chaine:
                    return matiere.upper()
        return np.nan
    
    
    def matiere(self):
        self.df['matiere_bracelet'] = self.df['matiere_bracelet'].apply(self.extraire_matiere)
        self.df['matiere_lunette'] = self.df['matiere_lunette'].apply(self.extraire_matiere)
        self.df['matiere_boucle'] = self.df['matiere_boucle'].apply(self.extraire_matiere)
        return self.df
    
    def mapping_matiere(self):
        mapping_matiere = {"ORACIER": "OR_ACIER",
           "DAUTRUCHE": "CUIR_AUTRUCHE",
           "BLANC":"OR_BLANC",
           "ROSE":"OR_ROSE",
           "VACHE":"CUIRE_DE_VACHE",
           "JAUNE":"OR_JAUNE",
           "ROUGE":"OR_ROUGE"    
        }       
        self.df['matiere_bracelet'] = self.df['matiere_bracelet'].replace(mapping_matiere)
        self.df['matiere_lunette'] = self.df['matiere_lunette'].replace(mapping_matiere)
        self.df['matiere_boucle'] = self.df['matiere_boucle'].replace(mapping_matiere)
        return self.df
     
    def nettoyage_matiere_boitier(self, chaine):
        if isinstance(chaine, str):  # Vérifier si la variable est une chaîne
            # Remplacer les barres obliques par des virgules avec espaces
            chaine = chaine.replace("/", ", ")
            # Nettoyer les virgules en trop
            chaine = re.sub(r'\s*,\s*', ', ', chaine)
            # Supprimer les espaces au début et à la fin
            chaine = chaine.strip()
            # Remplacer les virgules et espaces par des underscores
            chaine = chaine.replace(', ', '_')
            return chaine
        else:
            return np.nan  # Retourner 'INCONNUE' si ce n'est pas une chaîne
    
    
    
    def nettoyer_matiere_boitier(self):
        self.df['matiere_boitier'] = self.df['matiere_boitier'].apply(self.nettoyage_matiere_boitier)
        return self.df
    
    
    
    #def extraction_annee(self, valeur):
    # Utiliser une regex pour chercher un nombre à 4 chiffres (une année)
        """
        Extrait une année au format AAAA (entre 1900 et 2099) à partir d'une chaîne.

        Args:
            valeur (str): La chaîne à analyser.

        Returns:
            int ou NaN: L'année trouvée en tant qu'entier, ou NaN si aucune année n'est trouvée.
        """
        #match = re.search(r'\b(19|20)\d{2}\b', str(valeur))
        #if match:
            #return match.group(0)  # Si une année est trouvée, la retourner
        #else:
            #return np.nan  # Si aucune année n'est trouvée
    

    #def application_extraction_annee(self):
        """
        Applique l'extraction d'année sur la colonne 'annee_prod' du DataFrame.

        Returns:
            pd.DataFrame: DataFrame mis à jour avec une colonne 'annee_prod' contenant les années extraites.
        """
        #self.df['annee_prod'] = self.df['annee_prod'].apply(self.extraction_annee)
        #return self.df
        
    def regrouper_état(self, chaine):
            # Dictionnaire de correspondance entre les états et des catégories restreintes
        catégories_état = {
            "neuf": "Neuf",
            "jamais porté": "Neuf",
            "usure nulle": "Neuf",
            "aucune trace d'usure": "Neuf",
            "bon": "Bon",
            "légères traces d'usure": "Bon",
            "traces d'usure visibles": "Satisfaisant",
            "modéré": "Satisfaisant",
            "satisfaisant": "Satisfaisant",
            "fortement usagé": "Usé",
            "traces d'usure importantes": "Usé",
            "défectueux": "Défectueux",
            "incomplet": "Défectueux",
            "pas fonctionnelle": "Défectueux"
        }
        if isinstance(chaine, str):  # Vérifier si la chaîne est bien une chaîne de caractères
            chaine = chaine.lower()  # Convertir la chaîne en minuscule
            états_trouvés = []
            
            # Chercher les mots-clés de l'état dans la chaîne
            for mot_clé, catégorie in catégories_état.items():
                if mot_clé in chaine:
                    états_trouvés.append(catégorie)  # Ajouter la catégorie correspondante
            
            if états_trouvés:
                # Retourner la catégorie la plus représentative (par exemple, la plus courante)
                return max(set(états_trouvés), key=états_trouvés.count)
            else:
                return 'État non spécifié'
        else:
            return 'État non spécifié'  # Retourner "État non spécifié" si ce n'est pas une chaîne de caractères
    
    
    
    def regroupement_etat_montres(self):
        self.df['etat'] = self.df['etat'].apply(self.regrouper_état) 
        return self.df
    
    
 
    def extraire_elements_avant_euro(self, chaine):
        """
        Extrait les deux éléments avant le symbole '€' dans une chaîne.

        Args:
            chaine (str): La chaîne de texte contenant les informations de prix.

        Returns:
            list: Liste contenant jusqu'à deux éléments avant le symbole '€', ou une liste vide si non trouvé.
        """
        if isinstance(chaine, str):
            # Séparer par virgules et espaces, enlever les espaces inutiles
            elements = re.split(r'[,\s]+', chaine.strip())
            
            # Trouver la position de '€' et extraire les deux éléments précédents si présents
            if '€' in elements:
                index_fin = elements.index('€')
                return elements[max(0, index_fin - 2):index_fin]  # Retourne jusqu'à deux éléments avant '€'
            
        return []  # Retourne une liste vide si chaîne non valide ou pas de symbole '€'

    def extraction_elements_avant_euro(self):
        """
        Applique l'extraction des éléments avant '€' sur la colonne 'prix' et met à jour le DataFrame.

        Returns:
            pd.DataFrame: DataFrame mis à jour avec les éléments extraits dans la colonne 'prix'.
        """
        self.df['prix'] = self.df['prix'].apply(self.extraire_elements_avant_euro)
        return self.df

    
    def nettoyer_valeurs(self, colonne):
        """
        Nettoie et convertit les éléments de la colonne spécifiée en nombres.

        Args:
            colonne (str): Le nom de la colonne contenant les valeurs à nettoyer.

        Returns:
            pd.DataFrame: DataFrame avec la colonne spécifiée convertie en nombres.
        """
        valeurs_nettoyees = []
        
        for val in self.df[colonne]:
            if isinstance(val, list):  # Vérifier si val est une liste
                # Joindre les parties numériques en une seule chaîne
                nombre_str = ''.join(val)
                
                # Nettoyer et convertir en nombre si possible
                if nombre_str.strip():
                    try:
                        # Conversion en int si possible, sinon float
                        nombre = int(nombre_str) if '.' not in nombre_str else float(nombre_str)
                    except ValueError:
                        nombre = np.nan
                else:
                    nombre = np.nan
            else:
                nombre = np.nan  # Gérer les valeurs non liste
            
            valeurs_nettoyees.append(nombre)

        # Mettre à jour la colonne avec les valeurs nettoyées
        self.df[colonne] = valeurs_nettoyees
        return self.df
    
    
    
    def extraction_int(self, chaine):
        """
        Extrait le premier chiffre entier trouvé dans une chaîne de caractères.

        Args:
            chaine (str): La chaîne à analyser.

        Returns:
            int ou NaN: Le premier nombre entier trouvé ou NaN si aucun nombre n'est trouvé.
        """
        if pd.isna(chaine):
            return np.nan
        match = re.search(r'\d+', chaine)
        return int(match.group()) if match else np.nan

    def extraction_integer(self):
        """
        Extrait le premier nombre entier de la colonne et l'ajoute sous forme d'entier.
        
        Returns:
            pd.DataFrame: DataFrame mis à jour avec la colonnemodifiée.
        """
        # Utiliser directement apply avec extraire_chiffre pour plus de clarté
        self.df['reserve_de_marche'] = self.df['reserve_de_marche'].apply(self.extraction_int)
        self.df['etencheite'] = self.df['etencheite'].apply(self.extraction_int)
        self.df['diametre'] = self.df['diametre'].apply(self.extraction_int)
        
        return self.df
    