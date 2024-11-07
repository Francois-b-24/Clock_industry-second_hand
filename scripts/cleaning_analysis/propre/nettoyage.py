import pandas as pd
import numpy as np
import re
from datetime import datetime
from collections import defaultdict
import math

class Nettoyage:
    def __init__(self, df):
        self.df = df
    
    
    def nettoyer_colonnes(self, majuscule=True, remplacer_nan=np.nan) -> pd.DataFrame:
        for col in self.df.columns:
            # Supprimer les crochets et apostrophes dans les chaînes de caractères
            self.df[col] = self.df[col].apply(lambda x: re.sub(r"[\[\]'\"()]", '', str(x)) if isinstance(x, str) else x)
            
            # Supprimer les espaces multiples
            self.df[col] = self.df[col].apply(lambda x: re.sub(r'\s+', ' ', x).strip() if isinstance(x, str) else x)
        
        # Supprimer la colonne 'Date_recup' si elle existe
        if 'Date_recup' in self.df.columns:
            self.df.drop(columns='Date_recup', inplace=True)
        
        # Supprimer les doublons
        self.df.drop_duplicates(inplace=True)
        
        for col in self.df.columns:
            # Remplacer les valeurs NaN par la valeur spécifiée
            self.df[col] = self.df[col].fillna(remplacer_nan)
            
            # Supprimer les espaces en début et fin de chaîne si c'est une colonne de type chaîne
            if self.df[col].dtype == 'object':
                self.df[col] = self.df[col].str.strip()
                
                # Convertir en majuscule ou minuscule selon le paramètre
                if majuscule:
                    self.df[col] = self.df[col].str.upper()
                else:
                    self.df[col] = self.df[col].str.lower()
        
        # Remplacer les chaînes vides et 'None' par NaN
        self.df.replace('', np.nan, inplace=True)
        self.df.replace('None', np.nan, inplace=True)
        
        # Suppression des lignes pour lesquelles on ne connait pas la marque et le modèle
        self.df = self.df.dropna(subset=['marque','modele'])
        
        return self.df
    
    
    
    
    def remplissage(self, variable):
        # Grouper par 'marque', 'modele', 'mouvement' pour trouver les valeurs similaires
        groupes_similaires = self.df.groupby(['marque', 'modele', 'mouvement'])[variable]

        # Remplir les valeurs manquantes avec la première valeur non manquante des groupes similaires
        self.df[variable] = self.df[variable].fillna(groupes_similaires.transform('first'))

        return self.df
    
    
    
    
    
    def remplissage_mouvement(self):
        # Remplacer les listes vides par des NaN (si applicable, sinon cette étape peut être ignorée)
        self.df['mouvement'] = self.df['mouvement'].apply(lambda x: np.nan if isinstance(x, list) and not x else x)

        # Grouper par 'marque' et 'modele' pour trouver les valeurs similaires
        groupes_similaires = self.df.groupby(['marque', 'modele'])['mouvement']

        # Remplir les valeurs manquantes avec la première valeur non manquante des groupes similaires
        self.df['mouvement'] = self.df['mouvement'].fillna(groupes_similaires.transform('first'))

        return self.df
    
    
    
    
    def remplissage_reserve_marche(self):
        # Remplir 'Pas_de_reserve' pour les lignes où 'rouage' commence par 'Quar' ou 'ETA' et où 'variable' est NaN ou vide
        masque_quartz_eta = (
            (self.df['reserve_de_marche'].isna() | (self.df['reserve_de_marche'] == '')) &  # Si 'variable' est manquant ou vide
            self.df['rouage'].apply(lambda x: isinstance(x, str) and (x.startswith('Quar') or x.startswith('ETA')))  # Vérifier 'rouage'
        )
        self.df.loc[masque_quartz_eta, 'reserve_de_marche'] = 'Pas_de_reserve'

        # Remplir 'Pas_de_reserve' pour les lignes où 'mouvement' est 'Quartz' et où 'variable' est NaN
        masque_quartz_mouvement = (self.df['reserve_de_marche'].isna() | (self.df['reserve_de_marche'] == '')) & (self.df['mouvement'] == 'Quartz')
        self.df.loc[masque_quartz_mouvement, 'reserve_de_marche'] = 'Pas_de_reserve'

        return self.df
    
    

    def count_functions(self, fonction_string):
        """
        Compte le nombre de complications ou fonctions dans une chaîne donnée.

        Args:
            fonction_string (str): La chaîne à analyser.

        Returns:
            int ou str: Le nombre de fonctions trouvées, ou 'Non_renseignée' si aucune information.
        """
        if pd.isna(fonction_string):
            return 0
        if 'Fonctions\n' in fonction_string:
            fonctions_part = fonction_string.split('Fonctions\n')[1]
            fonctions_list = [func.strip() for func in fonctions_part.split(',')]
            return len(fonctions_list)
        elif 'Autres\n' in fonction_string:
            fonctions_part = fonction_string.split('Autres\n')[1]
            fonctions_list = [func.strip() for func in fonctions_part.split(',')]
            return len(fonctions_list)
        else:
            return 'Non_renseignée'

    def add_function_count_column(self, column_name):
        """
        Ajoute une colonne au DataFrame contenant le nombre de fonctions pour chaque entrée.

        Args:
            column_name (str): Le nom de la colonne à traiter dans le DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame mis à jour avec une nouvelle colonne de comptage.
        """
        self.df[f'comptage_{column_name}'] = self.df[column_name].apply(self.count_functions)
        return self.df
        
    

    def suppression_colonnes(self):
        """Fonctions pour supprimer les colonnes inutiles"
        Args:
            df (pd.DataFarme): DataFrame contenant les colonnes à traiter. 
            liste_colonnes (list): Liste des colonnes à traiter.
        
        Returns: 
            pd.DataFrame : DataFrame modifié.  
        """
        colonnes_a_supp = ['rouage', 'fonctions']
        self.df = self.df.drop(columns=colonnes_a_supp)
        
        return self.df
    
    
    
    def nettoyage_marque(self):
        marque = [i.replace(', ', '-').replace('.','') for i in self.df['marque']]
        self.df['marque'] = marque
        

    
    
    def nettoyage_modele(self):
        modele = [i.replace(',','').replace(' ','-') for i in self.df['modele']]
        self.df['modele'] = modele
    
    
    
    def nettoyage_mouvement(self):
        mapping = {"FOND, TRANSPARENT,, INDICATION, DE, LA, RÉSERVE, DE, MARCHE,, ÉTAT, DORIGINE/PIÈCES, ORIGINALES,, COUCHE, PVD/DLC" : "AUTOMATIQUE",
           
        "28000, A/H": "AUTOMATIQUE",
            "REMONTAGE, AUTOMATIQUE": "AUTOMATIQUE",
            "REMONTAGE, MANUEL" : "AUTOMATIQUE",
            "21600, A/H" : "AUTOMATIQUE",
            "REMONTAGE AUTOMATIQUE" : "AUTOMATIQUE",
            "MONTRE, CONNECTÉE" : "BATTERIE",
            "SQUELETTE" : "AUTOMATIQUE"
            } 
           
           
        self.df['mouvement'] = self.df['mouvement'].replace(mapping)
    
        return self.df
    
    
    
    
    def extraire_matiere(self,chaine):
        matières = ["acier", "or/acier", "cuir", "textile", "titane", "caoutchouc", "bronze",
            "silicone", "vache", "autruche", "bronze","plastique", "platine", "céramique","or",
            "aluminium", "argentt", "requin", "caoutchouc", "plastique", "silicone", 
            "céramique", "satin"]
        
        if isinstance(chaine, str):  # Vérifier si la chaîne est une chaîne de caractères
            for matiere in matières:
                if matiere.lower() in chaine.lower():
                    return matiere.upper()
        return 'INCONNUE'
    
    
    
    
    def extraction_matiere_bracelet(self, column_name):
        self.df[f'{column_name}'] = self.df[column_name].apply(self.extraire_matiere)
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
            return 'INCONNUE'  # Retourner 'INCONNUE' si ce n'est pas une chaîne
    
    
    
    def nettoyer_matiere_boitier(self):
        self.df['matiere_boitier'] = self.df['matiere_boitier'].apply(self.nettoyage_matiere_boitier)
        return self.df
    
    
    def extraction_annee(self, valeur):
        """
        Extrait une année au format AAAA (entre 1900 et 2099) à partir d'une chaîne.

        Args:
            valeur (str): La chaîne à analyser.

        Returns:
            int ou NaN: L'année trouvée en tant qu'entier, ou NaN si aucune année n'est trouvée.
        """
        match = re.search(r'\b(19|20)\d{2}\b', str(valeur))
        return int(match.group(0)) if match else np.nan

    def application_extraction_annee(self):
        """
        Applique l'extraction d'année sur la colonne 'annee_prod' du DataFrame.

        Returns:
            pd.DataFrame: DataFrame mis à jour avec une colonne 'annee_prod' contenant les années extraites.
        """
        self.df['annee_prod'] = self.df['annee_prod'].apply(self.extraction_annee)
        return self.df
    
    def nettoyage_sexe(self):
        mapping = {"HOMME/UNISEXE":"HOMME",
           "MONTRE HOMME/UNISEXE":"HOMME",
           "MONTRE, FEMME":"FEMME",
           "MONTRE, HOMME/UNISEXE":"HOMME"  
           }

        self.df['sexe'] = self.df['sexe'].replace(mapping)
        return self.df
    

    
    # Fonction pour regrouper l'état d'une chaîne dans une catégorie restreinte
    
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
    
    
    
    def extraire_chiffre(self, chaine):
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

    def extraction_chiffres_reserve_de_marche(self):
        """
        Extrait le premier nombre entier de la colonne 'reserve_de_marche' et l'ajoute sous forme d'entier.
        
        Returns:
            pd.DataFrame: DataFrame mis à jour avec la colonne 'reserve_de_marche' modifiée.
        """
        # Utiliser directement apply avec extraire_chiffre pour plus de clarté
        self.df['reserve_de_marche'] = self.df['reserve_de_marche'].apply(self.extraire_chiffre)
        return self.df
    
    def extraction_chiffres_etencheite(self):
        self.df['etencheite'] = self.df['etencheite'].apply(self.extraire_chiffre)
        return self.df

    def extraction_chiffres_diametre(self):
        self.df['diametre'] = self.df['diametre'].apply(self.extraire_chiffre)
        return self.df


    #def extraire_matiere_(self, chaine):
        # Liste des matières à rechercher
       # matières = ['ACIER', 'ROSE', 'JAUNE', 'CÉRAMIQUE', 'TITANE', 'BRONZE',
       #         'ALUMINIUM', 'BLANC', 'PLATINE', 'OR/ACIER', 'ARGENT', 'PLASTIQUE',
       #         'TUNGSTÈNE', 'ROUGE', 'CARBONE', 'OR', 'OR, BLANC', 'OR, ROSE', 'OR, JAUNE', 'PLAQUÉE, OR', 
       #         'CÉRAMIQUE, FONCÉE', 'LUNETTE, LISSE', 'OR, ROUGE', 
       #         'ACIER', 'OR, ROSE', 'OR, JAUNE', 'CÉRAMIQUE', 'TITANE', 'BRONZE',
       #         'ALUMINIUM', 'OR, BLANC', 'PLATINE', 'OR/ACIER', 'ARGENT',
       #         'MATIÈRE, PLASTIQUE', 'PLASTIQUE', None, 'TUNGSTÈNE', 'OR, ROUGE',
       #         'CARBONE', 'PLAQUÉE, OR']
    
       # if pd.isna(chaine):  # Vérifier si la chaîne est None ou NaN
       #     return np.nan
        
        # Mettre la chaîne en majuscules pour faciliter la recherche
       # chaine = chaine.upper()
        
        # Rechercher chaque matière dans la chaîne
       # for matiere in matières:
        #    if matiere in chaine:
        #        return matiere  # Retourner la première matière trouvée
        
       # return np.nan 


    def extraction_matiere_lunette(self):
        self.df['matiere_lunette'] = self.df['matiere_lunette'].apply(self.extraire_matiere)
        return self.df
    
    def extraction_matiere_boucle(self):
        self.df['matiere_boucle'] = self.df['matiere_boucle'].apply(self.extraire_matiere)
        return self.df
        
    def nettoyage_matiere_verre_et_boucle(self):
        mapping = {
        'VERRE SAPHIR' : 'SAPHIR',
        'VERRE, MINÉRAL' : 'MINÉRAL',
        'MATIÈRE, PLASTIQUE':'PLASTIQUE',
        'VERRE, SAPHIR':'SAPHIR'
            }
    
        self.df['matiere_verre'] = self.df['matiere_verre'].replace(mapping)
        self.df['boucle'] = self.df['boucle'].str.replace(', ','_')
        return self.df
    
    def nettoyage_ville(self):
        pays = [i.split(',')[0].strip() if isinstance(i, str) else 'INCONNU' for i in self.df['ville']]
        self.df = self.df.drop(columns=['ville'])
        self.df['pays'] = pays
        
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

        self.df['pays'] = self.df['pays'].replace(mapping)
        return self.df