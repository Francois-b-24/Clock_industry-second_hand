#!/usr/bin/env python
# coding: utf-8

# In[509]:


import sqlite3
import pandas as pd
import seaborn as sns
from collections import defaultdict
import math


# In[510]:


# Connexion à la base de données SQLite
connexion = sqlite3.connect('/Users/f.b/Desktop/Data_Science/Watches/montre.db')

# Exécution d'une requête SQL pour récupérer des données et conversion en DataFrame
df = pd.read_sql_query('SELECT * FROM montre', connexion)

# Fermeture de la connexion
connexion.close()


# In[511]:


df.to_csv('/Users/f.b/Desktop/Data_Science/Watches/watches_database.csv')
df = pd.read_csv('/Users/f.b/Desktop/Data_Science/Watches/watches_database.csv')

# Chargement de la BDD :
df = df.iloc[:, 1:]

# Suppression des doublons :
df = df.drop_duplicates(subset=['marque','modele','etat','prix','ville'], keep ='first')


# # Etude et traitement des valeurs manquantes :

# In[512]:


# Aperçu des valeurs manquantes
print(df.isnull().sum())

# Heatmap des valeurs manquantes
sns.heatmap(df.isnull(), cbar=False)

print(round(df.isnull().sum()/len(df)*100,2))


# In[513]:


# Retrait des lignes pour lesquelles les valeurs des variables ne sont pas renseignées.
# Au vue de la faible proportion que représentent ces lignes, on peut simplement les retirées sans crainte de perte d'information consiérable.

df = df.dropna(subset=['marque','modele','mouvement'])


# In[514]:


# Fonction pour remplir les valeurs manquantes de certaines variables en fonction de la marque, du modèle et du mouvement
def remplissage(df, variable):
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


# In[515]:


# Fonction pour remplir les valeurs manquantes de la variable mouvement qui sont égales à = [] en fonction de la marque, du modèle et du mouvement
def remplissage_mouvement(df, variable):
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


# In[516]:


def remplissage_reserve_marche(df, variable):
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


# In[517]:


df = remplissage_reserve_marche(df, 'reserve_de_marche')


# In[518]:


df = remplissage(df,'matiere_boitier')
df = remplissage(df,'matiere_bracelet')
df = remplissage(df,'sexe')
df = remplissage(df, 'diametre')
df = remplissage(df,'etencheite')
df = remplissage(df, 'matiere_lunette')
df = remplissage(df, 'matiere_verre')
df = remplissage(df,'boucle')
df = remplissage(df,'matiere_boucle')
df = remplissage(df,'rouage')
df = remplissage(df, 'reserve_de_marche')


# In[519]:


df = remplissage_mouvement(df, 'mouvement')


# In[520]:


df = df.dropna(subset=['rouage','etencheite','matiere_bracelet', 'etat', 'sexe', 'diametre', 'matiere_lunette',
                       'matiere_verre', 'boucle'])


# In[521]:


# Création d'une fonction pour la création d'une variable pour compter le nombre de complications que possède la montre
def count_functions(fonction_string):
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



# In[522]:


df['Complications'] = df['fonctions'].apply(count_functions)


# In[523]:


def remplissage_mouvement_bis(df, variable):
    for index, row in df.iterrows():
        if row[variable] == '[]':
            df.at[index, variable] = 'Quartz'
    return df


# In[524]:


def remplissage_mat_verre(df, variable):
    for index, row in df.iterrows():
        if row[variable] == '[]':
            df.at[index, variable] = 'Inconnue'
    return df


# In[525]:


def remplissage_reserve_marche_bis(df, variable):
    # Utiliser des opérations vectorisées pour identifier les lignes à modifier
    masque = (df[variable].isna()) & (df['mouvement'] == 'Quartz')

    # Appliquer la valeur 'Pas_de_reserve' aux lignes identifiées
    df.loc[masque, variable] = 'Pas_de_reserve'

    return df


# In[526]:


df = remplissage_mouvement_bis(df, 'mouvement')


# In[527]:


df = remplissage_mat_verre(df, 'matiere_verre')


# In[528]:


df = remplissage_reserve_marche_bis(df, 'reserve_de_marche')


# In[529]:


df = df[df['matiere_boitier'] != '[]']
df = df.drop(columns=['rouage', 'fonctions','id'])
df = df.dropna(subset=['reserve_de_marche'])


# # Traitement de la variable *Marque* :

# In[530]:


df['marque'] = df['marque'].astype(str)
marque = [i.replace('&', '') for i in df['marque']]
marque = [i.strip("['").strip("']")for i in marque]
marque = [i.replace(',','') for i in marque]
marque = [i.replace("''",'').replace(" ",'').replace("''",'_').replace('-','_') for i in marque]
marque = [i.upper() for i in marque]
df['marque'] = marque
df['marque'] = df['marque'].astype('category')


# # Traitement de la variable *Modele* :

# In[531]:


modele = [i.strip("['").strip("']").replace(',','').replace(' ','').replace("''","_") for i in df['modele']]
modele = [i.upper() for i in modele]
df['modele'] = modele
df['modele'] = df['modele'].astype('category')


# In[532]:


# Fonction pour extraire la base commune
def extraire_base(mot):
    mots = mot.split('_')
    return mots[0] if mots else mot

# Dictionnaire pour regrouper les modalités par base
groupes = defaultdict(list)

for mot in df['modele']:
    base = extraire_base(mot)
    groupes[base].append(mot)

# Créer une nouvelle liste avec les groupes remplacés par leurs noms
modalites_regroupees = []
for mot in df['modele']:
    base = extraire_base(mot)
    modalites_regroupees.append(base)


# In[533]:


seuil = 100
freq_apparition = df['modele'].value_counts()
df['modele'] = df['modele'].apply(lambda x: x if freq_apparition[x] > seuil else 'RARE')


# In[534]:


mapping = {
    'DATEJUST_36' : 'DATEJUST',
    'DATEJUST_41' : 'DATEJUST',
    'DATEJUST_31' : 'DATEJUST',
    'BLACK_BAY_FIFTY-EIGHT' : 'BLACK_BAY',
    'OYSTER_PERPETUAL_36' : 'OYSTER_PERPETUAL',
    'OYSTER_PERPETUAL_31': 'OYSTER_PERPETUAL',
    'LADY-DATEJUST' : 'LADY_DATEJUST'
}

df['modele'] = df['modele'].replace(mapping)


# # Traitement de la variable *Mouvement* :

# In[535]:


df['mouvement'] = df['mouvement'].replace({"['28000', 'A/h']": "automatique"})
mouvement = [i.strip("['").strip("']").replace(',','').replace(' ','') for i in df['mouvement']]
mouvement = [i.upper() for i in mouvement]
df['mouvement'] = mouvement
df['mouvement'] = df['mouvement'].astype('category')


# # Traitement de la variable *Matiere_bracelet*

# In[536]:


matiere_bracelet = [i.replace('mm)','Inconnue').replace("mm",'Inconnue').replace('/',"_") for i in df['matiere_bracelet']]
matiere_bracelet =  [i.upper() for i in matiere_bracelet]
df['matiere_bracelet'] = matiere_bracelet
df['matiere_bracelet'] = df['matiere_bracelet'].astype('category')

valeurs_a_remplacer = ['BRACELET','ROSE','NOIR','JAUNE',
                       'BRUN','BLANC','VERT','GRIS','BLEU','BORDEAUX',
                       'BEIGE']
df['matiere_bracelet'] = df['matiere_bracelet'].replace(valeurs_a_remplacer, "INCONNUE")
df['matiere_bracelet'] = df['matiere_bracelet'].replace({"D'AUTRUCHE" : "CUIR_AUTRUCHE",
                                                         "ARGENTÉ" : "ARGENT",
                                                         "VACHE" : "CUIR_VACHE"})


# # Traitement de la variable *Matiere_boitier*

# In[537]:


matiere_boitier = [i.strip(']').strip('[') for i in df['matiere_boitier']]
matiere_boitier = [i.replace("', '","_").replace("'",'').replace('/','_') for i in matiere_boitier]
matiere_boitier = [i.upper() for i in matiere_boitier]
df['matiere_boitier'] = matiere_boitier
df['matiere_boitier'] = df['matiere_boitier'].astype('category')


# # Traitement de la variable Etat

# In[538]:


etat = [i.replace("[", "").replace("]", "").replace("'", "").replace("\"", "").replace("(", "").replace(")", "") for i in df['etat']]
etat = [i.split(",")[0:2] for i in etat]
etat = [''.join(sous_liste).strip() for sous_liste in etat]
df['etat'] = etat
df['etat'] = df['etat'].astype('category')


# In[539]:


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

# Exemple d'utilisation
modalites_simplifiees = [obtenir_nouvelle_modalite(mod) for mod in df['etat']]


# In[540]:


df['etat'] = modalites_simplifiees


# In[541]:


mapping = {
    'Neuf/Très bon' : 'NEUF',
    'Bon/Satisfaisant' : 'SATISFAISANT',
    'Incomplet' :'INDETERMINE',
    'Autre' :'INDETERMINE',
    'Défectueux' : 'DEFECTUEUX'
}


df['etat'] = df['etat'].replace(mapping)


# # Traitement de la variable sexe

# In[542]:


df['sexe'] = df['sexe'].replace({'homme/Unisexe' : "HOMME"})
sexe = [i.upper() for i in df['sexe']]
df['sexe'] = sexe
df['sexe'] = df['sexe'].astype('category')


# # Traitement de la variable prix :

# In[543]:


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


# In[544]:


df['prix'] = df['prix'].apply(extraire_elements_avant_euro)


# In[545]:


prix = [''.join(sous_liste).strip() for sous_liste in df['prix']]
prix = [i.replace("(=","") for i in prix]
df['prix'] = prix
df = df[df['prix'] != '']
df['prix'] = df['prix'].astype('float')


# # Traitement de la variable réserve de marche

# In[546]:


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
    return liste


# In[547]:


df['reserve_de_marche'] = df['reserve_de_marche'].apply(extraire_elements_h)


# In[548]:


df['reserve_de_marche'].value_counts()


# # Traitement de la variable diametre

# In[550]:


diametre = [i.split(',')[0] for i in df['diametre']]
diametre = [i.replace(" ","") for i in diametre]
diametre = [i.replace('[',"").replace("]","").replace("'",'').replace("mm","") for i in diametre]
df['diametre'] = diametre
df['diametre'] = df['diametre'].astype('float')
diametre = [math.ceil(i) for i in df['diametre']]
df['diametre'] = diametre


# # Traitement de la varaible etencheite

# In[551]:


etencheite = [i.split(',')[0] for i in df['etencheite']]
etencheite = [i.replace('[',"").replace("]","").replace("'",'').replace('Non', '0').replace('Au-delà','0') for i in etencheite]
df['etencheite'] = etencheite
df['etencheite'] = df['etencheite'].astype('float')


# # Traitement de la variable matiere_lunette

# In[553]:


valeurs_a_remplacer = ['rose','jaune','blanc','rouge']
df['matiere_lunette'] = df['matiere_lunette'].replace(valeurs_a_remplacer, 'Indetermine')
matiere_lunette = [i.replace("/","_").upper() for i in df['matiere_lunette']]
df['matiere_lunette'] = matiere_lunette
df['matiere_lunette'] = df['matiere_lunette'].astype('category')


# # Traitement de la variable matiere_verre

# In[554]:


matiere_verre = [i.upper() for i in df['matiere_verre']]
matiere_verre = [i.replace("[","").replace("]","").replace("'","") for i in df['matiere_verre']]
df['matiere_verre'] = matiere_verre
df['matiere_verre'] = df['matiere_verre'].astype('category')


# # Traitement de la variable boucle

# In[555]:


boucle = [i.strip(",").replace("[","").replace("]","").replace("'","").replace(",","").replace(" ","_") for i in df['boucle']]
boucle = [i.upper() for i in boucle]
df['boucle'] = boucle
df['boucle'] = df['boucle'].astype('category')


# # Traitement de la variable matiere_boucle

# In[556]:


matiere_boucle = [i.strip(",").replace("[","").replace("]","").replace("'","").replace(",","").replace(" ","_").replace("/","_") for i in df['matiere_boucle']]
matiere_boucle = [i.upper() for i in matiere_boucle]
df['matiere_boucle'] = matiere_boucle
df['matiere_boucle'] = df['matiere_boucle'].astype('category')


# # Traitement de la variable ville

# In[557]:


pays = [i.replace('[','').replace(']','').replace("'","") for i in df['ville']]
pays = [i.split(',')[0] for i in pays]
pays = [i.upper() for i in pays]
df = df.drop(columns=['ville'])
df['pays'] = pays
df['pays'] = df['pays'].astype('category')


# In[559]:


df['pays'].unique().tolist()


# In[560]:


mapping = {
    'RÉPUBLIQUE' : 'RÉPUBLIQUE_TCHEQUE',
    'HONG' : 'HONG_KONG',
    'VIÊT' : 'VIETNAM',
    'PORTO' : 'PORTUGAL',
    'E.A.U.' : 'EMIRAT_ARABE_UNIS',
    'SRI': 'SRI_LANKA',
    'ARABIE' : 'ARABIE_SAOUDITE'
}

df['pays'] = df['pays'].replace(mapping)


# In[563]:


df['Complications'].astype('category')


# In[564]:


df['Date_recup'] = pd.to_datetime(df['Date_recup'])


# # Sauvegarde la BDD nettoyée :

# In[565]:


df.to_csv('/Users/f.b/Desktop/Data_Science/Watches/data_clean.csv')

