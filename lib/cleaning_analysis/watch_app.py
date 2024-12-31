import streamlit as st
import pandas as pd
from nettoyage import Nettoyage
import sqlite3
import numpy as np
import plotly.express as px

# Configuration de la mise en cache pour accélérer l'application
@st.cache_data
def chargement_base(path_load, path_save):
    """Charge les données depuis une base SQLite et les sauvegarde en CSV."""
    try:
        # Connexion à la base de données
        connexion = sqlite3.connect(path_load)
        df = pd.read_sql_query("SELECT * FROM montre", connexion)
        connexion.close()

        # Sauvegarder au format CSV
        df.to_csv(path_save, index=False)

        # Supprimer la colonne ID
        df = df.iloc[:, 1:]
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement des données : {e}")
        return pd.DataFrame()

# Charger les données
path_load = "data_app/montre.db"
path_save = "data_app/base.csv"

df = chargement_base(path_load, path_save)

if df.empty:
    st.error("Impossible de charger les données. Vérifiez les chemins ou la base de données.")
    st.stop()

# Nettoyage des données
nettoyage = Nettoyage(df)

try:
    df = nettoyage.nettoyage_colonnes()

    colonnes_a_renseigner = [
        "matiere_boitier", "matiere_bracelet", "sexe", "diametre", "etencheite",
        "matiere_lunette", "matiere_verre", "boucle", "matiere_boucle", "rouage",
        "reserve_de_marche", "mouvement"
    ]

    for col in colonnes_a_renseigner:
        df = nettoyage.remplissage(col)

    df = nettoyage.remplissage_mouvement()
    df = nettoyage.remplissage_reserve_marche()
    df = nettoyage.compteur_complications("fonctions")
    df = nettoyage.suppression_colonnes()
    df = nettoyage.mise_en_forme()
    df = nettoyage.nettoyer_matiere_boitier()
    df = nettoyage.matiere()
    df = nettoyage.mapping_matiere()
    df = nettoyage.regroupement_etat_montres()
    df = nettoyage.extraction_elements_avant_euro()
    df = nettoyage.nettoyer_valeurs("prix")
    df = nettoyage.extraction_integer()
except Exception as e:
    st.error(f"Erreur lors du nettoyage des données : {e}")
    st.stop()
    
    
# Création d'une variable de prix en logarithme qui nous sera utile lors de modélisation pour respecter les hypothèses des modèles.
df['prix_log'] = np.log(df['prix'])

# Création d'une variable de prix déflatée de 6%. ==> 
# Tiens compte de la commission appliquée sur le prix de vente
df['prix_sc'] = df['prix']*(1-0.06)

# Titre de l'application
st.title("Analyse des montres de luxe et influence des caractéristiques sur le prix")

# Récupération du nombre de lignes et de colonnes
n_lignes, n_colonnes = df.shape

# Affichage du nombre de lignes et de colonnes
st.write(f"La base de données contient {n_lignes} lignes et {n_colonnes} colonnes. Les données ont été récupérées sur le site chrono24. C'est une plateforme pour acheter ou vendre des montres de luxe d'occasion. En l'occurrence, la base de données contient des informations sur les montres mises en vente.")


# 1. Affichage des données brutes
st.header("Données")
st.write("Voici un aperçu des données disponibles :")
st.dataframe(df.head(10))


# Distribution des prix et deuxième graphique
st.subheader("Distribution des prix")

# Création de deux colonnes pour afficher les graphiques côte à côte
col1, col2 = st.columns(2)

# Premier graphique : Distribution des prix
with col1:
    st.write("**Distribution des prix**")
    fig1 = px.histogram(df, x="prix_sc", nbins=30, title="Distribution des prix des montres")
    fig1.update_layout(xaxis_title="Prix (€)", yaxis_title="Fréquence")
    st.plotly_chart(fig1)

# Deuxième graphique : Prix moyen par matériau de boîtier (par exemple)
with col2:
    st.write("**Distribution des prix (en log)**")
    fig2 = px.histogram(df, x="prix_log", nbins=30, title="Distribution des prix des montres")
    fig2.update_layout(xaxis_title="Prix (€)", yaxis_title="Fréquence")
    st.plotly_chart(fig2)

st.write('Dans le contexte de l’analyse des prix des montres, l’utilisation d’une échelle logarithmique présente plusieurs avantages : \n Gestion des variations extrêmes : Les prix des montres peuvent varier considérablement, allant de modèles abordables à des pièces de luxe très coûteuses. Une échelle logarithmique permet de compresser les valeurs élevées et d’étendre les valeurs plus faibles, offrant ainsi une vue d’ensemble plus équilibrée de la distribution des prix.')

# Identifier les colonnes catégorielles et numériques
colonnes_numeriques = df.select_dtypes(include=['number']).columns
colonnes_categorielles = df.select_dtypes(include=['object']).columns

# 3. Influence des caractéristiques sur le prix
st.header("Influence des caractéristiques sur le prix")
feature = st.selectbox(
    "Choisissez une caractéristique :",
    options=[col for col in df.columns if col != "prix"]
)

# Disposition des graphiques côte à côte
col1, col2 = st.columns(2)

if feature in colonnes_numeriques:
    # Nuage de points (scatter plot) pour les variables numériques
    with col1:
        st.subheader(f"Relation entre {feature} et le prix")
        fig = px.scatter(df, x="prix_sc", y=feature, title=f"Prix en fonction de {feature}")
        fig.update_layout(xaxis_title=feature, yaxis_title="Prix (€)")
        st.plotly_chart(fig)
else:
    # Calculer les prix moyens par catégorie
    prix_moyens = df.groupby(feature)['prix'].mean().reset_index()

    # Trier par prix moyen en ordre décroissant
    prix_moyens = prix_moyens.sort_values(by='prix', ascending=False)


    # Diagramme en barres (bar plot) pour les variables catégorielles
    with col1:
        st.subheader(f"Répartition du prix selon {feature}")
        # Créer un diagramme en barres avec Plotly
        fig = px.bar(prix_moyens, x='prix', y=feature,
             title=f"Prix moyen par {feature}",
             labels={feature: feature, 'prix': 'Prix moyen (€)'},
             color=feature,
             color_continuous_scale='Viridis')
        # Afficher le graphique dans Streamlit
    st.plotly_chart(fig)

# Liste des marques disponibles
marques = df['marque'].unique()

# Sélection de la marque
marque_selectionnee = st.selectbox("Choisissez une marque :", marques)

# Liste des modèles disponibles pour la marque sélectionnée
modeles = df[df['marque'] == marque_selectionnee]['modele'].unique()

# Sélection du modèle
modele_selectionne = st.selectbox("Choisissez un modèle :", modeles)

# Filtrage des données en fonction de la marque et du modèle sélectionnés
montres_selectionnees = df[(df['marque'] == marque_selectionnee) & (df['modele'] == modele_selectionne)]

# Calcul du prix moyen
prix_moyen = montres_selectionnees['prix'].mean()

# Affichage du résultat
st.write(f"Le prix moyen de la montre {modele_selectionne} de la marque {marque_selectionnee} est de {prix_moyen:.2f} €.")


# Footer
st.write("---")
st.write("Application développée avec ❤️ par GaboneseWrist")