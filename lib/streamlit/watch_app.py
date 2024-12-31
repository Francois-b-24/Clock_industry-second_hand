import streamlit as st
import pandas as pd
import sqlite3
import numpy as np
import plotly.express as px

# Lien Google Drive modifié pour le téléchargement direct
url = "https://drive.google.com/uc?id=1FI7ad6nMwtH2grh8fvHk8zbsXMhoJj0R"

try:
    # Charger avec des options pour ignorer les erreurs
    df = pd.read_csv(url, usecols=lambda column: column != 'Unnamed: 0')
except Exception as e:
    st.error(f"Erreur lors du chargement des données : {e}")
    
if df.empty:
    st.error("Impossible de charger les données. Vérifiez les chemins ou la base de données.")
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
        fig = px.scatter(df, x=feature, y="prix_sc", title=f"Prix en fonction de {feature}")
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
        fig = px.bar(prix_moyens, x=feature, y="prix_sc",
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

st.header("Prix moyen observé d'un modèle en particulier")
# Affichage du résultat
st.write(f"Le prix moyen de la montre {modele_selectionne} de la marque {marque_selectionnee} est de {prix_moyen:.2f} €.")


# Footer
st.write("---")
st.write("Application développée avec ❤️ par GaboneseWrist")