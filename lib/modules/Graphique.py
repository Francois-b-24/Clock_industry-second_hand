import pandas as pd
from rich.table import Table
from rich.console import Console
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import seaborn as sns

class graphique:
    def __init__(self, df):
        self.df = df
     
    
    def effectif_pays(self):
        effectif = self.df.pays.value_counts().reset_index(name="effectif")
        effectif.columns = ['pays', 'nbre_montre']
        effectif['pourcentage'] = (effectif['nbre_montre'] / effectif['nbre_montre'].sum())*100
        effectif['pourcentage'] = effectif['pourcentage'].round(2)
        return effectif
        
    def fig_pays(self, effectif):

        fig = px.choropleth(effectif, 
                            locations="pays",  # Nom de la colonne contenant les pays
                            locationmode="country names",  # Mode pour faire correspondre les noms des pays
                            color="nbre_montre",  # Colonne des effectifs
                            color_continuous_scale="Plasma",  # Palette de couleurs
                            hover_name="pays",
                            title="Provenance des offres")

        return fig.show()
    
    def tab_pays(self, effectif):
        # Créer une console pour afficher le tableau
        console = Console()

        # Initialiser la table
        table = Table(title="Provenance des montres")

        # Ajouter les colonnes
        table.add_column("Pays", justify="center", style="cyan", no_wrap=True)
        table.add_column("Nombre de montres", justify="right", style="magenta")
        table.add_column("%", justify="right", style="green")

        # Ajouter les lignes du tableau à partir des données
        for _, row in effectif.iterrows():
            table.add_row(
                str(row["pays"]),
                str(row["nbre_montre"]),
                f"{row['pourcentage']:.2f} %"
            )

        # Afficher la table
        console.print(table)
    
    def stat_pays(self, effectif):
        stats_localisation = self.df.groupby('pays')['prix'].agg(['mean', 'min', 'max']).reset_index()
        stats_localisation.columns = ['pays', 'prix_moyen', 'prix_min', 'prix_max']
        stats_localisation_merge = pd.merge(stats_localisation, effectif, how='left')
        stats_localisation_merge['prix_moyen'] = stats_localisation_merge['prix_moyen'].round(2)
        stats_localisation_sorted = stats_localisation_merge.sort_values(by="prix_moyen", ascending=False).reset_index(drop=True)
        return stats_localisation_sorted
    
    def tab_pays_2(self, stats_localisation_sorted):
        # Créer une console pour afficher le tableau
        console = Console()

        # Initialiser la table
        table = Table(title="Statistiques par pays")

        # Ajouter les colonnes
        table.add_column("Pays", justify="center", style="cyan", no_wrap=True)
        table.add_column("Nombre de montres", justify="right", style="magenta")
        table.add_column("Prix moyen", justify="right", style="green")
        table.add_column("Prix min", justify="right", style="yellow")
        table.add_column("Prix max", justify="right", style="red")

        # Ajouter les lignes du tableau à partir des données
        for _, row in stats_localisation_sorted.iterrows():
            table.add_row(
                str(row["pays"]),
                str(row["nbre_montre"]),
                f"{row['prix_moyen']:.2f}",
                f"{row['prix_min']:.2f}",
                f"{row['prix_max']:.2f}"
            )

        # Afficher la table
        console.print(table)
    
    def tableau(self, df, colonne):
        # Calcul des statistiques
        stat = self.df.groupby(colonne)['prix'].agg(['mean', 'min', 'max']).reset_index()
        stat.columns = [colonne, 'prix_moyen', 'prix_min', 'prix_max']
        stat['prix_moyen'] = stat['prix_moyen'].round(2)
        stat = stat.sort_values(by="prix_moyen", ascending=False).reset_index(drop=True)

        # Créer une console pour afficher le tableau
        console = Console()

        # Initialiser la table
        table = Table(title=f"Statistiques par {colonne.capitalize()}")

        # Ajouter les colonnes
        table.add_column(colonne.capitalize(), justify="center", style="cyan", no_wrap=True)
        table.add_column("Prix moyen", justify="right", style="green")
        table.add_column("Prix min", justify="right", style="yellow")
        table.add_column("Prix max", justify="right", style="red")

        # Ajouter les lignes du tableau à partir des données
        for _, row in stat.iterrows():
            table.add_row(
                str(row[colonne]),
                f"{row['prix_moyen']:.2f}",
                f"{row['prix_min']:.2f}",
                f"{row['prix_max']:.2f}"
            )

        # Afficher la table
        console.print(table)
    
    
    

    def boxplot(self, colonne):
        """
        Crée un boxplot interactif avec Matplotlib et Seaborn pour visualiser la distribution des prix selon une colonne donnée.
        
        Args:
            df (pd.DataFrame): Le DataFrame contenant les données.
            colonne (str): Le nom de la colonne pour la segmentation (par ex. 'Marque').
        
        Returns:
            None: Affiche directement le boxplot.
        """
        # Créer une figure et des axes
        plt.figure(figsize=(10, 8))
        
        # Créer le boxplot avec Seaborn
        sns.boxplot(
            data=self.df,
            x='prix_log',
            y=colonne,
            palette="Set2"  # Palette de couleurs
        )
        
        # Ajouter des titres et labels
        plt.title(f'Distribution des prix selon {colonne}', fontsize=14, fontweight='bold')
        plt.xlabel('Prix des montres (en log)', fontsize=12)
        plt.ylabel(colonne, fontsize=12)
        
        # Ajuster l'espacement
        plt.tight_layout()
        
        # Afficher le graphique
        plt.show()
        
 

    def barres(self, colonne):
        """
        Crée un barplot avec Matplotlib et Seaborn pour visualiser le prix moyen par catégorie.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données.
            colonne (str): Le nom de la colonne pour la segmentation (par ex. 'marque').

        Returns:
            None: Affiche directement le barplot.
        """
        # Calculer le prix moyen par catégorie
        df_grouped = self.df.groupby(colonne)['prix'].mean().reset_index()
        df_grouped = df_grouped.sort_values(by='prix', ascending=True)

        # Créer une figure et des axes
        plt.figure(figsize=(10, 8))

        # Créer le barplot avec Seaborn
        sns.barplot(
            data=df_grouped,
            x='prix',
            y=colonne,
            palette="viridis"  # Palette de couleurs
        )

        # Ajouter des titres et labels
        plt.title(f'Prix moyen par {colonne}', fontsize=14, fontweight='bold')
        plt.xlabel('Prix moyen (en euros)', fontsize=12)
        plt.ylabel(f'Types de {colonne}', fontsize=12)

        # Ajuster l'espacement
        plt.tight_layout()

        # Afficher le graphique
        plt.show()
