import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gt

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
        fig = go.Figure(data=[go.Table(
            header=dict(values=["Pays", "Nombre de montre","%"],
                        fill_color='paleturquoise',
                        align='center'),
            cells=dict(values=[effectif['pays'], effectif['nbre_montre'], effectif['pourcentage']],
                    fill_color='lavender',
                    align='left'))
        ])

        fig.update_layout(title="Provenance des montres")
        return fig.show()
    
    def stat_pays(self, effectif):
        stats_localisation = self.df.groupby('pays')['prix'].agg(['mean', 'min', 'max']).reset_index()
        stats_localisation.columns = ['pays', 'prix_moyen', 'prix_min', 'prix_max']
        stats_localisation_merge = pd.merge(stats_localisation, effectif, how='left')
        stats_localisation_merge['prix_moyen'] = stats_localisation_merge['prix_moyen'].round(2)
        stats_localisation_sorted = stats_localisation_merge.sort_values(by="prix_moyen", ascending=False).reset_index(drop=True)
        return stats_localisation_sorted
    
    def tab_pays_2(self, stats_localisation_sorted):

        fig = go.Figure(data=[go.Table(
            header=dict(values=["Pays", "Nombre de montre","prix_moyen","prix_min", "prix_max"],
                        fill_color='paleturquoise',
                        align='center'),
            cells=dict(values=[stats_localisation_sorted['pays'],stats_localisation_sorted['nbre_montre'], stats_localisation_sorted['prix_moyen'], stats_localisation_sorted['prix_min'],
                            stats_localisation_sorted['prix_max']],
                    fill_color='lavender',
                    align='left'))
        ])

        fig.update_layout(title="Statistiques par pays")
        return fig.show()
    
    def tableau(self, colonne):
        stat = self.df.groupby(colonne)['prix'].agg(['mean', 'min', 'max']).reset_index()
        stat.columns = [colonne, 'prix_moyen', 'prix_min', 'prix_max']
        stat['prix_moyen'] = stat['prix_moyen'].round(2)
        stat = stat.sort_values(by="prix_moyen", ascending=False).reset_index(drop=True)


        fig = go.Figure(data=[go.Table(
            header=dict(values=[colonne, "prix_moyen","prix_min", "prix_max"],
                        fill_color='paleturquoise',
                        align='center'),
            cells=dict(values=[stat[colonne], stat['prix_moyen'], stat['prix_min'], stat['prix_max']],
                    fill_color='lavender',
                    align='left'))
        ])

        fig.update_layout(title=f"Statistiques par {colonne}")
        return fig.show()
    
    
    def boxplot_plotly(self, colonne):
        """
        Crée un boxplot interactif avec Plotly pour visualiser la distribution des prix selon une colonne donnée.
        
        Args:
            df (pd.DataFrame): Le DataFrame contenant les données.
            colonne (str): Le nom de la colonne pour la segmentation (par ex. 'Marque').
        
        Returns:
            plotly.graph_objects.Figure: Le boxplot interactif.
        """
        # Création du boxplot avec Plotly
        fig = px.box(
            self.df,
            x='prix_log',
            y=colonne,
            color=colonne,  # Colorer en fonction des types
            labels={'prix_log': 'Prix des montres (en log)', colonne: f'{colonne}'},
            title=f'Distribution des prix selon {colonne}',
            height=600,
            width=800,
            template='plotly_white'
        )

        # Personnaliser le design
        fig.update_layout(
            boxmode='group',
            yaxis_title=f'{colonne}',
            xaxis_title='Prix des montres (en log)',
            showlegend=False
        )
    
        return fig.show()
    
    def barres_plotly(self, colonne):
        """
        Crée un barplot interactif avec Plotly pour visualiser le prix moyen par catégorie.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données.
            colonne (str): Le nom de la colonne pour la segmentation (par ex. 'marque').

        Returns:
            plotly.graph_objects.Figure: Le barplot interactif.
        """
        # Calculer le prix moyen par catégorie
        df_grouped = self.df.groupby(colonne)['prix'].mean().reset_index()
        df_grouped = df_grouped.sort_values(by='prix', ascending=True)

        # Création du barplot avec Plotly
        fig = px.bar(
            df_grouped,
            x='prix',
            y=colonne,
            orientation='h',  # Barres horizontales
            color='prix',
            color_continuous_scale='Darkmint',
            labels={'prix': 'Prix moyen (en euros)', colonne: f'Types de {colonne}'},
            title=f'Prix moyen par {colonne}',
            height=600,
            width=800,
            template='plotly_white'
        )
        
        # Personnalisation du graphique
        fig.update_layout(
            xaxis_title='Prix moyen (en euros)',
            yaxis_title=f'Types de {colonne}',
            coloraxis_showscale=False  # Masquer la légende de la barre de couleur
        )
        
        return fig.show()
