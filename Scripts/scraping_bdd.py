import time
import datetime
import requests
import pandas as pd
import numpy as np
import os
import requests
from urllib import request
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementClickInterceptedException
import sqlite3


def create_database():
    conn = sqlite3.connect('/Users/f.b/Desktop/Data_Science/Watches/Scripts/montre.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS montre
                 (id INTEGER PRIMARY KEY, 
                 marque TEXT, 
                 modele TEXT, 
                 mouvement TEXT,
                 matiere_boitier TEXT,
                 matiere_bracelet TEXT, 
                 annee_prod INTEGER,
                 etat TEXT,
                 sexe TEXT,
                 prix TEXT,
                 reserve_de_marche TEXT,
                 diametre TEXT,
                 etencheite TEXT, 
                 matiere_lunette TEXT,
                 matiere_verre TEXT,
                 boucle TEXT, 
                 matiere_boucle TEXT,
                 rouage TEXT,
                 ville TEXT,
                 fonctions TEXT,
                 Date_recup DATE
                 )''')
    conn.commit()
    conn.close()

create_database()


def insert_data(marque, modele, mouvement,matiere_boitier, matiere_bracelet, annee_prod,  etat, sexe, prix, reserve_de_marche, diametre, etencheite, matiere_lunette, matiere_verre, boucle, matiere_boucle, rouage, ville, fonctions, Date_recup):
    conn = sqlite3.connect('/Users/f.b/Desktop/Data_Science/Watches/montre.db')
    c = conn.cursor()
    c.execute("INSERT INTO montre (marque, modele, mouvement,matiere_boitier, matiere_bracelet, annee_prod,  etat, sexe, prix, reserve_de_marche, diametre, etencheite, matiere_lunette, matiere_verre, boucle, matiere_boucle, rouage, ville, fonctions, Date_recup) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
              (marque, modele, mouvement,matiere_boitier, matiere_bracelet, annee_prod,  etat, sexe, prix, reserve_de_marche, diametre, etencheite, matiere_lunette, matiere_verre, boucle, matiere_boucle, rouage, ville, fonctions, Date_recup))
    conn.commit()
    conn.close()


def recuperation_donnees(lien, nb_page=0): 
    
    """ Cette fonction a pour objet : 
    - Se rendre sur la page d'acceuil du site Chrono24 
    - Cliquer sur le cookie lorsque ce dernier apparaît
    - Cliquer sur  la rubrique de montres à choisir : Hommes/Femmes
    - Se diriger sur la page qui contient toutes les annonces 
    - Récupère les informations de chaque annonces de manière itérative
    - Stock ces élements dans un  dataframe. 
    """
    
    #Définissions du nombre de pages à parcourir : 
    nb_page = input('Combien de page(s) souhaites-tu parcourir : ')
    print('Je souhaite parcourir :', nb_page ,' page(s)')
    
    
    # On accède à la page principale 
    driver = webdriver.Firefox()
    driver.get(lien)
    
    time.sleep(5)

    # On clique sur le cookie au moment de son apparition  
    try :
        cookie = driver.find_element(By.CLASS_NAME, 'js-modal-content')
        cookie.find_element(By.CLASS_NAME, 'btn').click()
    except NoSuchElementException:
        pass
    

    time.sleep(5)
    
    # On clique sur le menu déroulant des différentes catégories 
    driver.find_elements(By.CLASS_NAME, 'js-carousel-cell')[0].click()

    time.sleep(5)
    
    # On clique sur la catégorie de montre pour laquelle on souhaiterait récupérer les données : En l'occurrence Hommes/Femmes
    categories = driver.find_element(By.CLASS_NAME, 'col-sm-10')
    categories.find_elements(By.TAG_NAME, 'a')[0].click()
    
    # On récupère toutes les annonces présentes sur la page 
    try :
        page_globale_montre = driver.find_element(By.ID, 'wt-watches')
        liste_montres = page_globale_montre.find_elements(By.CLASS_NAME, 'js-article-item-container')
    except ElementClickInterceptedException :
        pass

    longueur = len(liste_montres)
    print('la longueur de la première liste à parcourir est :', longueur)
    
    
    time.sleep(5)
    

    c = 0
    page = 0
    
    # On parcourt les éléments un à un : 
    condition = True
    while condition:
        for elem in range(len(liste_montres)):
            liste_montres[elem].click()
            
            c += 1 
            
            time.sleep(5) 
            
            try :
                cookie_2 = driver.find_element(By.CLASS_NAME, 'js-modal-content')
                cookie_2.find_element(By.CLASS_NAME,'btn-secondary').click()
            except NoSuchElementException:
                pass

            table = driver.find_element(By.TAG_NAME,'table')
            table_fonction = table.find_elements(By.TAG_NAME,'tbody') 
            
            try:
                fonctions = table_fonction[4].text
            except IndexError:
                fonctions = ""   
                  
            table_caracteristques = driver.find_element(By.TAG_NAME, 'table').text.split('\n')
            

            
            ## Récupération des données : 
            caracteristiques_decoupage = [elem.split() for elem in table_caracteristques]


            marque = str(next((valeurs[1:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Marque'),""))
            modele = str(next((valeurs[1:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Modèle'), ""))
            mouvement = str(next((valeurs[2:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Mouvement'), ""))
            matiere_boitier = str(next((valeurs[1:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Boîtier'), ""))
            matiere_bracelet = str(next((valeurs[-1] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'bracelet'), ""))
            annee_prod = str(next((valeurs[3] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'fabrication'), ""))
            etat = str(next((valeurs[1:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'État'), ""))
            sexe = str(next((valeurs[-1] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Sexe'), ""))
            prix = str(next((valeurs[1:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Prix'), ""))
            reserve_de_marche = str(next((valeurs[2:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Réserve'), ""))
            diametre = str(next((valeurs[1:3] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Diamètre'), ""))
            etencheite = str(next((valeurs[1:3] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Étanche'), ""))
            matiere_lunette = str(next((valeurs[-1] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'lunette'), ""))
            matiere_verre = str(next((valeurs[2:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Verre'), ""))
            boucle = str(next((valeurs[1:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Boucle'), ""))
            matiere_boucle = str((next((valeurs[4:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Matériau'), "")))
            rouage = str(next((valeurs[-1] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Calibre/Rouages'), ""))
            ville = str(next((valeurs[1:] for valeurs in caracteristiques_decoupage for elem in valeurs if elem == 'Emplacement'), ""))
            Date_recup = datetime.date.today()
            
            insert_data(marque, modele, mouvement, matiere_boitier, matiere_bracelet, annee_prod,  etat, sexe, prix, reserve_de_marche, diametre, etencheite, matiere_lunette, matiere_verre, boucle, matiere_boucle, rouage, ville, fonctions, Date_recup)

            
            driver.back()
            
            try:
                page_globale_montre = driver.find_element(By.ID, 'wt-watches')
                liste_montres = page_globale_montre.find_elements(By.CLASS_NAME, 'js-article-item-container')
            except NoSuchElementException:
                continue
            
            

        
        if c < longueur:
            continue
            
        elif c == longueur:
            driver.find_element(By.PARTIAL_LINK_TEXT,"Continuer").click() 
            time.sleep(5)
              
            c = 0
            
            try:
                page_globale_montre = driver.find_element(By.ID, 'wt-watches')
                liste_montres = page_globale_montre.find_elements(By.CLASS_NAME, 'js-article-item-container')
                longueur = len(liste_montres)
                print('La longueur de la liste à parcour est :',longueur)
                
            except NoSuchElementException:
                continue
            
            page += 1
            print('Nous sommes à la page :', page)
            if page == nb_page:
                print('La récupération est terminée.')
                break
    
    if __name__ == "__main__":
        create_database()
        recuperation_donnees(lien)      

    return
 
lien = 'https://www.chrono24.fr/' 
recuperation_donnees(lien)