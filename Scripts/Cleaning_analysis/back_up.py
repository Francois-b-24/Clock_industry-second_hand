import shutil

# Chemin vers le fichier de base de données SQLite
chemin_original = '/Users/f.b/Desktop/Watches/montre.db'

# Chemin vers le fichier de sauvegarde
chemin_sauvegarde = '/Users/f.b/Desktop/Watches/montre_backup.db'

# Copie du fichier original vers le fichier de sauvegarde
shutil.copyfile(chemin_original, chemin_sauvegarde)
