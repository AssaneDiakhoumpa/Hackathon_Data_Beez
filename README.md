# ETL Hackathon DataBeez – Pipeline Météo & Agro

## Présentation du projet

Ce projet implémente un **pipeline ETL** (Extract, Transform, Load) pour centraliser, transformer et analyser des données météorologiques et agricoles (FAO) du Sénégal.

L’objectif est de créer une **base de données exploitable** pour le suivi agro-climatique et la prédiction de rendements.



## Architecture du pipeline

### A. Extraction (Extract)

* Les données sont extraites depuis **trois sources** principales :

  1. **Weather (Météo)** : températures, précipitations, humidité par département et date.
  2. **FAO** : indicateurs agricoles (rendement, culture, département, etc.).
  3. **Copernicus** : indices agro-climatiques (optionnel, très volumineux).

* Les fichiers sont **sauvegardés temporairement** dans `/tmp/data` pour éviter de garder de gros DataFrames en mémoire.

* Les gros datasets sont lus et sauvegardés **par chunks** pour réduire l’usage mémoire et éviter les crashs.



### B. Transformation (Transform)

1. **Normalisation des colonnes** :

   * Harmonisation des noms de colonnes pour permettre la fusion (`departement`, `date`, etc.).
   * Conversion des dates en format standard (`datetime`).

2. **Fusion des datasets** :

   * Les données météo et FAO sont fusionnées sur `departement` et `date`.
   * Type de fusion : `left join` pour conserver toutes les données météo même si FAO est manquant.

3. **Nettoyage** :

   * Suppression des doublons.
   * Remplacement des valeurs manquantes pour les colonnes numériques par la moyenne de la colonne.
   * Calcul d’indicateurs dérivés si nécessaire (ex. `temp_moy` à partir de `temperature_2m_max` et `temperature_2m_min`).

4. **Résultat final** :

   * Un DataFrame harmonisé contenant :

     * Données météo : température, précipitation, humidité
     * Données FAO : culture, indicateur, rendement
   * Sauvegardé dans `/tmp/data/final.csv` pour le chargement.

---

### C. Chargement (Load)

* Les données transformées sont **chargées dans PostgreSQL**.
* Fonctionnalités clés :

  * Création automatique de la table `weather_agro_data` si elle n’existe pas.
  * Création automatique des colonnes manquantes correspondant aux nouvelles données.
  * Chargement par chunks pour les gros volumes de données.
* La table finale contient les colonnes :

  ```
  id, region, date, temperature_c, precipitation_mm, humidity_percent, departement, culture, indicateur, valeur
  ```
* Les colonnes FAO peuvent rester vides si le dataset FAO ne contient pas de données pour certaines régions ou années. Cela **n’est pas un bug**, mais un comportement attendu lié à la disponibilité des données.



## Optimisations et bonnes pratiques

* **Gestion mémoire** :

  * Libération des DataFrames après chaque étape.
  * Utilisation de `gc.collect()` pour forcer le garbage collector.
* **Robustesse** :

  * Logs détaillés à chaque étape.
  * Gestion des fichiers manquants et erreurs avec Airflow.
* **Scalabilité** :

  * Lecture et écriture des gros fichiers par chunks.
  * Pipeline prêt pour de grandes quantités de données Copernicus.



## Utilisation

1. Démarrer le conteneur Airflow.
2. Le DAG `etl_hackathon_dag` s’exécute automatiquement toutes les 30 minutes (configurable).
3. Les tâches :

   ```
   extract_data -> transform_data -> load_data
   ```
4. Vérifier le résultat final dans PostgreSQL :

   ```sql
   SELECT * FROM weather_agro_data;
   ```
