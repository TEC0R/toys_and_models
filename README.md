# Toys and Models Analytics Engineering Project

Ce projet implémente un pipeline d'analyse de données pour Toys and Models, utilisant Snowflake, PySpark, Airflow, et dbt.

## Structure du projet

- `airflow/`: Contient les DAGs et les opérateurs personnalisés pour Airflow
- `dbt_files/`: Contient les modèles dbt pour la transformation des données
- `spark/`: Contient les jobs PySpark pour l'extraction et le chargement des données
- `sql/`: Contient les scripts SQL, y compris le dump de la base de données initiale
- `config/`: Contient les fichiers de configuration pour MySQL et Snowflake

## Installation

1. Clonez ce dépôt
2. Installez les dépendances : `pip install -r requirements.txt`
3. Configurez Airflow, Snowflake, et votre base de données MySQL
4. Exécutez le script SQL dans `sql/Dump202403.sql` pour initialiser la base de données MySQL
5. Configurez les fichiers dans `config/` avec vos informations de connexion

## Utilisation

1. Démarrez Airflow et activez le DAG `toys_and_models_etl`
2. Les données seront extraites de MySQL, transformées avec PySpark, et chargées dans Snowflake
3. dbt sera utilisé pour effectuer des transformations supplémentaires dans Snowflake

## Contribuer

Les pull requests sont les bienvenues. Pour des changements majeurs, veuillez d'abord ouvrir une issue pour discuter de ce que vous aimeriez changer.