# pass-culture-data-analytics

C'est l'outil d'analyse de données du pass Culture.

## Lancer les commandes relatives à l'environnement Data Analytics
### Installer le package 
1. Créer le package à partir du code de pass-culture-data-analytics
`cd pass-culture-data-analytics`
`make dist`
2. L'installer dans un environnement virtuel
a) Activer l'environnement cible
b) 
`cd pass-culture-data-analytics`
`pip install -e .`

### Lancer la création des tables enrichies
`curl -X POST $DATA_ANALYTICS_DATASOURCE_URL?token=$DATA_ANALYTICS_TOKEN`
Où `$DATA_ANALYTICS_DATASOURCE_URL` correspond à l'url de Metabase et `$DATA_ANALYTICS_TOKEN` au token d'authentification autorisant la création des tables enrichies

### Requêter l'environnement connecté à la base Produit sur Metabase
Une fois le paquet installé, taper : 
`pc-data-analytics show_app_name_for_restore`

### Basculer de la base blue à green (et vice-versa)
Une fois le paquet installé, taper : 
`pc-data-analytics switch_host_for_restore`

## Simuler l'architecture fonctionnelle en local
### Démarrer Metabase en local
1. `cd pass-culture-data-analytics`
2. `make start-metabase`

### Démarrer uniquement les containers du backend
1. `cd pass-culture-data-analytics`
2. `make start-backend`

### Configurer Metabase
L'url pour accéder à Metabase en local est : http://localhost:3002/

Pour configurer Metabase, il suffit de créer un compte admin, puis de se connecter à la base produit. Pour cela, il faut renseigner les informations suivantes :
- Choisir Postgresql comme type de base de données
- Name : Produit
- Host : analytics-datasource-postgres
- Port : 5432
- Database name : pass_culture
- Database username : pass_culture
- Database password : passq

## Aide au développement
### Exécution des tests
Après avoir lancé les contenuers du backend, taper :
`make tests`

### Accéder à la base locale
1. `cd pass-culture-data-analytics`
2. `make access-database`

### Exécution de code python dans le container
1. `cd pass-culture-data-analytics`
2. `make run-python`

### Créer les vues de données enrichies en local
Après avoir lancé les conteneurs du backend, taper :
`make create-enriched-views`

### Accéder au résumé des commandes du Makefile
`make help`