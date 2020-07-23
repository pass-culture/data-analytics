# pass-culture-data-analytics

C'est l'outil d'analyse de données du pass Culture.

## Lancer les commandes relatives à l'environnement Data Analytics
### First setup

1. Cloner le repos
2. Ajouter ses variables d'environnment en local (voir ci-dessous)
3. Installer pipenv
4. `make start-metabase`
5. `make initialize-metabase`
6. Pour vérifier `make tests`

### Exemple de variables d'environnement en local :
Créer un fichier .env.local où on peut mettre ces variables d'environnements:

```
export METABASE_USER_NAME='admin@example.com'
export METABASE_PASSWORD='user@AZERTY123'
export METABASE_DBNAME='Produit'
export GREEN_DB_INFO='{
"app_name": "app-green",
"details": {
"port": "5432",
"host": "analytics-datasource-green-postgres",
"dbname": "pass_culture",
"user": "pass_culture",
"password": "passq"
}
}'

export BLUE_DB_INFO='{
"app_name": "app-blue",
"details": {
"port": "5432",
"host": "analytics-datasource-blue-postgres",
"dbname": "pass_culture",
"user": "pass_culture",
"password": "passq"
}
}'
```

## Simuler l'architecture fonctionnelle en local
### Pour démarrer tout Metabase en local
1. `cd pass-culture-data-analytics`
2. `make start-metabase`

### Pour démarrer uniquement les containers du backend
1. `cd pass-culture-data-analytics`
2. `make start-backend`

### Configurer Metabase en une commmande :

Ensuite exécuter les commandes suivantes :
1. `cd pass-culture-data-analytics`
2. Après un  `make start-metabase`
3. lancer `make initialize-metabase`

L'url pour accéder à Metabase en local est : http://localhost:3002/, connectez vous avec METABASE_USER_NAME et METABASE_PASSWORD

### Configurer Metabase manuellement :

Pour configurer Metabase, il suffit de créer un compte admin, puis de se connecter à la base produit. Pour cela, il faut renseigner les informations suivantes :
- Choisir Postgresql comme type de base de données
- Name : Produit
- Host : analytics-datasource-blue-postgres
- Port : 5432
- Database name : pass_culture
- Database username : pass_culture
- Database password : passq

### Installer le package
1. Créer le package à partir du code de pass-culture-data-analytics
`cd pass-culture-data-analytics`
`make dist`
2. L'installer dans un environnement virtuel

a) Activer l'environnement cible

Par exemple :
Monter un virtualenv ([lien](https://python-guide-pt-br.readthedocs.io/fr/latest/dev/virtualenvs.html)) afin d'avoir un environnement isolé et contextualisé : `pip install virtualenv`
- exécuter les commandes suivantes :
1. `cd pass-culture-data-analytics`
2. `virtualenv venv -p python3` (si vous n'avez pas python3).
3. Sinon faire `python3 -m venv venv`
4. `source venv/bin/activate`

b) Dans cet environnement :
`pip install -e <path_to_pass-culture-data-analytics>`

c) Vous pouvez faire un `pc-data-analytics` pour voir la liste des commandes à lancer


### Lancer la création des tables enrichies
`curl -X POST $DATA_ANALYTICS_DATASOURCE_URL?token=$DATA_ANALYTICS_TOKEN`
Où `$DATA_ANALYTICS_DATASOURCE_URL` correspond à l'url de Metabase et `$DATA_ANALYTICS_TOKEN` au token d'authentification autorisant la création des tables enrichies

### Requêter l'environnement connecté à la base Produit sur Metabase
Une fois le paquet installé, taper :
`pc-data-analytics show_app_name_for_restore`

### Basculer de la base blue à green (et vice-versa)
Une fois le paquet installé, taper :
`pc-data-analytics switch_host_for_restore`

## Aide au développement
### Exécution des tests
Après avoir lancé les contenuers du backend, taper :
`make tests`
Certains tests échoueront tant que les vues enrichies n'ont pas été créées (cela peut prendre quelques secondes)

### Accéder à la base locale
1. `cd pass-culture-data-analytics`
2. `make access-database`

### Exécution de code python dans le container
1. `cd pass-culture-data-analytics`
2. `make run-python`

### Reset metabase
1. `cd pass-culture-data-analytics`
2. `make reset-metabase`

### Créer les vues de données enrichies en local
Après avoir lancé les conteneurs du backend, taper :
1. `make create-enriched-views`
2. Il faut ensuite synchroniser la base sur metabase

- se rendre sur http://localhost:3002/admin/databases
- choisir la base à synchroniser (Produit)
- cliquer sur `Sync database schema now`
- vos tables enrichies sont maintenant accessibles


### Accéder au résumé des commandes du Makefile
`make help`
