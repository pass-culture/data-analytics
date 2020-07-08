# pass-culture-data-analytics

C'est l'outil d'analyse de données du pass Culture.

## Lancer les commandes relatives à l'environnement Data Analytics
<<<<<<< HEAD
### First setup
1. Cloner le repos
2. Ajouter ses variables d'environnment en local
3. Installer pipenv
4. `make start-metabase`
5. `make initialize-metabase`
6. Pour vérifier `make tests`

### Exemple de variables d'environnement en local :
A mettre dans votre .bshrc / .zshrc / .env.local ...

```
export METABASE_URL='http://localhost:3002'
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

=======
>>>>>>> (PC-3921): add metabase cli command to clean database and create views
### Installer le package
1. Créer le package à partir du code de pass-culture-data-analytics
`cd pass-culture-data-analytics`
`make dist`
2. L'installer dans un environnement virtuel

a) Activer l'environnement cible
<<<<<<< HEAD

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

=======
b)
`cd pass-culture-data-analytics`
`pip install -e .`
>>>>>>> (PC-3921): add metabase cli command to clean database and create views

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


### Configurer Metabase en une commmande :
1. `cd pass-culture-data-analytics`
2. Après un  `make start-backend`
3. lancer `make initialize-metabase`

L'url pour accéder à Metabase en local est : http://localhost:3002/, connectez vous avec METABASE_USER_NAME et METABASE_PASSWORD

### Configurer Metabase manuellement :
L'url pour accéder à Metabase en local est : http://localhost:3002/

Pour configurer Metabase, il suffit de créer un compte admin, puis de se connecter à la base produit. Pour cela, il faut renseigner les informations suivantes :
- Choisir Postgresql comme type de base de données
- Name : Produit
- Host : analytics-datasource-blue-postgres
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
