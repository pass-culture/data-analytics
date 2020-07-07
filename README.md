# pass-culture-data-analytics

C'est l'outil de visualisation de données du pass Culture.

## Utiliser Metabase
### Démarrer Metabase en local
1. `cd pass-culture-data-analytics`
2. `make start-metabase`

### Démarrer uniquement les containers nécessaires aux tests
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

## Créer les vues de données enrichies
Après avoir lancé les conteneurs, taper :

`make create-enriched-views`

## Configurer son IDE
- Monter un virtualenv ([lien](https://python-guide-pt-br.readthedocs.io/fr/latest/dev/virtualenvs.html)) afin d'avoir un environnement isolé et contextualisé : `brew install pipenv`
- exécuter les commandes suivantes :
1. `cd pass-culture-data-analytics`
2. `virtualenv venv -p python3` (si vous n'avez pas python3).
3. Sinon faire `python3 -m venv venv`
4. `source venv/bin/activate`
5. `pip install -r requirements.txt`

## Exécution des tests
`make tests`

# Accès à la base
1. `cd pass-culture-data-analytics`
2. `make access-database`

# Exécution de code python dans le container
1. `cd pass-culture-data-analytics`
2. `make run-python`
