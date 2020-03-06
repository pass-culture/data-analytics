# pass-culture-metabase

C'est l'outil de visualisation de données du pass Culture.

## Utiliser Metabase
### Démarrer Metabase en local
1. `cd pass-culture-metabase`
2. `docker-compose -f docker-compose.yml -f docker-compose-with-metabase.yml up`

### Démarrer uniquement les containers nécessaires aux tests
1. `cd pass-culture-metabase`
2. `docker-compose up`


### Configurer Metabase
L'url pour accéder à Metabase en local est : http://localhost:3002/

Pour configurer Metabase, il suffit de créer un compte admin, puis de se connecter à la base produit. Pour cela, il faut renseigner les informations suivantes :
- Choisir Postgresql comme type de base de données
- Name : Produit
- Host : pcm-postgres-product
- Port : 5432
- Database name : pass_culture
- Database username : pass_culture
- Database password : passq

## Créer les tables de données enrichies
Après avoir lancé les conteneurs, taper :

`docker exec -it pcm-enriched-data bash -c "cd /opt/pass-culture-metabase; python create_enriched_data_tables.py"`

## Configurer son IDE
- Monter un virtualenv ([lien](https://python-guide-pt-br.readthedocs.io/fr/latest/dev/virtualenvs.html)) afin d'avoir un environnement isolé et contextualisé : `pip install virtualenv`
- exécuter les commandes suivantes :
1. `cd pass-culture-metabase`
2. `virtualenv venv -p python3` (si vous n'avez pas python3). 
3. Sinon faire `python3 -m venv venv`
4. `source venv/bin/activate`
5. `pip install -r requirements.txt`

## Exécution des tests
`pytest`

# Accès à la base
`docker exec -it pcm-postgres-product psql -U pass_culture`

# Exécution de code python dans le container
 `docker exec -it pcm-enriched-data bash -c "cd /opt/pass-culture-metabase && PYTHONPATH=. python"`