# pass-culture-data-analytics

## Pré-requis : configurer son IDE
- Monter un virtualenv ([lien](https://python-guide-pt-br.readthedocs.io/fr/latest/dev/virtualenvs.html)) afin d'avoir un environnement isolé et contextualisé : `pip install virtualenv`
- exécuter les commandes suivantes :
1. `virtualenv venv -p python3` (si vous n'avez pas python3). 
2. Sinon faire `python3 -m venv venv`
3. `source venv/bin/activate`
4. `pip install -r requirements.txt`

## Utiliser Metabase

Metabase est l'outil de visualisation de données du pass Culture.

### Démarrer uniquement les containers nécessaires aux tests
`docker-compose up`

### Démarrer Metabase en local
`docker-compose -f docker-compose.yml -f docker-compose-with-metabase.yml up`

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

## Créer les vues de données enrichies
Après avoir lancé les conteneurs, taper :

`docker exec -it pcm-enriched-data bash -c "cd /opt/pass-culture-data-analytics; python create_enriched_data_views.py"`

## Exécution des tests
`pytest`

## Accès à la base
`docker exec -it pcm-postgres-product psql -U pass_culture`

## Exécution de code python dans le container
 `docker exec -it pcm-enriched-data bash -c "cd /opt/pass-culture-data-analytics && PYTHONPATH=. python"`