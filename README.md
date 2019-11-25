# pass-culture-metabase

C'est l'outil de visualisation de données du pass Culture.

## Utiliser Metabase
### Démarrer Metabase en local
Se placer à la racine du projet et taper :

`docker-compose up`

### Configurer Metabase
L'url pour aller sur Metabase en local est : http://localhost:3002/

Pour configurer Metabase, il suffit de créer un compte admin, puis de se connecter à la base produit. Pour cela, il faut renseigner les informations suivantes :
- Choisir Postgresql comme type de base de données
- Host : pcm-postgres-product
- Port : 5432
- Database name : pass_culture
- Database username : pass_culture
- Database password : passq

## Créer les tables de données enrichies
Après avoir lancé les conteneurs, taper :

`docker exec -it pcm-enriched-data bash -c "cd /opt/pass-culture-metabase; python create_enriched_data_tables.py"`

## Lancer les tests
`docker exec -it pcm-enriched-data bash -c "cd /opt/pass-culture-metabase; pytest"`