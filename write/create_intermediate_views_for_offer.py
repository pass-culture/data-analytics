from db import db


def create_enriched_offer_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW enriched_offer_data AS
        (
        select
            offer.id AS offer_id
            ,offer.id AS "Identifiant de l'offre"
            ,offer."name" AS "Nom de l'offe"
            ,offer."type" AS "Catégorie de l'offre"
            ,offer."dateCreated" AS "Date de création de l'offre"
        FROM offer
        );
        '''
    db.session.execute(query)
    db.session.commit()
