from db import db


def _get_is_physical_information_query() -> str:
    return '''
        SELECT
            offer.id AS offer_id
            ,case when offer.type IN ('ThingType.INSTRUMENT',
                                        'ThingType.JEUX',
                                        'ThingType.LIVRE_EDITION',
                                        'ThingType.MUSIQUE',
                                        'ThingType.OEUVRE_ART',
                                        'ThingType.AUDIOVISUEL')
             AND offer.url IS NULL 
             then true else false end as "Bien physique"
        FROM offer
    '''

def _get_is_outing_information_query() -> str:
    return '''
    SELECT
        offer.id AS offer_id
        ,case when offer.type IN ('EventType.SPECTACLE_VIVANT'
                         ,'EventType.CINEMA'
                         ,'EventType.JEUX'
                         ,'ThingType.SPECTACLE_VIVANT_ABO'
                         ,'EventType.MUSIQUE'
                         ,'ThingType.MUSEES_PATRIMOINE_ABO'
                         ,'ThingType.CINEMA_CARD'
                         ,'ThingType.PRATIQUE_ARTISTIQUE_ABO'
                         ,'ThingType.CINEMA_ABO'
                         ,'EventType.MUSEES_PATRIMOINE'
                         ,'EventType.PRATIQUE_ARTISTIQUE'
                         ,'EventType.CONFERENCE_DEBAT_DEDICACE')
        then true else false end as "Sorties"
    FROM offer
'''

def _get_offer_booking_information_query() -> str:
    return '''
    SELECT
        offer.id AS offer_id
        ,sum(booking."quantity") AS "Nombre de réservations"
        ,sum(case when booking."isCancelled" then booking."quantity" else null end) AS "Nombre de réservations annulées"
        ,sum(case when booking."isUsed" then booking."quantity" else null end) AS "Nombre de réservations validées"
    FROM offer
    LEFT JOIN stock ON stock."offerId" = offer.id
    LEFT JOIN booking ON stock.id = booking."stockId"
    GROUP BY offer_id
    '''

def _get_count_favorites_query() -> str:
    return '''
    SELECT 
        "offerId" AS offer_id
        ,count(*) AS "Nombre de fois où l'offre a été mise en favoris"
    FROM favorite
    GROUP BY offer_id
    '''

def create_is_physical_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW is_physical_view AS {_get_is_physical_information_query()}
        '''
    db.session.execute(view_query)
    db.session.commit()

def create_is_outing_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW is_outing_view AS {_get_is_outing_information_query()}
        '''
    db.session.execute(view_query)
    db.session.commit()

def create_booking_information_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW offer_booking_information_view AS {_get_offer_booking_information_query()}
        '''
    db.session.execute(view_query)
    db.session.commit()

def create_count_favorites_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW count_favorites_view AS {_get_count_favorites_query()}
        '''
    db.session.execute(view_query)
    db.session.commit()

def create_enriched_offer_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW enriched_offer_data AS (
        SELECT
            offerer.id AS "Identifiant de la structure"
            ,offerer."name" AS "Nom de la structure"
            ,venue.id AS "Identifiant du lieu"
            ,venue."name" AS "Nom du lieu"
            ,venue."departementCode" AS "Département du lieu"   
            ,offer.id AS offer_id
            ,offer."name" AS "Nom de l'offe"
            ,offer."type" AS "Catégorie de l'offre"
            ,offer."dateCreated" AS "Date de création de l'offre"
            ,offer."isDuo"
            ,venue."isVirtual" AS "Offre numérique"
            ,stock."beginningDatetime" AS "Date de début de l'évènement"
            ,stock."price" AS "Prix"
            ,stock."quantity" AS "Stock"
            ,is_physical_view."Bien physique"
            ,is_outing_view."Sorties"
            ,offer_booking_information_view."Nombre de réservations"
            ,offer_booking_information_view."Nombre de réservations annulées"
            ,offer_booking_information_view."Nombre de réservations validées"
            ,count_favorites_view."Nombre de fois où l'offre a été mise en favoris"
        FROM offer
        LEFT JOIN venue ON offer."venueId" = venue.id 
        LEFT JOIN offerer ON venue."managingOffererId" = offerer.id
        LEFT JOIN stock ON stock."offerId" = offer.id
        LEFT JOIN booking ON booking."stockId" = stock.id
        LEFT JOIN favorite ON favorite."offerId" = offer.id
        LEFT JOIN is_physical_view ON is_physical_view.offer_id = offer.id
        LEFT JOIN is_outing_view ON is_outing_view.offer_id = offer.id
        LEFT JOIN offer_booking_information_view ON offer_booking_information_view.offer_id = offer.id
        LEFT JOIN count_favorites_view ON count_favorites_view.offer_id = offer.id
        )
        '''
    db.session.execute(query)
    db.session.commit()
