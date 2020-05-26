from db import db


def _get_first_stock_creation_dates_query() -> str:
    return '''
    SELECT 
     offerer.id AS offerer_id, 
     MIN(stock."dateCreated") AS "Date de création du premier stock"
    FROM offerer 
    LEFT JOIN venue ON venue."managingOffererId" = offerer.id 
    LEFT JOIN offer ON offer."venueId" = venue.id 
    LEFT JOIN stock ON stock."offerId" = offer.id 
    GROUP BY offerer_id
    '''


def _get_first_booking_creation_dates_query() -> str:
    return '''
    SELECT 
     offerer.id AS offerer_id, 
     MIN(booking."dateCreated") AS "Date de première réservation"
    FROM offerer    
    LEFT JOIN venue ON venue."managingOffererId" = offerer.id 
    LEFT JOIN offer ON offer."venueId" = venue.id 
    LEFT JOIN stock ON stock."offerId" = offer.id 
    LEFT JOIN booking ON booking."stockId" = stock.id 
    GROUP BY offerer_id
    '''


def _get_number_of_offers_query() -> str:
    return '''
    SELECT 
     offerer.id AS offerer_id, 
     COUNT(offer.id) AS "Nombre d’offres"
    FROM offerer    
    LEFT JOIN venue ON venue."managingOffererId" = offerer.id 
    LEFT JOIN offer ON offer."venueId" = venue.id 
    GROUP BY offerer_id
    '''


def _get_number_of_bookings_not_cancelled_query() -> str:
    return '''
    SELECT 
     offerer.id AS offerer_id, 
     COUNT(booking.id) AS "Nombre de réservations non annulées"
    FROM offerer 
    LEFT JOIN venue ON venue."managingOffererId" = offerer.id 
    LEFT JOIN offer ON offer."venueId" = venue.id 
    LEFT JOIN stock ON stock."offerId" = offer.id 
    LEFT JOIN booking ON booking."stockId" = stock.id AND booking."isCancelled" IS FALSE
    GROUP BY offerer_id
    '''


def create_first_stock_creation_dates_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW related_stocks AS
        {_get_first_stock_creation_dates_query()}
        '''
    db.session.execute(query)
    db.session.commit()


def create_first_booking_creation_dates_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW related_bookings AS
        {_get_first_booking_creation_dates_query()}
        '''
    db.session.execute(query)
    db.session.commit()


def create_number_of_offers_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW related_offers AS
        {_get_number_of_offers_query()}
        '''
    db.session.execute(query)
    db.session.commit()


def create_number_of_bookings_not_cancelled_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW related_non_cancelled_bookings AS
        {_get_number_of_bookings_not_cancelled_query()}
        '''
    db.session.execute(query)
    db.session.commit()


def create_materialized_enriched_offerer_view() -> str:
    query = '''
    CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_offerer_data AS
    (SELECT
     offerer.id AS offerer_id,
     offerer."dateCreated" AS "Date de création",
     related_stocks."Date de création du premier stock",
     related_bookings."Date de première réservation",
     related_offers."Nombre d’offres",
     related_non_cancelled_bookings."Nombre de réservations non annulées",
     offerer_cultural_activity."APE_label" AS "Activité principale",
     offerer_departement_code.department_code AS "Département"

    FROM offerer
    LEFT JOIN related_stocks ON related_stocks.offerer_id = offerer.id
    LEFT JOIN related_bookings ON related_bookings.offerer_id = offerer.id
    LEFT JOIN related_offers ON related_offers.offerer_id = offerer.id
    LEFT JOIN related_non_cancelled_bookings ON related_non_cancelled_bookings.offerer_id = offerer.id
    LEFT JOIN offerer_cultural_activity ON offerer_cultural_activity.id = offerer.id
    LEFT JOIN offerer_departement_code ON offerer_departement_code.id = offerer.id

    )
    ;
    '''
    db.session.execute(query)
    db.session.commit()