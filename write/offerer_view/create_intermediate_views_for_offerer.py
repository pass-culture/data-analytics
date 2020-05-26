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


def _get_number_of_venues_per_offerer_query() -> str:
    return '''
    SELECT
        offerer.id AS offerer_id
        ,count(venue.id) AS "Nombre de lieux"
    FROM offerer
    LEFT JOIN venue
    ON offerer.id = venue."managingOffererId"
    GROUP BY 1
    '''


def _get_number_of_venues_with_offer_per_offerer_query() -> str:
    return '''
      WITH venues_with_offers AS (
      SELECT
          offerer.id AS offerer_id
          ,venue.id AS venue_id
          ,count(offer.id) AS count_offers
      FROM offerer
      LEFT JOIN venue
      ON offerer.id = venue."managingOffererId"
      LEFT JOIN offer
      ON venue.id = offer."venueId"
      GROUP BY 1,2
      )
      
      SELECT
          offerer_id
          ,COUNT(CASE WHEN count_offers > 0 THEN venue_id ELSE NULL END) AS "Nombre de lieux avec offres"
      FROM venues_with_offers
      GROUP BY 1
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


def create_number_of_venues_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW related_venues AS
        {_get_number_of_venues_per_offerer_query()}
        '''
    db.session.execute(query)
    db.session.commit()


def create_number_of_venues_without_offer_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW related_venues_with_offer AS
        {_get_number_of_venues_with_offer_per_offerer_query()}
        '''
    db.session.execute(query)
    db.session.commit()


def create_materialized_enriched_offerer_view() -> str:
    query = '''
    CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_offerer_data AS
    (SELECT
     offerer.id AS offerer_id,
     offerer.name AS "Nom",
     offerer."dateCreated" AS "Date de création",
     related_stocks."Date de création du premier stock",
     related_bookings."Date de première réservation",
     related_offers."Nombre d’offres",
     related_non_cancelled_bookings."Nombre de réservations non annulées",
     offerer_departement_code.department_code AS "Département",
     related_venues."Nombre de lieux",
     related_venues_with_offer."Nombre de lieux avec offres"
    FROM offerer
    LEFT JOIN related_stocks ON related_stocks.offerer_id = offerer.id
    LEFT JOIN related_bookings ON related_bookings.offerer_id = offerer.id
    LEFT JOIN related_offers ON related_offers.offerer_id = offerer.id
    LEFT JOIN related_non_cancelled_bookings ON related_non_cancelled_bookings.offerer_id = offerer.id
    LEFT JOIN offerer_departement_code ON offerer_departement_code.id = offerer.id
    LEFT JOIN related_venues ON related_venues.offerer_id = offerer.id
    LEFT JOIN related_venues_with_offer ON related_venues_with_offer.offerer_id = offerer.id
    )
    ;
    '''
    db.session.execute(query)
    db.session.commit()