import pandas
from pandas import DataFrame
from db import db, CONNECTION


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

def create_siren_dataframe() -> DataFrame:
    query = '''
    SELECT
        id
        ,siren
    FROM offerer
    WHERE siren IS NOT NULL 
    '''
    siren_df = pandas.read_sql(query, CONNECTION, index_col='id')
    return siren_df
