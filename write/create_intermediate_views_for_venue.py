from datetime import datetime

from db import db


def _get_number_of_bookings_per_venue() -> str:
    return '''
    SELECT
        venue.id AS venue_id
        ,count(booking.id) AS total_bookings
    FROM venue
    LEFT JOIN offer
    ON venue.id = offer."venueId"
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' or offer."bookingEmail" is null)
    AND offer."type" NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
    LEFT JOIN stock
    ON stock."offerId" = offer.id
    LEFT JOIN booking
    ON stock.id = booking."stockId"
    GROUP BY venue.id
    '''


def _get_number_of_non_cancelled_bookings_per_venue() -> str:
    return '''
    SELECT
        venue.id AS venue_id
        ,count(booking.id) AS non_cancelled_bookings
    FROM venue
    LEFT JOIN offer
    ON venue.id = offer."venueId"
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' or offer."bookingEmail" is null)
    AND offer."type" NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
    LEFT JOIN stock
    ON stock."offerId" = offer.id
    LEFT JOIN booking
    ON stock.id = booking."stockId"
    AND NOT booking."isCancelled"
    GROUP BY venue.id
    '''


def _get_number_of_used_bookings() -> str:
    return '''
    SELECT
        venue.id AS venue_id
        ,count(booking.id) AS used_bookings
    FROM venue
    LEFT JOIN offer
    ON venue.id = offer."venueId"
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' or offer."bookingEmail" is null)
    AND offer."type" NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
    LEFT JOIN stock
    ON stock."offerId" = offer.id
    LEFT JOIN booking
    ON stock.id = booking."stockId"
    AND  booking."isUsed"
    GROUP BY venue.id
    '''


def _get_first_offer_creation_date() -> str:
    return '''
    SELECT
        venue.id AS venue_id
        ,MIN(offer."dateCreated") AS first_offer_creation_date
    FROM venue
    LEFT JOIN offer
    ON venue.id = offer."venueId"
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' or offer."bookingEmail" is null)
    AND offer."type" NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
    GROUP BY venue.id
    '''


def _get_last_offer_creation_date() -> str:
    return '''
    SELECT
        venue.id AS venue_id
        ,MAX(offer."dateCreated") AS last_offer_creation_date
    FROM venue
    LEFT JOIN offer
    ON venue.id = offer."venueId"
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' or offer."bookingEmail" is null)
    AND offer."type" NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
    GROUP BY venue.id
    '''


def _get_offers_created_per_venue() -> str:
    return '''
    SELECT
        venue.id AS venue_id
        ,count(offer.id) AS offers_created
    FROM venue
    LEFT JOIN offer
    ON venue.id = offer."venueId"
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' or offer."bookingEmail" is null)
    AND offer."type" NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
    GROUP BY venue.id
    '''


def _get_theoretic_revenue_per_venue() -> str:
    return '''
    SELECT
        venue.id AS venue_id
        ,COALESCE(SUM(booking.amount * booking.quantity), 0) AS theoretic_revenue
    FROM venue
    LEFT JOIN offer
    ON venue.id = offer."venueId"
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' or offer."bookingEmail" is null)
    AND offer."type" NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
    LEFT JOIN stock
    ON offer.id = stock."offerId"
    LEFT JOIN booking
    ON booking."stockId" = stock.id
    AND NOT booking."isCancelled"
    GROUP BY venue.id
    '''


def _get_real_revenue_per_venue() -> str:
    return '''
    SELECT
        venue.id AS venue_id
        ,COALESCE(SUM(booking.amount * booking.quantity), 0) AS real_revenue
    FROM venue
    LEFT JOIN offer
    ON venue.id = offer."venueId"
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' or offer."bookingEmail" is null)
    AND offer."type" NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
    LEFT JOIN stock
    ON offer.id = stock."offerId"
    LEFT JOIN booking
    ON booking."stockId" = stock.id
    AND NOT booking."isCancelled"
    AND booking."isUsed"
    GROUP BY venue.id
    '''


def create_total_bookings_per_venue_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW total_bookings_per_venue AS {_get_number_of_bookings_per_venue()}
        '''
    db.session.execute(view_query)
    db.session.commit()


def create_non_cancelled_bookings_per_venue_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW non_cancelled_bookings_per_venue AS {_get_number_of_non_cancelled_bookings_per_venue()}
        '''
    db.session.execute(view_query)
    db.session.commit()


def create_used_bookings_per_venue_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW used_bookings_per_venue  AS {_get_number_of_used_bookings()}
        '''
    db.session.execute(view_query)
    db.session.commit()


def create_first_offer_creation_date_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW first_offer_creation_date AS {_get_first_offer_creation_date()}
        '''
    db.session.execute(view_query)
    db.session.commit()


def create_last_offer_creation_date_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW last_offer_creation_date AS {_get_last_offer_creation_date()}
        '''
    db.session.execute(view_query)
    db.session.commit()


def create_offers_created_per_venue_view() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW offers_created_per_venue AS {_get_offers_created_per_venue()}
        '''
    db.session.execute(view_query)
    db.session.commit()


def create_theoretic_revenue_per_venue() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW theoretic_revenue_per_venue AS {_get_theoretic_revenue_per_venue()}
        '''
    db.session.execute(view_query)
    db.session.commit()


def create_real_revenue_per_venue() -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW real_revenue_per_venue AS {_get_real_revenue_per_venue()}
        '''
    db.session.execute(view_query)
    db.session.commit()


def create_enriched_venue_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW enriched_venue_data AS (
        SELECT
            venue.id AS venue_id
            ,COALESCE(venue."publicName",venue."name") AS "Nom du lieu"
            ,venue."bookingEmail" AS email
            ,venue.address AS "Adresse"
            ,venue.latitude
            ,venue.longitude
            ,venue."departementCode" AS "Département"
            ,venue."postalCode" AS "Code postal"
            ,venue.city AS "Ville"
            ,venue.siret
            ,venue."isVirtual" AS "Lieu numérique"
            ,venue."managingOffererId" AS "Identifiant de la structure"
            ,offerer."name" AS "Nom de la structure"
            ,venue_type."label" AS "Type de lieu"
            ,venue_label."label" AS "Label du lieu"
            ,total_bookings_per_venue.total_bookings AS "Nombre total de réservations"
            ,non_cancelled_bookings_per_venue.non_cancelled_bookings AS "Nombre de réservations non annulées"
            ,used_bookings_per_venue.used_bookings AS "Nombre de réservations validées"
            ,first_offer_creation_date.first_offer_creation_date AS "Date de création de la première offre"
            ,last_offer_creation_date.last_offer_creation_date AS "Date de création de la dernière offre"
            ,offers_created_per_venue.offers_created AS "Nombre d'offres créées"
            ,theoretic_revenue_per_venue.theoretic_revenue AS "Chiffre d'affaires théorique réalisé"
            ,real_revenue_per_venue.real_revenue AS "Chiffre d'affaires réel réalisé"
        FROM venue
        LEFT JOIN offerer
        ON venue."managingOffererId" = offerer.id
        LEFT JOIN venue_type
        ON venue."venueTypeId" = venue_type.id
        LEFT JOIN venue_label
        ON venue_label.id = venue."venueLabelId"
        LEFT JOIN total_bookings_per_venue
        ON venue.id = total_bookings_per_venue.venue_id
        LEFT JOIN non_cancelled_bookings_per_venue
        ON venue.id = non_cancelled_bookings_per_venue.venue_id
        LEFT JOIN used_bookings_per_venue
        ON venue.id = used_bookings_per_venue.venue_id
        LEFT JOIN first_offer_creation_date
        ON venue.id = first_offer_creation_date.venue_id
        LEFT JOIN last_offer_creation_date
        ON venue.id = last_offer_creation_date.venue_id
        LEFT JOIN offers_created_per_venue
        ON venue.id = offers_created_per_venue.venue_id
        LEFT JOIN theoretic_revenue_per_venue
        ON venue.id = theoretic_revenue_per_venue.venue_id
        LEFT JOIN real_revenue_per_venue
        ON venue.id = real_revenue_per_venue.venue_id
        )
        '''
    db.session.execute(query)
    db.session.commit()