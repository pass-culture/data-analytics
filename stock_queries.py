import pandas
from sqlalchemy.engine import Connection

STOCK_COLUMNS = {"offer_id": "Identifiant de l'offre",
                 "offer_name": "Nom de l'offre",
                 "offerer_id": "offerer_id",
                "offer_type": "Type d'offre",
                 "venue_departement_code": "Département",
                 "stock_issued_at": "Date de création du stock",
                 "booking_limit_datetime": "Date limite de réservation",
                 "beginning_datetime": "Date de début de l'évènement",
                 "available": "Stock disponible brut de réservations",
                "booking_quantity": "Nombre total de réservations",
                 "bookings_cancelled": "Nombre de réservations annulées",
                "bookings_paid": "Nombre de réservations ayant un paiement"}


def get_stocks_details(connection: Connection):
    stocks_details = [
        get_stocks_information(connection),
        get_stocks_offer_information(connection),
        get_stocks_venue_information(connection),
        get_stocks_booking_information(connection),
    ]

    return pandas.concat(stocks_details, axis=1)[STOCK_COLUMNS.values()]


def get_stocks_information(connection: Connection):
    query = f'''
    SELECT
     id AS stock_id,
     "offerId" AS "{STOCK_COLUMNS["offer_id"]}",
     "dateCreated" AS "{STOCK_COLUMNS["stock_issued_at"]}",
     "bookingLimitDatetime" AS "{STOCK_COLUMNS["booking_limit_datetime"]}",
     "beginningDatetime" AS "{STOCK_COLUMNS["beginning_datetime"]}",
     available AS "{STOCK_COLUMNS["available"]}"
    FROM stock
    '''
    return pandas \
        .read_sql(query, connection, index_col="stock_id") \
        .astype({STOCK_COLUMNS["booking_limit_datetime"]: 'datetime64', STOCK_COLUMNS["beginning_datetime"]: 'datetime64'})


def get_stocks_offer_information(connection: Connection):
    query = f'''
    SELECT
     stock.id AS stock_id,
     name AS "{STOCK_COLUMNS["offer_name"]}",
     type AS "{STOCK_COLUMNS["offer_type"]}"
    FROM stock
    JOIN offer ON stock."offerId"=offer.id
    '''
    return pandas \
        .read_sql(query, connection, index_col="stock_id")


def get_stocks_venue_information(connection: Connection):
    query = f'''
    SELECT
     stock.id AS stock_id,
     venue."managingOffererId" AS "{STOCK_COLUMNS["offerer_id"]}",
     venue."departementCode" AS "{STOCK_COLUMNS["venue_departement_code"]}"
    FROM stock
    JOIN offer ON stock."offerId"=offer.id
    JOIN venue ON offer."venueId"=venue.id
    '''
    return pandas \
        .read_sql(query, connection, index_col="stock_id")


def get_stocks_booking_information(connection: Connection):
    query = f'''
    WITH
    last_status AS (
    SELECT DISTINCT ON (payment_status."paymentId")
     payment_status."paymentId", payment_status.status, date
    FROM payment_status
    ORDER BY payment_status."paymentId", date DESC
    ),
    
    valid_payment AS (
    SELECT "bookingId" FROM payment
    LEFT JOIN last_status ON last_status."paymentId"=payment.id
    WHERE last_status.status != 'BANNED'
    ),
    
    booking_with_payment AS (
    SELECT booking.id AS booking_id, booking.quantity AS booking_quantity
    FROM booking
    WHERE booking.id IN (SELECT "bookingId" FROM valid_payment)
    )
    
    SELECT
     stock.id AS stock_id,
     COALESCE(SUM(booking.quantity), 0) AS "{STOCK_COLUMNS["booking_quantity"]}",
     COALESCE(SUM(booking.quantity * booking."isCancelled"::int), 0) AS "{STOCK_COLUMNS["bookings_cancelled"]}",
     COALESCE(SUM(booking_with_payment.booking_quantity), 0) AS "{STOCK_COLUMNS["bookings_paid"]}"
    FROM stock
    LEFT JOIN booking ON booking."stockId"=stock.id
    LEFT JOIN booking_with_payment ON booking_with_payment.booking_id=booking.id
    GROUP BY stock.id
    ORDER BY stock.id
    '''
    return pandas \
        .read_sql(query, connection, index_col="stock_id")
