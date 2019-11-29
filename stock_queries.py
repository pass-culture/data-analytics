import pandas


def get_stocks_details(connection):
    stocks_details = [
        get_stocks_information(connection),
        get_stocks_offer_information(connection),
        get_stocks_venue_information(connection),
        get_stocks_booking_information(connection),
        get_stock_creation_date(connection)
    ]
    return pandas.concat(stocks_details, axis=1)[["Identifiant de l'offre", "Nom de l'offre", "offerer_id",
                                                  "Type d'offre", "Département", "Date de création du stock",
                                                  "Date limite de réservation", "Date de début de l'évènement",
                                                  "Stock disponible brut de réservations",
                                                  "Nombre total de réservations", "Nombre de réservations annulées",
                                                  "Nombre de réservations ayant un paiement"]]


def get_stocks_information(connection):
    query = '''
    SELECT
     id AS stock_id,
     "offerId" AS "Identifiant de l'offre",
     "bookingLimitDatetime" AS "Date limite de réservation",
     "beginningDatetime" AS "Date de début de l'évènement",
     available AS "Stock disponible brut de réservations"
    FROM stock
    '''
    return pandas \
        .read_sql(query, connection, index_col="stock_id") \
        .astype({"Date limite de réservation": 'datetime64', "Date de début de l'évènement": 'datetime64'})


def get_stocks_offer_information(connection):
    query = '''
    SELECT
     stock.id AS stock_id,
     name AS "Nom de l'offre",
     type AS "Type d'offre"
    FROM stock
    JOIN offer ON stock."offerId"=offer.id
    '''
    return pandas \
        .read_sql(query, connection, index_col="stock_id")


def get_stocks_venue_information(connection):
    query = '''
    SELECT
     stock.id AS stock_id,
     venue."managingOffererId" AS offerer_id,
     venue."departementCode" AS "Département"
    FROM stock
    JOIN offer ON stock."offerId"=offer.id
    JOIN venue ON offer."venueId"=venue.id
    '''
    return pandas \
        .read_sql(query, connection, index_col="stock_id")


def get_stocks_booking_information(connection):
    query = '''
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
    SELECT booking.id AS booking_id, booking.quantity
    FROM booking
    WHERE booking.id IN (SELECT "bookingId" FROM valid_payment)
    )
    
    SELECT
     stock.id AS stock_id,
     COALESCE(SUM(booking.quantity), 0) AS "Nombre total de réservations",
     COALESCE(SUM(booking.quantity * booking."isCancelled"::int), 0) AS "Nombre de réservations annulées",
     COALESCE(SUM(booking_with_payment.quantity), 0) AS "Nombre de réservations ayant un paiement"
    FROM stock
    LEFT JOIN booking ON booking."stockId"=stock.id
    LEFT JOIN booking_with_payment ON booking_with_payment.booking_id=booking.id
    GROUP BY stock.id
    '''
    return pandas \
        .read_sql(query, connection, index_col="stock_id")


def get_stock_creation_date(connection):
    query = '''
    SELECT
     issued_at AS "Date de création du stock",
     (changed_data ->>'id')::int AS stock_id
    FROM activity
    WHERE table_name='stock'
    AND verb='insert'
    '''
    return pandas \
        .read_sql(query, connection, index_col="stock_id")
