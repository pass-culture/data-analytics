STOCK_COLUMNS = {
    "offer_id": "Identifiant de l'offre",
    "offer_name": "Nom de l'offre",
    "offerer_id": "offerer_id",
    "offer_type": "Type d'offre",
    "venue_departement_code": "Département",
    "stock_issued_at": "Date de création du stock",
    "booking_limit_datetime": "Date limite de réservation",
    "beginning_datetime": "Date de début de l'évènement",
    "quantity": "Stock disponible brut de réservations",
    "booking_quantity": "Nombre total de réservations",
    "bookings_cancelled": "Nombre de réservations annulées",
    "bookings_paid": "Nombre de réservations ayant un paiement",
}


def create_stocks_booking_view(ENGINE) -> None:
    query = f"""
        CREATE OR REPLACE VIEW stock_booking_information AS
        {_get_stocks_booking_information_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(query)


def create_available_stocks_view(ENGINE) -> None:
    query = f"""
        CREATE OR REPLACE VIEW available_stock_information AS
        {_get_available_stock_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(query)


def _get_stocks_booking_information_query() -> str:
    return f"""
    (WITH last_status AS 
    ( SELECT DISTINCT ON (payment_status."paymentId") payment_status."paymentId", 
    payment_status.status, 
    date
    FROM payment_status
    ORDER BY payment_status."paymentId", date DESC
    ),

    valid_payment AS
    ( SELECT "bookingId"
    FROM payment
    LEFT JOIN last_status ON last_status."paymentId" = payment.id
    WHERE last_status.status != 'BANNED'
    ),

    booking_with_payment AS
    ( SELECT
    booking.id AS booking_id, 
    booking.quantity AS booking_quantity
    FROM booking
    WHERE booking.id IN(SELECT "bookingId" FROM valid_payment)
    )

    SELECT stock.id AS stock_id,
    COALESCE(SUM(booking.quantity), 0) AS "{STOCK_COLUMNS["booking_quantity"]}",
    COALESCE(SUM(booking.quantity * booking."isCancelled"::int), 0) AS "{STOCK_COLUMNS["bookings_cancelled"]}",
    COALESCE(SUM(booking_with_payment.booking_quantity), 0) AS "{STOCK_COLUMNS["bookings_paid"]}"
    FROM stock
    LEFT JOIN booking ON booking."stockId" = stock.id
    LEFT JOIN booking_with_payment ON booking_with_payment.booking_id = booking.id
    GROUP BY stock.id
    ORDER BY stock.id)
    """


def _get_available_stock_query() -> str:
    return """
    WITH bookings_grouped_by_stock AS (
    SELECT 
     booking."stockId", 
     SUM(booking.quantity) as "number_of_booking"
    FROM booking
    LEFT JOIN stock ON booking."stockId" = stock.id
    WHERE booking."isCancelled" = 'false' 
    GROUP BY booking."stockId" 
    )

    SELECT
     stock.id AS stock_id,
    CASE 
	    WHEN stock."quantity" IS NULL THEN NULL
	    ELSE GREATEST(stock."quantity" - COALESCE(bookings_grouped_by_stock."number_of_booking", 0), 0)
    END AS "Stock disponible réel"
    FROM stock
    LEFT JOIN bookings_grouped_by_stock 
    ON bookings_grouped_by_stock."stockId" = stock.id
    """


def create_enriched_stock_view(ENGINE) -> None:
    query = f"""
        CREATE OR REPLACE VIEW enriched_stock_data AS
        (SELECT
         stock.id AS stock_id,
         stock."offerId" AS "{STOCK_COLUMNS["offer_id"]}",
         offer.name AS "{STOCK_COLUMNS["offer_name"]}",
         venue."managingOffererId" AS {STOCK_COLUMNS["offerer_id"]},
         offer.type AS "{STOCK_COLUMNS["offer_type"]}",
         venue."departementCode" AS "{STOCK_COLUMNS["venue_departement_code"]}",
         stock."dateCreated" AS "{STOCK_COLUMNS["stock_issued_at"]}",
         stock."bookingLimitDatetime" AS "{STOCK_COLUMNS["booking_limit_datetime"]}",
         stock."beginningDatetime" AS "{STOCK_COLUMNS["beginning_datetime"]}",
         available_stock_information."Stock disponible réel",
         stock.quantity AS "{STOCK_COLUMNS["quantity"]}",
         stock_booking_information."{STOCK_COLUMNS["booking_quantity"]}",
         stock_booking_information."{STOCK_COLUMNS["bookings_cancelled"]}",
         stock_booking_information."{STOCK_COLUMNS["bookings_paid"]}"
         FROM stock
         LEFT JOIN offer ON stock."offerId" = offer.id
         LEFT JOIN venue ON venue.id = offer."venueId"
         LEFT JOIN stock_booking_information ON stock.id = stock_booking_information.stock_id
         LEFT JOIN available_stock_information ON stock_booking_information.stock_id = available_stock_information.stock_id);
        """
    with ENGINE.connect() as connection:
        connection.execute(query)
