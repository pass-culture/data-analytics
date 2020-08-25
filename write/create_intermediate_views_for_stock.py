STOCK_COLUMNS = {"offer_id": "Identifiant de l'offre",
                 "new_id": "new_id"}


def create_stocks_booking_view(ENGINE) -> None:
    query = f'''
        CREATE OR REPLACE VIEW stock_booking_information AS
        {_get_stocks_booking_information_query()}
        '''
    with ENGINE.connect() as connection:
        connection.execute(query)


def create_available_stocks_view(ENGINE) -> None:
    query = f'''
        CREATE OR REPLACE VIEW available_stock_information AS
        {_get_available_stock_query()}
        '''
    with ENGINE.connect() as connection:
        connection.execute(query)


def _get_stocks_booking_information_query() -> str:
    return f'''
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
    '''


def _get_available_stock_query() -> str:
    return '''
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
    '''



def _create_function_do_stuff(ENGINE) -> str:
    function_name = 'do_stuff'
    # CREATE EXTENSION plpython3u;
    with ENGINE.connect() as connection:
        connection.execute(f"""
            DROP FUNCTION do_stuff(bigint);
            CREATE OR REPLACE FUNCTION {function_name}(offer_id BIGINT)
            RETURNS TEXT AS
            $$
                from base64 import b32encode

                def humanize(integer):
                    if integer is None:
                        return None
                    b32 = b32encode(int_to_bytes(integer))
                    return b32.decode('ascii')\
                            .replace('O', '8')\
                            .replace('I', '9')\
                            .rstrip('=')

                def int_to_bytes(x):
                    return x.to_bytes((x.bit_length() + 7) // 8, 'big')

                return humanize(offer_id)
            $$
            LANGUAGE 'plpython3u' VOLATILE;
        """)
    return function_name

def create_enriched_stock_view(ENGINE) -> None:
    do_stuff = _create_function_do_stuff(ENGINE)
    query = f'''
        CREATE OR REPLACE VIEW enriched_stock_data AS
        SELECT
         stock.id AS stock_id,
        (SELECT * FROM {do_stuff}(stock.id))
         FROM stock;
        '''
    with ENGINE.connect() as connection:
        connection.execute(query)
