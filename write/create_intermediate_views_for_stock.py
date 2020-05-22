import pandas
import pytest

from db import db, CONNECTION
from tests.data_creators import clean_database, clean_views, create_user, create_product, create_offerer, create_venue, \
    create_offer, create_stock, create_booking, create_payment, create_payment_status

STOCK_COLUMNS = {"offer_id": "Identifiant de l'offre",
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
                 "bookings_paid": "Nombre de réservations ayant un paiement"}

def create_stocks_booking_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW stock_booking_information AS
        {_get_stocks_booking_information_query()}
        '''
    db.session.execute(query)
    db.session.commit()

def create_available_stocks_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW available_stock_information AS
        {_get_available_stock_query()}
        '''
    db.session.execute(query)
    db.session.commit()

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


def create_enriched_stock_view() -> None:
    query = f'''
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
        '''
    db.session.execute(query)
    db.session.commit()


class StockQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)
        clean_views()

    class GetStocksBookingInformationQueryTest:
        def test_should_return_column_with_total_number_of_bookings_cancelled_and_not(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2, is_cancelled=True)
            create_booking(app, user_id=2, stock_id=2, id=5, quantity=1, token='IS02JE')

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 1],
                name="Nombre total de réservations")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre total de réservations"],
                                               expected_stocks_booking_information)

        def test_should_return_column_with_total_number_of_bookings_at_0_when_no_booking(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2, is_cancelled=True)

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 0],
                name="Nombre total de réservations")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre total de réservations"],
                                               expected_stocks_booking_information)

        def test_should_return_column_with_number_of_cancelled_bookings(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2, is_cancelled=True)
            create_booking(app, user_id=2, stock_id=2, id=5, quantity=1, token='IS02JE')

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 0],
                name="Nombre de réservations annulées")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations annulées"],
                                               expected_stocks_booking_information)

        def test_should_return_column_with_0_cancelled_bookings_if_no_booking(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1], name='stock_id'),
                data=[0],
                name="Nombre de réservations annulées")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations annulées"],
                                               expected_stocks_booking_information)

        def test_should_return_column_with_number_of_bookings_appearing_on_payment(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2)
            create_booking(app, user_id=2, stock_id=2, id=5, quantity=1, token='IS02JE')
            create_payment(app, booking_id=4, id=1)
            create_payment_status(app, payment_id=1)

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 0],
                name="Nombre de réservations ayant un paiement")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"],
                                               expected_stocks_booking_information)

        def test_should_not_count_bookings_appearing_on_payment_if_payment_s_current_status_is_banned(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2)
            create_payment(app, booking_id=4, id=1)
            create_payment_status(app, payment_id=1, id=1, date='2019-01-01', status='PENDING')
            create_payment_status(app, payment_id=1, id=2, date='2019-01-02', status='BANNED')

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[0, 0],
                name="Nombre de réservations ayant un paiement")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION,
                                                               index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"],
                                               expected_stocks_booking_information)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"],
                                               expected_stocks_booking_information)

        def test_should_count_bookings_appearing_on_payment_if_payment_is_no_longer_banned(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2)
            create_payment(app, booking_id=4, id=1)
            create_payment_status(app, payment_id=1, id=1, date='2019-07-18', status='BANNED')
            create_payment_status(app, payment_id=1, id=2, date='2019-07-19', status='SENT')

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 0],
                name="Nombre de réservations ayant un paiement")

            # When
            query = _get_stocks_booking_information_query()


            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"],
                                               expected_stocks_booking_information)