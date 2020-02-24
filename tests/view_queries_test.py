from datetime import datetime

import pandas
import pytest

from db import SESSION, CONNECTION
from query_enriched_data_tables import create_enriched_stock_data
from stock_queries import create_stock_view, create_stocks_offer_view, create_stock_venue_view, \
    create_stocks_booking_view
from tests.utils import clean_database, create_user, create_product, create_offerer, create_venue, create_offer, \
    create_stock, create_booking, create_payment, create_payment_status

class ViewQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_class(self):
        clean_database()

    @pytest.fixture(scope='session')
    def drop_view(self):
        SESSION.execute('DROP VIEW enriched_stock_data;')
        SESSION.commit()

    class GetEnrichedStockDataTest:
        def test_should_return_all_values(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1, product_type='EventType.CINEMA')
            create_product(id=2, product_type='ThingType.LIVRE_EDITION')
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1, product_type='EventType.CINEMA', name="Test")
            create_stock(id=1, offer_id=3, booking_limit_datetime='2019-11-23',
                         beginning_datetime='2019-11-24', available=10, date_created='2019-11-01')
            create_offer(id=2, venue_id=1, product_id=2, name="Test bis", product_type='ThingType.LIVRE_EDITION')
            create_stock(id=2, offer_id=2, available=12, date_created='2019-10-01')
            create_booking(user_id=1, stock_id=1, quantity=2, id=4)
            create_payment(booking_id=4, id=1)
            create_payment_status(payment_id=1, status='PENDING', date='2019-01-01', id=1)

            create_stock_view()
            create_stocks_offer_view()
            create_stock_venue_view()
            create_stocks_booking_view()

            expected_stocks_details = pandas.DataFrame(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data={"Identifiant de l'offre": [3, 2],
                      "Nom de l'offre": ["Test", "Test bis"],
                      "offerer_id": [3, 3],
                      "Type d'offre": ["EventType.CINEMA", 'ThingType.LIVRE_EDITION'],
                      "Département": [None, None],
                      "Date de création du stock": [datetime(2019, 11, 1), datetime(2019, 10, 1)],
                      "Date limite de réservation": [datetime(2019, 11, 23), pandas.NaT],
                      "Date de début de l'évènement": [datetime(2019, 11, 24), pandas.NaT],
                      "Stock disponible brut de réservations": [10, 12],
                      "Nombre total de réservations": [2, 0],
                      "Nombre de réservations annulées": [0, 0],
                      "Nombre de réservations ayant un paiement": [2, 0]

                      }
            )

            # When
            create_enriched_stock_data()

            # Then
            stocks_details = pandas.read_sql_table('enriched_stock_data', CONNECTION, index_col='stock_id')
            pandas.testing.assert_frame_equal(stocks_details, expected_stocks_details)