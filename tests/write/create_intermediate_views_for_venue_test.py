import pandas
from pandas import Int64Index

from db import ENGINE
from tests.data_creators import (
    create_user,
    create_offerer,
    create_venue,
    create_offer,
    create_stock,
    create_booking,
    create_product,
    create_deposit,
)
from utils.database_cleaners import clean_database, clean_views
from write.create_intermediate_views_for_venue import (
    _get_number_of_bookings_per_venue,
    _get_number_of_non_cancelled_bookings_per_venue,
    _get_number_of_used_bookings,
    _get_first_offer_creation_date,
    _get_last_offer_creation_date,
    _get_offers_created_per_venue,
    _get_theoretic_revenue_per_venue,
    _get_real_revenue_per_venue,
)


class VenueQueriesTest:
    def teardown_method(self):
        clean_database()
        clean_views()

    class GetNumberOfBookingsTest:
        def test_should_return_exact_number_of_bookings(self, app):
            # Given
            create_user(id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1, product_type="ThingType.ACTIVATION")
            create_offer(
                venue_id=1, product_id=1, id=1, product_type="ThingType.ACTIVATION"
            )
            create_stock(offer_id=1, id=1)
            create_product(id=2, product_type="ThingType.MUSIQUE")
            create_offer(
                venue_id=1, product_id=2, id=2, product_type="ThingType.MUSIQUE"
            )
            create_stock(offer_id=2, id=2)
            create_booking(id=1, user_id=1, quantity=1, stock_id=2, is_used=True)
            create_booking(
                id=2, user_id=1, quantity=1, stock_id=2, token="ABC321", is_used=True
            )
            create_booking(
                id=3,
                user_id=1,
                quantity=3,
                stock_id=2,
                token="FAM321",
                is_cancelled=True,
            )
            expected_number_of_bookings_per_venue = pandas.Series(
                data=[3], name="total_bookings", index=Int64Index([1], name="venue_id")
            )

            # When
            query = _get_number_of_bookings_per_venue()

            # Then
            with ENGINE.connect() as connection:
                total_bookings_per_venue = pandas.read_sql(
                    query, connection, index_col="venue_id"
                )
            pandas.testing.assert_series_equal(
                total_bookings_per_venue["total_bookings"],
                expected_number_of_bookings_per_venue,
            )

    class GetNumberOfNonCancelledBookingsTest:
        def test_should_return_exact_number_of_non_cancelled_bookings(self, app):
            # Given
            create_user(id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1, product_type="ThingType.ACTIVATION")
            create_offer(
                venue_id=1, product_id=1, id=1, product_type="ThingType.ACTIVATION"
            )
            create_stock(offer_id=1, id=1)
            create_product(id=2, product_type="ThingType.MUSIQUE")
            create_offer(
                venue_id=1, product_id=2, id=2, product_type="ThingType.MUSIQUE"
            )
            create_stock(offer_id=2, id=2)
            create_booking(id=1, user_id=1, quantity=1, stock_id=2, is_used=True)
            create_booking(
                id=2, user_id=1, quantity=1, stock_id=2, token="ABC321", is_used=True
            )
            create_booking(
                id=3,
                user_id=1,
                quantity=3,
                stock_id=2,
                token="FAM321",
                is_cancelled=True,
            )
            create_booking(id=4, user_id=1, quantity=3, stock_id=1, token="CON321")
            expected_number_of_non_cancelled_bookings_per_venue = pandas.Series(
                data=[2],
                name="non_cancelled_bookings",
                index=Int64Index([1], name="venue_id"),
            )

            # When
            query = _get_number_of_non_cancelled_bookings_per_venue()

            # Then
            with ENGINE.connect() as connection:
                total_non_cancelled_bookings_per_venue = pandas.read_sql(
                    query, connection, index_col="venue_id"
                )
            pandas.testing.assert_series_equal(
                total_non_cancelled_bookings_per_venue["non_cancelled_bookings"],
                expected_number_of_non_cancelled_bookings_per_venue,
            )

    class GetNumberOfUsedBookingsTest:
        def test_should_return_exact_number_of_used_bookings(self, app):
            # Given
            create_user(id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1, product_type="ThingType.ACTIVATION")
            create_offer(
                venue_id=1, product_id=1, id=1, product_type="ThingType.ACTIVATION"
            )
            create_stock(offer_id=1, id=1)
            create_product(id=2, product_type="ThingType.MUSIQUE")
            create_offer(
                venue_id=1, product_id=2, id=2, product_type="ThingType.MUSIQUE"
            )
            create_stock(offer_id=2, id=2)
            create_booking(id=1, user_id=1, quantity=1, stock_id=2, is_used=True)
            create_booking(
                id=2, user_id=1, quantity=1, stock_id=2, token="ABC321", is_used=True
            )
            create_booking(
                id=3,
                user_id=1,
                quantity=3,
                stock_id=2,
                token="FAM321",
                is_cancelled=True,
            )
            create_booking(id=4, user_id=1, quantity=3, stock_id=1, token="CON321")
            expected_number_of_used_bookings_per_venue = pandas.Series(
                data=[2], name="used_bookings", index=Int64Index([1], name="venue_id")
            )

            # When
            query = _get_number_of_used_bookings()

            # Then
            with ENGINE.connect() as connection:
                total_used_bookings_per_venue = pandas.read_sql(
                    query, connection, index_col="venue_id"
                )
            pandas.testing.assert_series_equal(
                total_used_bookings_per_venue["used_bookings"],
                expected_number_of_used_bookings_per_venue,
            )

    class GetFirstOfferCreationDateTest:
        def test_should_return_first_offer_creation_date(self, app):
            # Given
            create_user(id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1, product_type="ThingType.ACTIVATION")
            create_offer(
                venue_id=1, product_id=1, id=1, product_type="ThingType.ACTIVATION"
            )
            create_stock(offer_id=1, id=1)
            create_product(id=2, product_type="ThingType.MUSIQUE")
            create_offer(
                venue_id=1,
                product_id=2,
                id=2,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-10",
            )
            create_offer(
                venue_id=1,
                product_id=2,
                id=3,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-11",
            )
            create_stock(offer_id=2, id=2)
            create_booking(id=1, user_id=1, quantity=1, stock_id=2, is_used=True)
            create_booking(
                id=2, user_id=1, quantity=1, stock_id=2, token="ABC321", is_used=True
            )
            create_booking(
                id=3,
                user_id=1,
                quantity=3,
                stock_id=2,
                token="FAM321",
                is_cancelled=True,
            )
            create_booking(id=4, user_id=1, quantity=3, stock_id=1, token="CON321")
            expected_first_offer_creation_date = pandas.Series(
                data=["2020-05-10"],
                name="first_offer_creation_date",
                index=Int64Index([1], name="venue_id"),
            )
            expected_first_offer_creation_date = pandas.to_datetime(
                expected_first_offer_creation_date, format="%Y-%m-%d"
            )
            # When
            query = _get_first_offer_creation_date()

            # Then
            with ENGINE.connect() as connection:
                first_offer_creation_date = pandas.read_sql(
                    query, connection, index_col="venue_id"
                )
            pandas.testing.assert_series_equal(
                expected_first_offer_creation_date,
                first_offer_creation_date["first_offer_creation_date"],
            )

    class GetLastOfferCreationDateTest:
        def test_should_return_last_offer_creation_date(self, app):
            # Given
            create_user(id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1, product_type="ThingType.ACTIVATION")
            create_offer(
                venue_id=1,
                product_id=1,
                id=1,
                product_type="ThingType.ACTIVATION",
                date_created="2020-05-13",
            )
            create_stock(offer_id=1, id=1)
            create_product(id=2, product_type="ThingType.MUSIQUE")
            create_offer(
                venue_id=1,
                product_id=2,
                id=2,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-10",
            )
            create_offer(
                venue_id=1,
                product_id=2,
                id=3,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-11",
            )
            create_stock(offer_id=2, id=2)
            create_booking(id=1, user_id=1, quantity=1, stock_id=2, is_used=True)
            create_booking(
                id=2, user_id=1, quantity=1, stock_id=2, token="ABC321", is_used=True
            )
            create_booking(
                id=3,
                user_id=1,
                quantity=3,
                stock_id=2,
                token="FAM321",
                is_cancelled=True,
            )
            create_booking(id=4, user_id=1, quantity=3, stock_id=1, token="CON321")
            expected_last_offer_creation_date = pandas.Series(
                data=["2020-05-11"],
                name="last_offer_creation_date",
                index=Int64Index([1], name="venue_id"),
            )
            expected_last_offer_creation_date = pandas.to_datetime(
                expected_last_offer_creation_date, format="%Y-%m-%d"
            )
            # When
            query = _get_last_offer_creation_date()

            # Then
            with ENGINE.connect() as connection:
                last_offer_creation_date = pandas.read_sql(
                    query, connection, index_col="venue_id"
                )
            pandas.testing.assert_series_equal(
                expected_last_offer_creation_date,
                last_offer_creation_date["last_offer_creation_date"],
            )

    class GetOffersCreatedPerVenueTest:
        def test_should_return_number_of_offers_created_per_venue(self, app):
            # Given
            create_user(id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1, product_type="ThingType.ACTIVATION")
            create_offer(
                venue_id=1,
                product_id=1,
                id=1,
                product_type="ThingType.ACTIVATION",
                date_created="2020-05-13",
            )
            create_stock(offer_id=1, id=1)
            create_product(id=2, product_type="ThingType.MUSIQUE")
            create_offer(
                venue_id=1,
                product_id=2,
                id=2,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-10",
            )
            create_offer(
                venue_id=1,
                product_id=2,
                id=3,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-11",
            )
            create_stock(offer_id=2, id=2)
            create_booking(id=1, user_id=1, quantity=1, stock_id=2, is_used=True)
            create_booking(
                id=2, user_id=1, quantity=1, stock_id=2, token="ABC321", is_used=True
            )
            create_booking(
                id=3,
                user_id=1,
                quantity=3,
                stock_id=2,
                token="FAM321",
                is_cancelled=True,
            )
            create_booking(id=4, user_id=1, quantity=3, stock_id=1, token="CON321")
            expected_offers_created_per_venue = pandas.Series(
                data=[2], name="offers_created", index=Int64Index([1], name="venue_id")
            )
            # When
            query = _get_offers_created_per_venue()

            # Then
            with ENGINE.connect() as connection:
                offers_created_per_venue = pandas.read_sql(
                    query, connection, index_col="venue_id"
                )
            pandas.testing.assert_series_equal(
                expected_offers_created_per_venue,
                offers_created_per_venue["offers_created"],
            )

    class TheoreticalRevenuePerVenueTest:
        def test_should_return_theoretic_revenue_per_venue(self, app):
            # Given
            create_user(id=1)
            create_deposit(id=1, user_id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1, product_type="ThingType.ACTIVATION")
            create_offer(
                venue_id=1,
                product_id=1,
                id=1,
                product_type="ThingType.ACTIVATION",
                date_created="2020-05-13",
            )
            create_stock(offer_id=1, id=1)
            create_product(id=2, product_type="ThingType.MUSIQUE")
            create_offer(
                venue_id=1,
                product_id=2,
                id=2,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-10",
            )
            create_offer(
                venue_id=1,
                product_id=2,
                id=3,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-11",
            )
            create_stock(offer_id=2, id=2, price=10)
            create_stock(offer_id=3, id=3, price=20)
            create_booking(
                id=1, user_id=1, quantity=1, stock_id=2, amount=10, is_used=True
            )
            create_booking(
                id=2,
                user_id=1,
                quantity=1,
                stock_id=2,
                amount=10,
                token="ABC321",
                is_used=True,
            )
            create_booking(
                id=3,
                user_id=1,
                quantity=1,
                stock_id=2,
                amount=10,
                token="FAM321",
                is_cancelled=True,
            )
            create_booking(
                id=4,
                user_id=1,
                quantity=1,
                stock_id=1,
                token="CON321",
                is_cancelled=False,
            )
            create_booking(
                id=5,
                user_id=1,
                quantity=2,
                stock_id=3,
                amount=20,
                token="LAB123",
                is_cancelled=False,
            )
            expected_theoretic_revenue_per_venue = pandas.Series(
                data=[60.0],
                name="theoretic_revenue",
                index=Int64Index([1], name="venue_id"),
            )
            # When
            query = _get_theoretic_revenue_per_venue()

            # Then
            with ENGINE.connect() as connection:
                theoretic_revenue_per_venue = pandas.read_sql(
                    query, connection, index_col="venue_id"
                )
            pandas.testing.assert_series_equal(
                expected_theoretic_revenue_per_venue,
                theoretic_revenue_per_venue["theoretic_revenue"],
            )

    class RealRevenuePerVenueTest:
        def test_should_return_real_revenue_per_venue(self, app):
            # Given
            create_user(id=1)
            create_deposit(id=1, user_id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1, product_type="ThingType.ACTIVATION")
            create_offer(
                venue_id=1,
                product_id=1,
                id=1,
                product_type="ThingType.ACTIVATION",
                date_created="2020-05-13",
            )
            create_stock(offer_id=1, id=1)
            create_product(id=2, product_type="ThingType.MUSIQUE")
            create_offer(
                venue_id=1,
                product_id=2,
                id=2,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-10",
            )
            create_offer(
                venue_id=1,
                product_id=2,
                id=3,
                product_type="ThingType.MUSIQUE",
                date_created="2020-05-11",
            )
            create_stock(offer_id=2, id=2, price=10)
            create_stock(offer_id=3, id=3, price=20)
            create_booking(
                id=1, user_id=1, quantity=1, stock_id=2, amount=10, is_used=True
            )
            create_booking(
                id=2,
                user_id=1,
                quantity=1,
                stock_id=2,
                amount=10,
                token="ABC321",
                is_used=True,
            )
            create_booking(
                id=3,
                user_id=1,
                quantity=1,
                stock_id=2,
                amount=10,
                token="FAM321",
                is_cancelled=True,
            )
            create_booking(
                id=4,
                user_id=1,
                quantity=1,
                stock_id=1,
                token="CON321",
                is_cancelled=False,
            )
            create_booking(
                id=5,
                user_id=1,
                quantity=2,
                stock_id=3,
                amount=20,
                token="LAB123",
                is_cancelled=False,
            )
            expected_real_revenue_per_venue = pandas.Series(
                data=[20.0], name="real_revenue", index=Int64Index([1], name="venue_id")
            )
            # When
            query = _get_real_revenue_per_venue()

            # Then
            with ENGINE.connect() as connection:
                real_revenue_per_venue = pandas.read_sql(
                    query, connection, index_col="venue_id"
                )
            pandas.testing.assert_series_equal(
                expected_real_revenue_per_venue, real_revenue_per_venue["real_revenue"]
            )
