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
    create_payment,
    create_payment_status,
)
from utils.database_cleaners import clean_database, clean_views
from write.create_intermediate_views_for_booking import (
    get_booking_amount,
    get_booking_payment_status,
    get_booking_ranking,
    get_booking_ranking_in_category,
)


class BookingQueriesTest:
    def teardown_method(self):
        clean_database()
        clean_views()

    class GetBookingAmountTest:
        def test_should_return_exact_booking_amount(self, app):
            # Given
            create_user(id=1)
            create_deposit(user_id=1)
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
                id=2,
                user_id=1,
                quantity=1,
                amount=10,
                stock_id=2,
                token="ABC321",
                is_used=True,
            )
            create_booking(
                id=3,
                user_id=1,
                quantity=3,
                amount=5,
                stock_id=2,
                token="FAM321",
                is_cancelled=True,
            )
            expected_amount_per_booking = pandas.Series(
                data=[0.0, 10.0, 15.0],
                name="Montant de la réservation",
                index=Int64Index([1, 2, 3], name="booking_id"),
            )

            # When
            query = get_booking_amount()

            # Then
            with ENGINE.connect() as connection:
                booking_amount = pandas.read_sql(
                    query, connection, index_col="booking_id"
                )
            pandas.testing.assert_series_equal(
                booking_amount["Montant de la réservation"], expected_amount_per_booking
            )

        class GetBookingPaymentTest:
            def test_should_return_booking_payment_status(self, app):
                # Given
                create_user(id=1)
                create_deposit(user_id=1)
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
                    id=2,
                    user_id=1,
                    quantity=1,
                    amount=10,
                    stock_id=2,
                    token="ABC321",
                    is_used=True,
                )
                create_booking(
                    id=3,
                    user_id=1,
                    quantity=3,
                    amount=5,
                    stock_id=2,
                    token="FAM321",
                    is_cancelled=True,
                )
                create_payment(id=1, booking_id=2, amount=10)
                create_payment_status(id=1, payment_id=1, status="SENT")
                create_payment(id=2, booking_id=3, amount=15)
                create_payment_status(id=2, payment_id=2, status="PENDING")
                expected_booking_payment_status = pandas.Series(
                    data=["Remboursé"],
                    name="Remboursé",
                    index=Int64Index([2], name="booking_id"),
                )

                # When
                query = get_booking_payment_status()

                # Then
                with ENGINE.connect() as connection:
                    booking_payment = pandas.read_sql(
                        query, connection, index_col="booking_id"
                    )
                pandas.testing.assert_series_equal(
                    booking_payment["Remboursé"], expected_booking_payment_status
                )

        class GetBookingRankingsTest:
            def test_should_return_booking_ranking(app):
                # Given
                create_user(id=1)
                create_user(id=2, email="test@passculture.app")
                create_deposit(id=1, user_id=1)
                create_deposit(id=2, user_id=2)
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
                    id=2,
                    user_id=1,
                    quantity=1,
                    amount=10,
                    stock_id=2,
                    token="ABC321",
                    is_used=True,
                    date_created="2020-05-11",
                )
                create_booking(
                    id=3,
                    user_id=1,
                    quantity=3,
                    amount=5,
                    stock_id=2,
                    token="FAM321",
                    is_cancelled=True,
                    date_created="2020-06-21",
                )
                create_booking(
                    id=4,
                    user_id=2,
                    quantity=1,
                    amount=5,
                    stock_id=2,
                    token="OLD321",
                    is_used=True,
                    date_created="2020-06-21",
                )
                create_payment(id=1, booking_id=2, amount=10)
                create_payment_status(id=1, payment_id=1, status="SENT")
                create_payment(id=2, booking_id=3, amount=15)
                create_payment_status(id=2, payment_id=2, status="PENDING")
                expected_booking_ranking = pandas.Series(
                    data=[1, 2, 3, 1],
                    name="Classement de la réservation",
                    index=Int64Index([1, 2, 3, 4], name="booking_id"),
                )

                # When
                query = get_booking_ranking()

                # Then
                with ENGINE.connect() as connection:
                    booking_ranking = pandas.read_sql(
                        query, connection, index_col="booking_id"
                    )
                pandas.testing.assert_series_equal(
                    booking_ranking["Classement de la réservation"],
                    expected_booking_ranking,
                )

            def test_should_return_booking_ranking(app):
                # Given
                create_user(id=1)
                create_user(id=2, email="test@passculture.app")
                create_deposit(id=1, user_id=1)
                create_deposit(id=2, user_id=2)
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
                create_product(id=3, product_type="ThingType.AUDIOVISUEL")
                create_offer(
                    venue_id=1, product_id=3, id=3, product_type="ThingType.AUDIOVISUEL"
                )
                create_stock(offer_id=3, id=3)
                create_booking(id=1, user_id=1, quantity=1, stock_id=2, is_used=True)
                create_booking(
                    id=2,
                    user_id=1,
                    quantity=1,
                    amount=10,
                    stock_id=3,
                    token="ABC321",
                    is_used=True,
                    date_created="2020-05-11",
                )
                create_booking(
                    id=3,
                    user_id=1,
                    quantity=3,
                    amount=5,
                    stock_id=2,
                    token="FAM321",
                    is_cancelled=True,
                    date_created="2020-06-21",
                )
                create_booking(
                    id=4,
                    user_id=2,
                    quantity=1,
                    amount=5,
                    stock_id=3,
                    token="OLD321",
                    is_used=True,
                    date_created="2020-06-21",
                )
                create_booking(
                    id=5,
                    user_id=2,
                    quantity=1,
                    amount=5,
                    stock_id=2,
                    token="YOO321",
                    is_used=True,
                    date_created="2020-10-25",
                )
                create_payment(id=1, booking_id=2, amount=10)
                create_payment_status(id=1, payment_id=1, status="SENT")
                create_payment(id=2, booking_id=3, amount=15)
                create_payment_status(id=2, payment_id=2, status="PENDING")
                expected_booking_ranking = pandas.Series(
                    data=[1, 1, 2, 1, 1],
                    name="Classement de la réservation dans la même catégorie",
                    index=Int64Index([1, 2, 3, 4, 5], name="booking_id"),
                )

                # When
                query = get_booking_ranking_in_category()

                # Then
                with ENGINE.connect() as connection:
                    booking_ranking = pandas.read_sql(
                        query, connection, index_col="booking_id"
                    )
                pandas.testing.assert_series_equal(
                    booking_ranking[
                        "Classement de la réservation dans la même catégorie"
                    ],
                    expected_booking_ranking,
                )
