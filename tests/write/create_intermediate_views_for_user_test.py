import pandas
import pytest
from freezegun import freeze_time
from pandas import Int64Index

from db import CONNECTION
from write.create_intermediate_views_for_user import _get_experimentation_sessions_query, _get_users_seniority_query, \
    _get_actual_amount_spent_query, _get_theoric_amount_spent_query, _get_theoric_amount_spent_in_digital_goods_query, \
    _get_theoric_amount_spent_in_physical_goods_query, _get_theoric_amount_spent_in_outings_query
from utils.database_cleaners import clean_database, clean_views
from tests.data_creators import create_user, create_offerer, create_venue, create_offer, create_stock, \
    create_booking, create_product, create_deposit


class UserQueriesTest:
    def teardown_method(self):
        clean_database()
        clean_views()

    class GetExperimentationSessionsTest:
        def test_should_return_1_when_user_has_used_activation_booking(self, app):
            # Given
            create_user(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1)
            create_product(app, id=1, product_type='ThingType.ACTIVATION')
            create_offer(app, venue_id=1, product_id=1, id=1, product_type='ThingType.ACTIVATION')
            create_stock(app, offer_id=1)
            create_booking(app, user_id=1, stock_id=1, is_used=True)

            # When
            query = _get_experimentation_sessions_query()

            # Then
            experimentation_sessions = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_series_equal(
                experimentation_sessions["Vague d'expérimentation"],
                pandas.Series(data=[1], name="Vague d'expérimentation", index=Int64Index([1], name='user_id'))
            )

        def test_should_return_2_when_user_has_unused_activation_booking(self, app):
            # Given
            create_user(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1, product_type='ThingType.ACTIVATION')
            create_offer(app, venue_id=1, product_id=1, id=1, product_type='ThingType.ACTIVATION')
            create_stock(app, offer_id=1)
            create_booking(app, user_id=1, stock_id=1, is_used=False)

            # When
            query = _get_experimentation_sessions_query()

            # Then
            experimentation_sessions = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_series_equal(
                experimentation_sessions["Vague d'expérimentation"],
                pandas.Series(data=[2], name="Vague d'expérimentation", index=Int64Index([1], name='user_id'))
            )

        def test_should_return_2_when_user_does_not_have_activation_booking(self, app):
            # Given
            create_user(app)

            # When
            query = _get_experimentation_sessions_query()

            # Then
            experimentation_sessions = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_series_equal(
                experimentation_sessions["Vague d'expérimentation"],
                pandas.Series(data=[2], name="Vague d'expérimentation", index=Int64Index([1], name='user_id'))
            )

        def test_should_return_1_when_user_has_one_used_and_one_unused_activation_booking(self, app):
            # Given
            create_user(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1, product_type='ThingType.ACTIVATION')
            create_offer(app, venue_id=1, product_id=1, id=1, product_type='ThingType.ACTIVATION')
            create_stock(app, offer_id=1)
            create_booking(app, user_id=1, stock_id=1, id=1, is_used=False)
            create_booking(app, user_id=1, stock_id=1, id=2, token='9JZL30', is_used=True)

            # When
            query = _get_experimentation_sessions_query()

            # Then
            experimentation_sessions = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_series_equal(
                experimentation_sessions["Vague d'expérimentation"],
                pandas.Series(data=[1], name="Vague d'expérimentation", index=Int64Index([1], name='user_id')))

        def test_should_return_an_empty_series_if_user_cannot_book_free_offers(self, app):
            # Given
            create_user(app, can_book_free_offers=False)

            # When
            query = _get_experimentation_sessions_query()

            # Then
            experimentation_sessions = pandas.read_sql(query, CONNECTION, index_col='user_id')
            assert experimentation_sessions["Vague d'expérimentation"].empty

    class GetUserSeniorityTest:
        @freeze_time('2020-01-21 11:00:00')
        def test_if_activation_dates_is_today_return_seniority_of_zero_day(self, app):
            # Given
            create_user(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1)
            create_product(app, id=1, product_type='ThingType.ACTIVATION')
            create_offer(app, venue_id=1, product_id=1, id=1, product_type='ThingType.ACTIVATION')
            create_stock(app, offer_id=1)
            create_booking(app, user_id=1, stock_id=1, is_used=True,  date_used='2020-01-22')

            # When
            query = _get_users_seniority_query()

            # Then
            user_seniority = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(user_seniority, pandas.DataFrame([0.], columns=["Ancienneté en jours"],
                                                                             index=Int64Index([1], name="user_id")))

        def test_if_activation_dates_is_empty_return_empty_series(self, app):
            # Given
            activation_dates = pandas.DataFrame([], columns=["Date d'activation"],
                                                index=Int64Index([], name="user_id"))

            # When
            query = _get_users_seniority_query()

            # Then
            user_seniority = pandas.read_sql(query, CONNECTION, index_col='user_id')
            assert user_seniority.empty

    class GetUserActualAmountSpent:

        def test_if_user_has_not_booked_return_zero(self, app):
            # Given
            create_user(app, id=45)

            # When
            query = _get_actual_amount_spent_query()

            # Then
            amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(amount_spent, pandas.DataFrame([0.], columns=["Montant réél dépensé"],
                                                                             index=Int64Index([45], name="user_id")))

        def test_if_user_has_booked_10_then_return_10(self, app):
            # Given
            create_user(app, id=45)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, id=1)
            create_deposit(app, user_id=45)
            create_booking(app, user_id=45, stock_id=1, quantity=1, amount=10, is_cancelled=False, is_used=True)

            # When
            query = _get_actual_amount_spent_query()

            # Then
            amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(amount_spent, pandas.DataFrame([10.], columns=["Montant réél dépensé"],
                                                                             index=Int64Index([45], name="user_id")))

        def test_if_booking_is_not_used_return_zero(self, app):
            # Given
            create_user(app, id=45)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, id=1)
            create_deposit(app, user_id=45)
            create_booking(app, user_id=45, stock_id=1, quantity=1, amount=10, is_cancelled=False, is_used=False)

            # When
            query = _get_actual_amount_spent_query()

            # Then
            amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(amount_spent, pandas.DataFrame([0.], columns=["Montant réél dépensé"],
                                                                             index=Int64Index([45], name="user_id")))

        def test_if_quanity_is_2_and_amount_10_return_20(self, app):
            # Given
            create_user(app, id=45)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, id=1)
            create_deposit(app, user_id=45)
            create_booking(app, user_id=45, stock_id=1, quantity=2, amount=10, is_cancelled=False, is_used=True)

            # When
            query = _get_actual_amount_spent_query()

            # Then
            amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(amount_spent, pandas.DataFrame([20.], columns=["Montant réél dépensé"],
                                                                             index=Int64Index([45], name="user_id")))

        def test_if_is_cancelled_return_0(self, app):
            # Given
            create_user(app, id=45)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, id=1)
            create_deposit(app, user_id=45)
            create_booking(app, user_id=45, stock_id=1, quantity=1, amount=10, is_cancelled=True, is_used=True)

            # When
            query = _get_actual_amount_spent_query()

            # Then
            amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(amount_spent, pandas.DataFrame([0.], columns=["Montant réél dépensé"],
                                                                             index=Int64Index([45], name="user_id")))

        def test_if_user_cannot_book_free_offer_return_empty_data_frame(self, app):
            # Given
            create_user(app, id=45, can_book_free_offers=False)

            # When
            query = _get_actual_amount_spent_query()

            # Then
            amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            assert amount_spent.empty

    class GetUserTheoricAmountSpent:

        def test_if_user_cannot_book_free_offer_return_empty_data_frame(self, app):
            # Given
            create_user(app, id=45, can_book_free_offers=False)

            # When
            query = _get_theoric_amount_spent_query()

            # Then
            theoric_amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            assert theoric_amount_spent.empty

        def test_if_user_has_not_booked_return_0(self, app):
            # Given
            create_user(app, id=45)

            # When
            query = _get_theoric_amount_spent_query()

            # Then
            theoric_amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent,
                                              pandas.DataFrame([0.], columns=["Montant théorique dépensé"],
                                                               index=Int64Index([45], name="user_id")))

        def test_if_is_cancelled_return_0(self, app):
            # Given
            create_user(app, id=45)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, id=1)
            create_deposit(app, user_id=45)
            create_booking(app, user_id=45, stock_id=1, quantity=1, amount=10, is_cancelled=True, is_used=True)

            # When
            query = _get_theoric_amount_spent_query()

            # Then
            theoric_amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent,
                                              pandas.DataFrame([0.], columns=["Montant théorique dépensé"],
                                                               index=Int64Index([45], name="user_id")))

        def test_if_booking_is_not_used_and_not_cancelled_and_amount_10_return_10(self, app):
            # Given
            create_user(app, id=45)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, id=1)
            create_deposit(app, user_id=45)
            create_booking(app, user_id=45, stock_id=1, quantity=1, amount=10, is_cancelled=False, is_used=False)

            # When
            query = _get_theoric_amount_spent_query()

            # Then
            theoric_amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent,
                                              pandas.DataFrame([10.], columns=["Montant théorique dépensé"],
                                                               index=Int64Index([45], name="user_id")))

        def test_if_booking_is_used_and_not_cancelled_and_amount_10_return_10(self, app):
            # Given
            create_user(app, id=45)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, id=1)
            create_deposit(app, user_id=45)
            create_booking(app, user_id=45, stock_id=1, quantity=1, amount=10, is_cancelled=False, is_used=True)

            # When
            query = _get_theoric_amount_spent_query()

            # Then
            theoric_amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent,
                                              pandas.DataFrame([10.], columns=["Montant théorique dépensé"],
                                                               index=Int64Index([45], name="user_id")))

        def test_if_booking_amount_10_and_quantity_2_return_20(self, app):
            # Given
            create_user(app, id=45)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, id=1)
            create_deposit(app, user_id=45)
            create_booking(app, user_id=45, stock_id=1, quantity=2, amount=10, is_cancelled=False, is_used=True)

            # When
            query = _get_theoric_amount_spent_query()

            # Then
            theoric_amount_spent = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent,
                                              pandas.DataFrame([20.], columns=["Montant théorique dépensé"],
                                                               index=Int64Index([45], name="user_id")))

    class GetTheoricAmountSpentInDigitalGoodsTest:
        def test_if_user_has_no_booking_return_0(self, app):
            # Given
            create_user(app, id=1)

            # When
            query = _get_theoric_amount_spent_in_digital_goods_query()

            # Then
            theoric_amount_spent_in_digital = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_digital,
                                              pandas.DataFrame([0.], columns=["Dépenses numériques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_user_can_book_free_offer_is_false_return_empty_data_frame(self, app):
            # Given
            create_user(app, id=1, can_book_free_offers=False)

            # When
            query = _get_theoric_amount_spent_in_digital_goods_query()

            # Then
            theoric_amount_spent_in_digital = pandas.read_sql(query, CONNECTION, index_col='user_id')
            assert theoric_amount_spent_in_digital.empty

        def test_if_booking_on_digital_good_return_amount(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.MUSIQUE')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.MUSIQUE', url='url', product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, amount=10, quantity=2, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_digital_goods_query()

            # Then
            theoric_amount_spent_in_digital = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_digital,
                                              pandas.DataFrame([20.], columns=["Dépenses numériques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_not_on_digital_good_type_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.INSTRUMENT')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.INSTRUMENT', product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_digital_goods_query()

            # Then
            theoric_amount_spent_in_digital = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_digital,
                                              pandas.DataFrame([0.], columns=["Dépenses numériques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_on_digital_good_type_but_url_empty_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.AUDIOVISUEL')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.AUDIOVISUEL', url=None, product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_digital_goods_query()

            # Then
            theoric_amount_spent_in_digital = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_digital,
                                              pandas.DataFrame([0.], columns=["Dépenses numériques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_on_digital_good_is_cancelled_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.AUDIOVISUEL')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.AUDIOVISUEL', url='url', product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, is_cancelled=True, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_digital_goods_query()

            # Then
            theoric_amount_spent_in_digital = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_digital,
                                              pandas.DataFrame([0.], columns=["Dépenses numériques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_on_non_capped_type_with_url_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.CINEMA_CARD')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.CINEMA_CARD', url='url', product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, is_cancelled=False, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_digital_goods_query()

            # Then
            theoric_amount_spent_in_digital = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_digital,
                                              pandas.DataFrame([0.], columns=["Dépenses numériques"],
                                                               index=Int64Index([1], name="user_id")))

    class GetTheoricAmountSpentInPhysicalGoodsTest:
        def test_if_user_has_no_booking_return_0(self, app):
            # Given
            create_user(app, id=1)

            # When
            query = _get_theoric_amount_spent_in_physical_goods_query()

            # Then
            theoric_amount_spent_in_physical = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_physical,
                                              pandas.DataFrame([0.], columns=["Dépenses physiques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_user_can_book_free_offer_is_false_return_empty_data_frame(self, app):
            # Given
            create_user(app, id=1, can_book_free_offers=False)

            # When
            query = _get_theoric_amount_spent_in_physical_goods_query()

            # Then
            theoric_amount_spent_in_physical = pandas.read_sql(query, CONNECTION, index_col='user_id')
            assert theoric_amount_spent_in_physical.empty

        def test_if_booking_on_physical_good_return_amount(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.INSTRUMENT')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.INSTRUMENT', url=None, product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, amount=10, quantity=2, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_physical_goods_query()

            # Then
            theoric_amount_spent_in_physical = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_physical,
                                              pandas.DataFrame([20.], columns=["Dépenses physiques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_not_on_physical_good_type_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.JEUX_VIDEO_ABO')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.JEUX_VIDEO_ABO', url='u.rl', product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_physical_goods_query()

            # Then
            theoric_amount_spent_in_physical = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_physical,
                                              pandas.DataFrame([0.], columns=["Dépenses physiques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_on_physical_good_type_but_has_url_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.MUSIQUE')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.MUSIQUE', url='u.rl', product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_physical_goods_query()

            # Then
            theoric_amount_spent_in_physical = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_physical,
                                              pandas.DataFrame([0.], columns=["Dépenses physiques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_on_physical_good_is_cancelled_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.INSTRUMENT')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.INSTRUMENT', url=None, product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, is_cancelled=True, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_physical_goods_query()

            # Then
            theoric_amount_spent_in_physical = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_physical,
                                              pandas.DataFrame([0.], columns=["Dépenses physiques"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_on_non_capped_type_without_url_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='EventType.CINEMA')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='EventType.CINEMA', url=None, product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, is_cancelled=False, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_physical_goods_query()

            # Then
            theoric_amount_spent_in_physical = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_physical,
                                              pandas.DataFrame([0.], columns=["Dépenses physiques"],
                                                               index=Int64Index([1], name="user_id")))

    class GetTheoricAmountSpentInOuting:
        def test_if_user_has_no_booking_return_0(self, app):
            # Given
            create_user(app, id=1)

            # When
            query = _get_theoric_amount_spent_in_outings_query()

            # Then
            theoric_amount_spent_in_outings = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_outings,
                                              pandas.DataFrame([0.], columns=["Dépenses sorties"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_user_can_book_free_offer_is_false_return_empty_data_frame(self, app):
            # Given
            create_user(app, id=1, can_book_free_offers=False)

            # When
            query = _get_theoric_amount_spent_in_outings_query()

            # Then
            theoric_amount_spent_in_outings = pandas.read_sql(query, CONNECTION, index_col='user_id')
            assert theoric_amount_spent_in_outings.empty

        def test_if_booking_on_outings_return_amount(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.MUSEES_PATRIMOINE_ABO')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.MUSEES_PATRIMOINE_ABO', product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, amount=10, quantity=2, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_outings_query()

            # Then
            theoric_amount_spent_in_outings = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_outings,
                                              pandas.DataFrame([20.], columns=["Dépenses sorties"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_not_on_outings_type_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.JEUX_VIDEO_ABO')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.JEUX_VIDEO_ABO', url='u.rl', product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_outings_query()

            # Then
            theoric_amount_spent_in_outings = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_outings,
                                              pandas.DataFrame([0.], columns=["Dépenses sorties"],
                                                               index=Int64Index([1], name="user_id")))

        def test_if_booking_on_outings_is_cancelled_return_0(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.SPECTACLE_VIVANT_ABO')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.SPECTACLE_VIVANT_ABO', url=None, product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, user_id=1, is_cancelled=True, amount=10, quantity=1, stock_id=20)

            # When
            query = _get_theoric_amount_spent_in_outings_query()

            # Then
            theoric_amount_spent_in_outings = pandas.read_sql(query, CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(theoric_amount_spent_in_outings,
                                              pandas.DataFrame([0.], columns=["Dépenses sorties"],
                                                               index=Int64Index([1], name="user_id")))
