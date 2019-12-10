from datetime import datetime

import pandas
import pytest

from db import CONNECTION, ENGINE
from tests.utils import create_user, create_offerer, create_venue, create_offer, create_stock, \
    create_booking, create_recommendation, create_product, update_table_column
from user_queries import get_beneficiary_users_details, get_experimentation_sessions, \
    get_departments, get_activation_dates, get_typeform_filling_dates, get_first_connection_dates, \
    get_date_of_first_bookings, get_date_of_second_bookings, get_date_of_bookings_on_third_product_type, \
    get_last_recommendation_dates, get_number_of_bookings, get_number_of_non_cancelled_bookings


class UserQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_class(self):
        ENGINE.execute('''
                        DELETE FROM "recommendation";
                        DELETE FROM "booking";
                        DELETE FROM "stock";
                        DELETE FROM "offer";
                        DELETE FROM "product";
                        DELETE FROM "venue";
                        DELETE FROM "mediation";
                        DELETE FROM "offerer";
                        DELETE FROM "user";
                        DELETE FROM activity;
                        ''')

    class GetAllExperimentationUsersDetailsTest:
        def test_should_not_return_details_when_user_cannot_book_free_offers(self):
            # Given
            create_user(can_book_free_offers=False)

            # When
            beneficiary_users_details = get_beneficiary_users_details(CONNECTION)

            # Then
            assert beneficiary_users_details.empty

        def test_should_return_values_for_users_who_can_book_free_offers(self):
            # Given
            date_before_changes = datetime.now()
            activation_id = 1
            active_user_id = 2
            create_user(can_book_free_offers=True, departement_code="93",
                        date_created=datetime(2019, 1, 1, 12, 0, 0), needs_to_fill_cultural_survey=True, id=1)
            create_user(can_book_free_offers=True, departement_code="08", email="em@a.il",
                        needs_to_fill_cultural_survey=True, id=active_user_id)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=activation_id, product_type='ThingType.ACTIVATION')
            create_offer(venue_id=1, product_id=activation_id, id=1, product_type='ThingType.ACTIVATION')
            create_stock(offer_id=activation_id, id=1)
            create_booking(user_id=active_user_id, stock_id=activation_id, is_used=False, id=1)
            create_product(id=2, product_type='ThingType.JEUX_VIDEO')
            create_product(id=3, product_type='ThingType.AUDIOVISUEL')
            create_product(id=4, product_type='ThingType.CINEMA_ABO')
            create_offer(venue_id=1, product_id=2, product_type='ThingType.JEUX_VIDEO', id=2)
            create_offer(venue_id=1, product_id=3, product_type='ThingType.AUDIOVISUEL', id=3)
            create_offer(venue_id=1, product_id=4, product_type='ThingType.CINEMA_ABO', id=4)

            create_recommendation(offer_id=activation_id, user_id=active_user_id, date_created=datetime(2019, 2, 3),
                                  id=1)
            create_stock(offer_id=2, id=2)
            create_stock(offer_id=3, id=3)
            create_stock(offer_id=4, id=4)
            create_booking(user_id=active_user_id, stock_id=2, date_created=datetime(2019, 3, 7), token='18J2K1', id=2)
            create_booking(user_id=active_user_id, stock_id=3, date_created=datetime(2019, 4, 7), token='1U2I12', id=3)
            create_booking(user_id=active_user_id, stock_id=4, date_created=datetime(2019, 5, 7), token='J91U21',
                           is_cancelled=True, id=4)
            update_table_column(table_name='booking', id=activation_id, column='"isUsed"', value='True')
            update_table_column(table_name='"user"', id=active_user_id, column='"needsToFillCulturalSurvey"',
                                value='False')
            recommendation_creation_date = datetime.utcnow()
            create_recommendation(offer_id=3, user_id=active_user_id, date_created=recommendation_creation_date, id=2)
            date_after_changes = datetime.utcnow()

            expected_first_row = pandas.Series(
                data=[active_user_id, "93", datetime(2019, 1, 1, 12, 0, 0), pandas.NaT, pandas.NaT, pandas.NaT,
                      pandas.NaT, pandas.NaT, pandas.NaT, 0,
                      0],
                index=["Vague d'expérimentation", "Département", "Date d'activation", "Date de remplissage du typeform",
                       "Date de première connexion", "Date de première réservation", "Date de deuxième réservation",
                       "Date de première réservation dans 3 catégories différentes", "Date de dernière recommandation",
                       "Nombre de réservations totales", "Nombre de réservations non annulées"])
            columns_to_check_for_exact_values_in_second_row = ["Vague d'expérimentation",
                                                               "Département",
                                                               "Date de première connexion",
                                                               "Date de première réservation",
                                                               "Date de deuxième réservation",
                                                               "Date de première réservation dans 3 catégories différentes",
                                                               "Date de dernière recommandation",
                                                               "Nombre de réservations totales",
                                                               "Nombre de réservations non annulées"]
            expected_second_row = pandas.Series(
                data=[1, "08", datetime(2019, 2, 3), datetime(2019, 3, 7), datetime(2019, 4, 7), datetime(2019, 5, 7),
                      recommendation_creation_date, 3, 2], index=columns_to_check_for_exact_values_in_second_row)

            # When
            beneficiary_users_details = get_beneficiary_users_details(CONNECTION)

            # Then
            assert beneficiary_users_details.shape == (2, 11)
            actual = beneficiary_users_details.loc[0]
            assert actual.equals(expected_first_row)
            actual = beneficiary_users_details.loc[1, columns_to_check_for_exact_values_in_second_row]
            assert actual.equals(
                expected_second_row
            )
            assert date_before_changes < beneficiary_users_details.loc[1, "Date d'activation"] < date_after_changes
            assert date_before_changes < beneficiary_users_details.loc[
                1, "Date de remplissage du typeform"] < date_after_changes

    class GetExperimentationSessionsTest:
        def test_should_return_1_when_user_has_used_activation_booking(self):
            # Given
            create_user(id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1, product_type='ThingType.ACTIVATION')
            create_offer(venue_id=1, product_id=1, id=1, product_type='ThingType.ACTIVATION')
            create_stock(offer_id=1)
            create_booking(user_id=1, stock_id=1, is_used=True)

            # When
            experimentation_sessions = get_experimentation_sessions(CONNECTION)

            # Then
            assert experimentation_sessions["Vague d'expérimentation"].equals(
                pandas.Series(data=[1], name="Vague d'expérimentation", index=[1]))

        def test_should_return_2_when_user_has_unused_activation_booking(self):
            # Given
            create_user(id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1)
            create_product(product_type='ThingType.ACTIVATION', id=1)
            create_offer(venue_id=1, product_type='ThingType.ACTIVATION', product_id=1, id=1)
            create_stock(offer_id=1)
            create_booking(user_id=1, stock_id=1, is_used=False)

            # When
            experimentation_sessions = get_experimentation_sessions(CONNECTION)

            # Then
            assert experimentation_sessions["Vague d'expérimentation"].equals(
                pandas.Series(data=[2], name="Vague d'expérimentation", index=[1]))

        def test_should_return_2_when_user_does_not_have_activation_booking(self):
            # Given
            create_user()

            # When
            experimentation_sessions = get_experimentation_sessions(CONNECTION)

            # Then
            assert experimentation_sessions["Vague d'expérimentation"].equals(
                pandas.Series(data=[2], name="Vague d'expérimentation", index=[1]))

        def test_should_return_an_empty_series_if_user_cannot_book_free_offers(self):
            # Given
            create_user(can_book_free_offers=False)

            # When
            experimentation_sessions = get_experimentation_sessions(CONNECTION)

            # Then
            assert experimentation_sessions["Vague d'expérimentation"].empty

    class GetDepartmentsTest:
        def test_should_return_user_departement_code_when_user_can_book_free_offer(self):
            # Given
            create_user(departement_code="01", id=1)

            # When
            departements = get_departments(CONNECTION)

            # Then
            assert departements["Département"].equals(
                pandas.Series(data=["01"], name="Département", index=[1]))

        def test_should_return_empty_series_when_user_cannot_book_free_offer(self):
            # Given
            create_user(departement_code="01", can_book_free_offers=False)

            # When
            departements = get_departments(CONNECTION)

            # Then
            assert departements["Département"].empty

        class GetActivationDateTest:
            def test_should_return_the_date_at_which_the_activation_booking_was_used(self):
                # Given
                date_before_changes = datetime.utcnow()
                create_user(date_created=datetime(2019, 8, 31), id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.ACTIVATION')
                create_offer(id=1, product_type='ThingType.ACTIVATION', product_id=1, venue_id=1)
                create_stock(offer_id=1, id=1)
                create_booking(user_id=1, stock_id=1, id=1)
                update_table_column(id=1, table_name='booking', column='"isUsed"', value=True)
                date_after_used = datetime.utcnow()

                # When
                activation_dates = get_activation_dates(CONNECTION)

                # Then
                assert date_before_changes < activation_dates.loc[1, "Date d'activation"] < date_after_used

            def test_should_return_the_date_at_which_the_user_was_created_when_no_activation_booking(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31))

                # When
                activation_dates = get_activation_dates(CONNECTION)

                # Then
                assert activation_dates["Date d'activation"].equals(
                    pandas.Series(data=[datetime(2019, 8, 31)], name="Date d'activation", index=[1]))

            def test_should_return_the_date_at_which_the_user_was_created_when_non_used_activation_booking(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31), id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.ACTIVATION')
                create_offer(id=1, product_type='ThingType.ACTIVATION', product_id=1, venue_id=1)
                create_stock(offer_id=1)
                create_booking(user_id=1, stock_id=1)

                # When
                activation_dates = get_activation_dates(CONNECTION)

                # Then
                assert activation_dates["Date d'activation"].equals(
                    pandas.Series(data=[datetime(2019, 8, 31)], name="Date d'activation", index=[1]))

            def test_should_return_empty_series_when_user_cannot_book_free_offers(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31), can_book_free_offers=False, id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.ACTIVATION')
                create_offer(id=1, product_type='ThingType.ACTIVATION', product_id=1, venue_id=1)
                create_stock(offer_id=1)
                create_booking(user_id=1, stock_id=1)

                # When
                activation_dates = get_activation_dates(CONNECTION)

                # Then
                assert activation_dates["Date d'activation"].empty

        class GetTypeformFillingDatesTest:
            def test_should_return_the_date_at_which_needs_to_fill_cultural_survey_was_updated_to_false(self):
                # Given
                date_before_changes = datetime.utcnow()
                create_user(needs_to_fill_cultural_survey=True, id=1)
                update_table_column(id=1, table_name='"user"', column='"needsToFillCulturalSurvey"', value=False)
                date_after_used = datetime.utcnow()

                # When
                typeform_filling_dates = get_typeform_filling_dates(CONNECTION)

                # Then
                assert date_before_changes < typeform_filling_dates.loc[
                    1, "Date de remplissage du typeform"] < date_after_used

            def test_should_return_the_last_date_at_which_needs_to_fill_cultural_survey_was_updated_to_false_when_two_updates(
                    self):
                # Given
                create_user(needs_to_fill_cultural_survey=True, id=1)
                update_table_column(id=1, table_name='"user"', column='"needsToFillCulturalSurvey"', value=False)
                after_first_update = datetime.utcnow()
                update_table_column(id=1, table_name='"user"', column='"needsToFillCulturalSurvey"', value=True)
                update_table_column(id=1, table_name='"user"', column='"needsToFillCulturalSurvey"', value=False)
                date_after_second_update = datetime.utcnow()

                # When
                typeform_filling_dates = get_typeform_filling_dates(CONNECTION)

                # Then
                assert after_first_update < typeform_filling_dates.loc[
                    1, "Date de remplissage du typeform"] < date_after_second_update

            def test_should_return_None_when_has_filled_cultural_survey_was_never_updated_to_false(self):
                # Given
                create_user(needs_to_fill_cultural_survey=True, id=1)

                # When
                typeform_filling_dates = get_typeform_filling_dates(CONNECTION)

                # Then
                assert typeform_filling_dates.loc[1, "Date de remplissage du typeform"] is None

            def test_should_return_None_when_has_filled_cultural_survey_was_never_updated_to_false(self):
                # Given
                create_user(needs_to_fill_cultural_survey=True, id=1)

                # When
                typeform_filling_dates = get_typeform_filling_dates(CONNECTION)

                # Then
                assert typeform_filling_dates.loc[1, "Date de remplissage du typeform"] is None

            def test_should_return_empty_series_if_user_cannot_book_free_offers(self):
                # Given
                create_user(can_book_free_offers=False)

                # When
                typeform_filling_dates = get_typeform_filling_dates(CONNECTION)

                # Then
                assert typeform_filling_dates.empty

        class GetFirstConnectionDatesTest:
            def test_should_return_the_creation_date_of_the_first_recommendation_of_the_user(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31), can_book_free_offers=True, id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=1, product_type='ThingType.AUDIOVISUEL', product_id=1, venue_id=1)
                create_recommendation(offer_id=1, user_id=1, date_created=datetime(2019, 1, 1))

                # When
                first_connections = get_first_connection_dates(CONNECTION)

                # Then
                assert first_connections.loc[1, "Date de première connexion"] == datetime(2019, 1, 1)

            def test_should_return_None_if_the_user_has_no_recommendation(self):
                # Given
                create_user()

                # When
                first_connections = get_first_connection_dates(CONNECTION)

                # Then
                assert first_connections.loc[1, "Date de première connexion"] is None

            def test_should_return_empty_series_if_user_cannot_book_free_offers(self):
                # Given
                create_user(can_book_free_offers=False)

                # When
                first_connections = get_first_connection_dates(CONNECTION)

                # Then
                assert first_connections.empty

        class GetDateOfFirstBookingsTest:

            def test_should_return_the_creation_date_of_the_user_s_first_booking(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31), can_book_free_offers=True, id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=1, product_type='ThingType.AUDIOVISUEL', product_id=1, venue_id=1)
                create_stock(offer_id=1)
                first_booking_date = datetime(2019, 9, 19, 12, 0, 0)
                create_booking(user_id=1, stock_id=1, date_created=first_booking_date)

                # When
                first_booking_dates = get_date_of_first_bookings(CONNECTION)

                # Then
                assert first_booking_dates.loc[1, "Date de première réservation"] == first_booking_date

            def test_should_return_None_when_the_user_has_not_booked(self):
                # Given
                create_user()

                # When
                first_booking_dates = get_date_of_first_bookings(CONNECTION)

                # Then
                assert first_booking_dates.loc[1, "Date de première réservation"] is None

            def test_should_return_None_when_the_user_only_booked_an_activation_offer(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31), can_book_free_offers=True, id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.ACTIVATION')
                create_offer(id=1, product_type='ThingType.ACTIVATION', product_id=1, venue_id=1)
                create_stock(offer_id=1, id=1)
                first_booking_date = datetime(2019, 9, 19, 12, 0, 0)
                create_booking(user_id=1, stock_id=1, date_created=first_booking_date)

                # When
                first_booking_dates = get_date_of_first_bookings(CONNECTION)

                # Then
                assert first_booking_dates.loc[1, "Date de première réservation"] is None

            def test_should_return_an_empty_series_when_user_cannot_book_free_offers(self):
                # Given
                user = create_user(can_book_free_offers=False)

                # When
                first_booking_dates = get_date_of_first_bookings(CONNECTION)

                # Then
                assert first_booking_dates.empty

        class GetNumberOfSecondBookingsTest:

            def test_should_return_the_creation_date_of_the_user_s_second_booking(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31), can_book_free_offers=True, id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=1, product_type='ThingType.AUDIOVISUEL', product_id=1, venue_id=1)
                create_stock(offer_id=1, id=1)
                first_booking_date = datetime(2019, 9, 19, 12, 0, 0)
                second_booking_date = datetime(2019, 9, 22, 12, 0, 0)
                create_booking(user_id=1, stock_id=1, date_created=first_booking_date, id=1, token='OIA023')
                create_booking(user_id=1, stock_id=1, date_created=second_booking_date, id=2)

                # When
                second_booking_dates = get_date_of_second_bookings(CONNECTION)

                # Then
                assert second_booking_dates.loc[1, "Date de deuxième réservation"] == second_booking_date

            def test_should_return_None_when_user_has_only_one_booking(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31), can_book_free_offers=True, id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=1, product_type='ThingType.AUDIOVISUEL', product_id=1, venue_id=1)
                create_stock(offer_id=1, id=1)
                first_booking_date = datetime(2019, 9, 19, 12, 0, 0)
                booking = create_booking(user_id=1, stock_id=1, date_created=first_booking_date)

                # When
                second_booking_dates = get_date_of_second_bookings(CONNECTION)

                # Then
                assert second_booking_dates.loc[1, "Date de deuxième réservation"] is None

            def test_should_return_None_when_the_user_has_no_more_than_one_booking_appart_from_activation_offer(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31), can_book_free_offers=True, id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=1, product_type='ThingType.AUDIOVISUEL', product_id=1, venue_id=1)
                create_stock(offer_id=1, id=1)
                create_booking(user_id=1, stock_id=1, id=1)

                create_product(id=2, product_type='ThingType.ACTIVATION')
                create_offer(id=2, product_type='ThingType.ACTIVATION', product_id=2, venue_id=1)
                create_stock(offer_id=2, id=2)
                create_booking(user_id=1, stock_id=2, id=2, token='IJ201J')

                # When
                second_booking_dates = get_date_of_second_bookings(CONNECTION)

                # Then
                assert second_booking_dates.loc[1, "Date de deuxième réservation"] is None

            def test_should_return_empty_series_when_user_cannot_book_free_offers(self):
                # Given
                create_user(can_book_free_offers=False)

                # When
                second_booking_dates = get_date_of_second_bookings(CONNECTION)

                # Then
                assert second_booking_dates.empty

        class GetNumberOfBookingsOnThirdProductTypeTest:

            def test_should_return_the_creation_date_of_the_user_s_first_booking_on_more_than_three_different_types(
                    self):
                # Given
                create_user(id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.CINEMA')
                create_offer(id=1, product_type='ThingType.CINEMA', product_id=1, venue_id=1)
                create_stock(offer_id=1, id=1)
                booking_date_cinema = datetime(2019, 9, 19, 12, 0, 0)
                create_booking(user_id=1, stock_id=1, date_created=booking_date_cinema, id=1)
                create_product(id=2, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=2, product_type='ThingType.AUDIOVISUEL', product_id=2, venue_id=1)
                create_stock(offer_id=2, id=2)
                booking_date_audiovisuel = datetime(2019, 9, 20, 12, 0, 0)
                create_booking(user_id=1, stock_id=2, date_created=booking_date_audiovisuel, token='9ZK3MK', id=2)
                create_product(id=3, product_type='ThingType.JEUX_VIDEO')
                create_offer(id=3, product_type='ThingType.JEUX_VIDEO', product_id=3, venue_id=1)
                create_stock(offer_id=3, id=3)
                booking_date_jeux_video1 = datetime(2019, 9, 21, 12, 0, 0)
                create_booking(user_id=1, stock_id=3, date_created=booking_date_jeux_video1, token='OE03J2', id=3)
                create_product(id=4, product_type='ThingType.JEUX_VIDEO')
                create_offer(id=4, product_type='ThingType.JEUX_VIDEO', product_id=4, venue_id=1)
                create_stock(offer_id=4, id=4)
                booking_date_jeux_video2 = datetime(2019, 9, 21, 12, 0, 0)
                create_booking(user_id=1, stock_id=4, date_created=booking_date_jeux_video2, token='9EJ201', id=4)

                # When
                bookings_on_third_product_type = get_date_of_bookings_on_third_product_type(CONNECTION)

                # Then
                assert bookings_on_third_product_type.loc[
                           1, "Date de première réservation dans 3 catégories différentes"] == booking_date_jeux_video1

            def test_should_return_None_when_three_different_types_are_reached_thanks_to_an_activation_offer(self):
                # Given
                create_user(id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.CINEMA')
                create_offer(id=1, product_type='ThingType.CINEMA', product_id=1, venue_id=1)
                create_stock(offer_id=1, id=1)
                booking_date_cinema = datetime(2019, 9, 19, 12, 0, 0)
                create_booking(user_id=1, stock_id=1, date_created=booking_date_cinema, id=1)
                create_product(id=2, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=2, product_type='ThingType.AUDIOVISUEL', product_id=2, venue_id=1)
                create_stock(offer_id=2, id=2)
                booking_date_audiovisuel = datetime(2019, 9, 20, 12, 0, 0)
                create_booking(user_id=1, stock_id=2, date_created=booking_date_audiovisuel, token='9ZK3MK', id=2)
                create_product(id=3, product_type='ThingType.ACTIVATION')
                create_offer(id=3, product_type='ThingType.ACTIVATION', product_id=3, venue_id=1)
                create_stock(offer_id=3, id=3)
                booking_date_jeux_video1 = datetime(2019, 9, 21, 12, 0, 0)
                create_booking(user_id=1, stock_id=3, date_created=booking_date_jeux_video1, token='OE03J2', id=3)

                # When
                bookings_on_third_product_type = get_date_of_bookings_on_third_product_type(CONNECTION)

                # Then
                assert bookings_on_third_product_type.loc[
                           1, "Date de première réservation dans 3 catégories différentes"] is None

            def test_should_return_empty_series_when_user_cannot_book_free_offers(self):
                # Given
                create_user(can_book_free_offers=False)

                # When
                bookings_on_third_product_type = get_date_of_bookings_on_third_product_type(CONNECTION)

                # Then
                assert bookings_on_third_product_type.empty

        class GetLastRecommendationDateTest:

            def test_should_return_the_creation_date_of_the_last_recommendation_created_for_user(self):
                # Given
                create_user(date_created=datetime(2019, 8, 31), can_book_free_offers=True, id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=1, product_type='ThingType.AUDIOVISUEL', product_id=1, venue_id=1)
                create_recommendation(offer_id=1, user_id=1, date_created=datetime.utcnow(), id=1)
                date_after_first_recommendation = datetime.utcnow()
                create_recommendation(offer_id=1, user_id=1, date_created=datetime.utcnow(), id=2)
                date_after_second_recommendation = datetime.utcnow()

                # When
                last_recommendation_dates = get_last_recommendation_dates(CONNECTION)

                # Then
                assert date_after_first_recommendation < last_recommendation_dates.loc[
                    1, "Date de dernière recommandation"] < date_after_second_recommendation

            def test_should_return_None_when_no_recommendation_for_the_user(self):
                # Given
                create_user()

                # When
                last_recommendation_dates = get_last_recommendation_dates(CONNECTION)

                # Then
                assert last_recommendation_dates.loc[1, "Date de dernière recommandation"] is None

            def test_should_return_empty_series_when_user_cannot_book_free_offers(self):
                # Given
                create_user(can_book_free_offers=False)

                # When
                last_recommendation_dates = get_last_recommendation_dates(CONNECTION)

                # Then
                assert last_recommendation_dates.empty

        class GetNumberOfBookingsTest:

            def test_should_return_the_number_of_cancelled_and_non_cancelled_bookings_for_user_ignoring_activation_offers(
                    self):
                # Given
                create_user(id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.CINEMA')
                create_offer(id=1, product_type='ThingType.CINEMA', product_id=1, venue_id=1)
                create_stock(offer_id=1, id=1)
                create_booking(user_id=1, stock_id=1, id=1)
                create_product(id=2, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=2, product_type='ThingType.AUDIOVISUEL', product_id=2, venue_id=1)
                create_stock(offer_id=2, id=2)
                create_booking(user_id=1, stock_id=2, token='9ZK3MK', id=2)
                create_product(id=3, product_type='ThingType.ACTIVATION')
                create_offer(id=3, product_type='ThingType.ACTIVATION', product_id=3, venue_id=1)
                create_stock(offer_id=3, id=3)
                create_booking(user_id=1, stock_id=3, token='OE03J2', id=3)

                # When
                bookings = get_number_of_bookings(CONNECTION)

                # Then
                assert bookings.loc[1, "Nombre de réservations totales"] == 2

            def test_should_return_0_when_user_has_no_bookings(self):
                # Given
                create_user(id=1)

                # When
                bookings = get_number_of_bookings(CONNECTION)

                # Then
                assert bookings.loc[1, "Nombre de réservations totales"] == 0

            def test_should_return_empty_series_when_user_cannot_book_free_offers(self):
                # Given
                create_user(can_book_free_offers=False)

                # When
                bookings = get_number_of_bookings(CONNECTION)

                # Then
                assert bookings.empty

        class GetNumberOfNonCancelledBookingsTest:

            def test_should_return_the_number_of_non_cancelled_bookings_for_user_ignoring_activation_offers(self):
                # Given
                create_user(id=1)
                create_offerer(id=1)
                create_venue(offerer_id=1)
                create_product(id=1, product_type='ThingType.CINEMA')
                create_offer(id=1, product_type='ThingType.CINEMA', product_id=1, venue_id=1)
                create_stock(offer_id=1, id=1)
                create_booking(user_id=1, stock_id=1, id=1, is_cancelled=False)
                create_product(id=2, product_type='ThingType.AUDIOVISUEL')
                create_offer(id=2, product_type='ThingType.AUDIOVISUEL', product_id=2, venue_id=1)
                create_stock(offer_id=2, id=2)
                create_booking(user_id=1, stock_id=2, token='9ZK3MK', id=2, is_cancelled=True)

                # When
                non_cancelled_bookings = get_number_of_non_cancelled_bookings(CONNECTION)

                # Then
                assert non_cancelled_bookings.loc[1, "Nombre de réservations non annulées"] == 1

            def test_should_return_0_when_user_has_no_bookings(self):
                # Given
                create_user(id=1)

                # When
                non_cancelled_bookings = get_number_of_non_cancelled_bookings(CONNECTION)

                # Then
                assert non_cancelled_bookings.loc[1, "Nombre de réservations non annulées"] == 0

            def test_should_return_empty_series_when_user_cannot_book_free_offers(self):
                # Given
                create_user(can_book_free_offers=False)

                # When
                non_cancelled_bookings = get_number_of_non_cancelled_bookings(CONNECTION)

                # Then
                assert non_cancelled_bookings.empty
