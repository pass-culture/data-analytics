from datetime import datetime

import pandas

from db import ENGINE
from tests.data_creators import (
    create_user,
    create_product,
    create_offerer,
    create_venue,
    create_offer,
    create_stock,
    create_booking,
    create_payment_status,
    create_payment,
    create_favorite,
)
from utils.database_cleaners import clean_database, clean_views
from write.create_intermediate_views_for_stock import (
    create_stocks_booking_view,
    create_available_stocks_view,
    create_enriched_stock_view,
)
from write.create_views import (
    create_enriched_user_data,
    create_enriched_offerer_data,
    create_enriched_offer_data,
    create_enriched_venue_data,
    create_enriched_booking_data
)


class ViewQueriesTest:
    class CreateEnrichedStockViewTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        def test_should_return_all_values(self):
            # Given
            create_user(id=1)
            create_user(id=2, email="other@test.com")
            create_product(id=1, product_type="EventType.CINEMA")
            create_product(id=2, product_type="ThingType.LIVRE_EDITION")
            create_offerer(id=3)
            create_venue(
                offerer_id=3,
                id=1,
                siret=None,
                postal_code=None,
                city=None,
                departement_code=None,
                is_virtual=True,
            )
            create_offer(
                venue_id=1,
                product_id=1,
                id=3,
                product_type="EventType.CINEMA",
                name="Test",
            )
            create_stock(
                offer_id=3,
                id=1,
                date_created="2019-11-01",
                quantity=10,
                booking_limit_datetime="2019-11-23",
                beginning_datetime="2019-11-24",
            )
            create_offer(
                venue_id=1,
                product_id=2,
                id=2,
                product_type="ThingType.LIVRE_EDITION",
                name="Test bis",
            )
            create_stock(offer_id=2, id=2, date_created="2019-10-01", quantity=12)
            create_booking(user_id=1, stock_id=1, id=4, quantity=2)
            create_payment(booking_id=4, id=1)
            create_payment_status(
                payment_id=1, id=1, date="2019-01-01", status="PENDING"
            )

            create_stocks_booking_view(ENGINE)
            create_available_stocks_view(ENGINE)

            expected_stocks_details = pandas.DataFrame(
                index=pandas.Index(data=[1, 2], name="stock_id"),
                data={
                    "Identifiant de l'offre": [3, 2],
                    "Nom de l'offre": ["Test", "Test bis"],
                    "offerer_id": [3, 3],
                    "Type d'offre": ["EventType.CINEMA", "ThingType.LIVRE_EDITION"],
                    "Département": [None, None],
                    "Date de création du stock": [
                        datetime(2019, 11, 1),
                        datetime(2019, 10, 1),
                    ],
                    "Date limite de réservation": [datetime(2019, 11, 23), pandas.NaT],
                    "Date de début de l'évènement": [
                        datetime(2019, 11, 24),
                        pandas.NaT,
                    ],
                    "Stock disponible réel": [8, 12],
                    "Stock disponible brut de réservations": [10, 12],
                    "Nombre total de réservations": [2, 0],
                    "Nombre de réservations annulées": [0, 0],
                    "Nombre de réservations ayant un paiement": [2, 0],
                },
            )

            # When
            create_enriched_stock_view(ENGINE)

            # Then
            with ENGINE.connect() as connection:
                stocks_details = pandas.read_sql_table(
                    "enriched_stock_data", connection, index_col="stock_id"
                )
            pandas.testing.assert_frame_equal(stocks_details, expected_stocks_details)

    class CreateEnrichedUserViewTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        def test_should_create_enriched_user_data_view_with_columns(self):
            # Given
            expected_columns = [
                "Vague d'expérimentation",
                "Département",
                "Code postal",
                "Statut",
                "Date d'activation",
                "Date de première connexion",
                "Date de première réservation",
                "Date de deuxième réservation",
                "Date de première réservation dans 3 catégories différentes",
                "Nombre de réservations totales",
                "Nombre de réservations non annulées",
                "Ancienneté en jours",
                "Montant réél dépensé",
                "Montant théorique dépensé",
                "Dépenses numériques",
                "Dépenses physiques",
                "Dépenses sorties",
                "Date de dernière réservation",
                "user_humanized_id",
            ]

            # When
            create_enriched_user_data(ENGINE)

            # Then
            with ENGINE.connect() as connection:
                beneficiary_users_details = pandas.read_sql_table(
                    "enriched_user_data", connection, index_col="user_id"
                )
            assert sorted(expected_columns) == sorted(beneficiary_users_details.columns)

    class CreateEnrichedOffererViewTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        def test_should_create_enriched_offerer_data_view_with_columns(self):
            # When
            create_enriched_offerer_data(ENGINE)

            # Then
            expected_columns = [
                "Nom",
                "Date de création",
                "Date de création du premier stock",
                "Date de première réservation",
                "Nombre d’offres",
                "Nombre de réservations non annulées",
                "Département",
                "Nombre de lieux",
                "Nombre de lieux avec offres",
                "offerer_humanized_id",
                "Lien de validation de la structure",
                "Chiffre d'affaire réel année civile en cours",
            ]

            with ENGINE.connect() as connection:
                offerers_details = pandas.read_sql_table(
                    "enriched_offerer_data", connection, index_col="offerer_id"
                )
            assert sorted(expected_columns) == sorted(offerers_details.columns)

    class CreateEnrichedOfferViewTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        def test_should_create_enriched_offer_data_view_with_columns(self):
            # When
            create_enriched_offer_data(ENGINE)

            # Then
            expected_columns = [
                "Identifiant de la structure",
                "Nom de la structure",
                "Identifiant du lieu",
                "Nom du lieu",
                "Département du lieu",
                "Nom de l'offre",
                "Catégorie de l'offre",
                "Date de création de l'offre",
                "isDuo",
                "Offre numérique",
                "Bien physique",
                "Sortie",
                "Nombre de réservations",
                "Nombre de réservations annulées",
                "Nombre de réservations validées",
                "Nombre de fois où l'offre a été mise en favoris",
                "Stock",
                "offer_humanized_id",
                "Lien portail pro",
                "Lien WEBAPP",
            ]
            with ENGINE.connect() as connection:
                offers_details = pandas.read_sql_table(
                    "enriched_offer_data", connection, index_col="offer_id"
                )
            assert sorted(expected_columns) == sorted(offers_details.columns)

        def test_should_create_enriched_offer_data_view_without_duplicates(self):
            # Given
            create_user(id=1)
            create_user(id=2, email="other@test.com")
            create_user(id=3, email="louie.lopez@test.com")
            create_product(id=1, product_type="EventType.CINEMA")
            create_product(id=2, product_type="ThingType.LIVRE_EDITION")
            create_offerer(id=3)
            create_offerer(id=4, siren="234567890")
            create_venue(offerer_id=3, id=1, siret="12345678900026")
            create_venue(offerer_id=4, id=2, siret="23456789000067")
            create_offer(
                venue_id=1,
                product_id=1,
                id=3,
                product_type="EventType.CINEMA",
                name="Test",
            )
            create_offer(
                venue_id=2,
                product_id=2,
                id=4,
                product_type="ThingType.LIVRE_EDITION",
                name="RIP Dylan Rieder",
            )
            create_stock(
                offer_id=3,
                id=1,
                date_created="2019-11-01",
                quantity=10,
                booking_limit_datetime="2019-11-23",
                beginning_datetime="2019-11-24",
            )
            create_stock(offer_id=4, id=2, date_created="2019-10-01", quantity=12)
            create_booking(user_id=1, stock_id=1, id=4, quantity=2)
            create_payment(booking_id=4, id=1)
            create_payment_status(
                payment_id=1, id=1, date="2019-01-01", status="PENDING"
            )
            create_favorite(id=1, offer_id=3, user_id=1)
            create_favorite(id=2, offer_id=4, user_id=2)
            create_favorite(id=3, offer_id=3, user_id=3)

            expected_enriched_offer = pandas.DataFrame(
                index=pandas.Index(data=[3, 4], name="offer_id"),
                data={
                    "Identifiant de la structure": [3, 4],
                    "Nom de la structure": ["Test Offerer", "Test Offerer"],
                    "Identifiant du lieu": [1, 2],
                    "Nom du lieu": ["Test Venue", "Test Venue"],
                    "Département du lieu": ["93", "93"],
                    "Nom de l'offre": ["Test", "RIP Dylan Rieder"],
                    "Catégorie de l'offre": [
                        "EventType.CINEMA",
                        "ThingType.LIVRE_EDITION",
                    ],
                    "Date de création de l'offre": [
                        datetime(2019, 11, 20),
                        datetime(2019, 11, 20),
                    ],
                    "isDuo": [False, False],
                    "Offre numérique": [False, False],
                    "Bien physique": [False, True],
                    "Sortie": [True, False],
                    "Nombre de réservations": [2.0, 0.0],
                    "Nombre de réservations annulées": [0.0, 0.0],
                    "Nombre de réservations validées": [0.0, 0.0],
                    "Nombre de fois où l'offre a été mise en favoris": [2.0, 1.0],
                    "Stock": [10.0, 12.0],
                    "offer_humanized_id": ["AM", "AQ"],
                    "Lien portail pro": ["https://pro.passculture.beta.gouv.fr/offres/AM","https://pro.passculture.beta.gouv.fr/offres/AQ"],
                    "Lien WEBAPP" : ["https://app.passculture.beta.gouv.fr/offre/details/AM","https://app.passculture.beta.gouv.fr/offre/details/AQ"]
                },
            )

            # When
            create_enriched_offer_data(ENGINE)

            # Then
            with ENGINE.connect() as connection:
                offer_details = pandas.read_sql_table(
                    "enriched_offer_data", connection, index_col="offer_id"
                )
            pandas.testing.assert_frame_equal(offer_details, expected_enriched_offer)

    class CreateEnrichedVenueViewTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        def test_should_create_enriched_venue_data_view_with_columns(self, app):
            # When
            create_enriched_venue_data(ENGINE)

            # Then
            expected_columns = [
                "Nom du lieu",
                "email",
                "Adresse",
                "latitude",
                "longitude",
                "Département",
                "Code postal",
                "Ville",
                "siret",
                "Lieu numérique",
                "Identifiant de la structure",
                "Nom de la structure",
                "Type de lieu",
                "Label du lieu",
                "Nombre total de réservations",
                "Nombre de réservations non annulées",
                "Nombre de réservations validées",
                "Date de création de la première offre",
                "Date de création de la dernière offre",
                "Nombre d'offres créées",
                "Chiffre d'affaires théorique réalisé",
                "Chiffre d'affaires réel réalisé",
                "venue_humanized_id",
            ]

            with ENGINE.connect() as connection:
                venue_details = pandas.read_sql_table(
                    "enriched_venue_data", connection, index_col="venue_id"
                )
            assert sorted(expected_columns) == sorted(venue_details.columns)

    class CreateEnrichedBookingViewTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        def test_should_create_enriched_booking_data_view_with_columns(self):
            # Given
            expected_columns = [
                "Date de réservation",
                "quantity",
                "amount",
                "isCancelled",
                "isUsed",
                "Date d'annulation",
                "Date de début de l'événement",
                "Type d'offre",
                "Nom de l'offre",
                "Nom du lieu",
                "Label du lieu",
                "Type de lieu",
                "Département du lieu",
                "Nom de la structure",
                "Département de l'utilisateur",
                "Date de création de l'utilisateur",
                "Montant de la réservation",
                "Remboursé",
                "Réservation de bien physique",
                "Réservation de bien numérique",
                "Réservation de sortie",
                "Classement de la réservation",
                "Classement de la réservation dans la même catégorie"
            ]

            # When
            create_enriched_booking_data(ENGINE)

            # Then
            with ENGINE.connect() as connection:
                booking_details = pandas.read_sql_table(
                    "enriched_booking_data", connection, index_col="id"
                )
            assert sorted(expected_columns) == sorted(booking_details.columns)