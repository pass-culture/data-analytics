from db import db
from stock_queries import STOCK_COLUMNS


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
         stock.available AS "{STOCK_COLUMNS["available"]}",
         stock_booking_information."{STOCK_COLUMNS["booking_quantity"]}",
         stock_booking_information."{STOCK_COLUMNS["bookings_cancelled"]}",
         stock_booking_information."{STOCK_COLUMNS["bookings_paid"]}" 
         FROM stock
         LEFT JOIN offer ON stock."offerId" = offer.id
         LEFT JOIN venue ON venue.id = offer."venueId"
         LEFT JOIN stock_booking_information ON stock.id = stock_booking_information.stock_id);
        '''
    db.session.execute(query)
    db.session.commit()


def create_enriched_user_view() -> None:
    query = '''
        CREATE OR REPLACE VIEW enriched_user_data AS
        (SELECT
         "user".id AS user_id,
         "user"."departementCode" AS "Département",
         "user"."culturalSurveyFilledDate" AS "Date de remplissage du typeform",
         experimentation_sessions."Vague d'expérimentation",
         activation_dates."Date d'activation",
         first_connection_dates."Date de première connexion",
         date_of_first_bookings."Date de première réservation",
         date_of_second_bookings."Date de deuxième réservation",
         date_of_bookings_on_third_product."Date de première réservation dans 3 catégories différentes",
         last_recommendation_dates."Date de dernière recommandation",
         number_of_bookings."Nombre de réservations totales",
         number_of_non_cancelled_bookings."Nombre de réservations non annulées",
         users_seniority."Ancienneté en jours",
         actual_amount_spent."Montant réél dépensé",
         theoric_amount_spent."Montant théorique dépensé",
         theoric_amount_spent_in_digital_goods."Dépenses numériques",
         theoric_amount_spent_in_physical_goods."Dépenses physiques"
        FROM "user"
        LEFT JOIN experimentation_sessions ON "user".id = experimentation_sessions."user_id"
        LEFT JOIN activation_dates ON experimentation_sessions.user_id = activation_dates.user_id
        LEFT JOIN first_connection_dates ON activation_dates.user_id = first_connection_dates.user_id
        LEFT JOIN date_of_first_bookings ON first_connection_dates.user_id = date_of_first_bookings.user_id
        LEFT JOIN date_of_second_bookings ON date_of_first_bookings.user_id = date_of_second_bookings.user_id
        LEFT JOIN date_of_bookings_on_third_product ON date_of_second_bookings.user_id = date_of_bookings_on_third_product.user_id
        LEFT JOIN last_recommendation_dates ON date_of_bookings_on_third_product.user_id = last_recommendation_dates.user_id
        LEFT JOIN number_of_bookings ON last_recommendation_dates.user_id = number_of_bookings.user_id
        LEFT JOIN number_of_non_cancelled_bookings ON number_of_bookings.user_id = number_of_non_cancelled_bookings.user_id
        LEFT JOIN users_seniority ON number_of_non_cancelled_bookings.user_id = users_seniority.user_id
        LEFT JOIN actual_amount_spent ON users_seniority.user_id = actual_amount_spent.user_id
        LEFT JOIN theoric_amount_spent ON actual_amount_spent.user_id = theoric_amount_spent.user_id
        LEFT JOIN theoric_amount_spent_in_digital_goods ON theoric_amount_spent.user_id = theoric_amount_spent_in_digital_goods.user_id
        LEFT JOIN theoric_amount_spent_in_physical_goods ON theoric_amount_spent_in_digital_goods.user_id = theoric_amount_spent_in_physical_goods.user_id
        WHERE "user"."canBookFreeOffers");
        '''
    db.session.execute(query)
    db.session.commit()
