from datetime import datetime

from db import ENGINE


def _get_experimentation_sessions_query() -> str:
    return '''
    (WITH experimentation_session AS 
    (
        SELECT
        DISTINCT ON (user_id)
         "isUsed" AS is_used,
         "userId" AS user_id
        FROM booking
        JOIN stock ON stock.id = booking."stockId"
        JOIN offer
         ON offer.id = stock."offerId"
         AND offer.type = 'ThingType.ACTIVATION'
        ORDER BY user_id, is_used DESC 
    )

    SELECT
     CASE
      WHEN experimentation_session.is_used THEN 1
      ELSE 2
     END AS "Vague d'expérimentation",
     "user".id AS user_id
    FROM "user"
    LEFT JOIN experimentation_session ON experimentation_session.user_id = "user".id
    WHERE "user"."canBookFreeOffers")
    '''


def _get_activation_dates_query() -> str:
    return '''
    (WITH validated_activation_booking AS (
     SELECT booking."dateUsed" AS date_used, booking."userId", booking."isUsed" AS is_used
     FROM booking
     JOIN stock
      ON stock.id = booking."stockId"
     JOIN offer
      ON stock."offerId" = offer.id
      AND offer.type = 'ThingType.ACTIVATION'
     WHERE booking."isUsed"
    )

    SELECT
     CASE
      WHEN validated_activation_booking.is_used THEN validated_activation_booking.date_used
      ELSE "user"."dateCreated"
     END AS "Date d'activation",
     "user".id as user_id
    FROM "user"
    LEFT JOIN validated_activation_booking
     ON validated_activation_booking."userId" = "user".id
    WHERE "user"."canBookFreeOffers")
    '''


def _get_first_connection_dates_query() -> str:
    return '''
    (WITH recommendation_dates AS (
     SELECT
      MIN(recommendation."dateCreated") AS first_recommendation_date,
      MAX(recommendation."dateCreated") AS last_recommendation_date,
      "user".id AS user_id,
      "user"."canBookFreeOffers"
     FROM "user"
     LEFT JOIN recommendation ON recommendation."userId" = "user".id
     GROUP BY "user".id, "user"."canBookFreeOffers"
    )
    
    SELECT
     recommendation_dates.first_recommendation_date AS "Date de première connexion",
     recommendation_dates.user_id AS user_id
    FROM recommendation_dates
    WHERE recommendation_dates."canBookFreeOffers")
    '''


def _get_date_of_first_bookings_query() -> str:
    return '''
     SELECT
        booking."userId" AS user_id
        ,min(booking."dateCreated") AS "Date de première réservation"
    FROM booking
    JOIN stock ON stock.id = booking."stockId"
    JOIN offer ON offer.id = stock."offerId"
    WHERE offer.type != 'ThingType.ACTIVATION'
    AND offer."bookingEmail" != 'jeux-concours@passculture.app'
    GROUP BY user_id
    '''


def _get_date_of_second_bookings_query() -> str:
    return '''
     WITH ranked_booking_data AS (
         SELECT
            booking."userId" AS user_id
            ,booking."dateCreated" AS date_creation_booking
            ,rank() over (partition by booking."userId" order by booking."dateCreated" asc) AS rank_booking
           FROM booking
           JOIN stock ON stock.id = booking."stockId"
           JOIN offer ON offer.id = stock."offerId"
           WHERE offer.type != 'ThingType.ACTIVATION'
           AND offer."bookingEmail" != 'jeux-concours@passculture.app'
    )
    
    SELECT
        user_id
        ,date_creation_booking AS "Date de deuxième réservation"
    FROM ranked_booking_data
    WHERE rank_booking = 2
    '''


def _get_date_of_bookings_on_third_product_type_query() -> str:
    return '''
    WITH
     bookings_on_distinct_types AS (
      SELECT DISTINCT ON (offer.type, booking."userId") offer.type, booking."userId", booking."dateCreated"
      FROM booking
      JOIN stock ON stock.id = booking."stockId"
      JOIN offer ON offer.id = stock."offerId"
       AND offer.type != 'ThingType.ACTIVATION'
      ORDER BY offer.type, booking."userId", booking."dateCreated" ASC
     ),
     first_booking_on_third_category AS (
      SELECT
       ordered_dates."dateCreated" AS date,
       ordered_dates."userId"
      FROM (
       SELECT
        ROW_NUMBER()
         OVER(
          PARTITION BY "userId"
          ORDER BY bookings_on_distinct_types."dateCreated" ASC
         ) AS rank, bookings_on_distinct_types."dateCreated",
        bookings_on_distinct_types."userId"
       FROM bookings_on_distinct_types
          ) AS ordered_dates
      WHERE ordered_dates.rank = 3
    )

      SELECT
       first_booking_on_third_category.date AS "Date de première réservation dans 3 catégories différentes",
       "user".id as user_id
      FROM "user"
      LEFT JOIN first_booking_on_third_category ON first_booking_on_third_category."userId" = "user".id
      WHERE "user"."canBookFreeOffers"
    '''


def _get_last_recommendation_dates_query() -> str:
    return '''
    (WITH recommendation_dates AS (
     SELECT
      MIN(recommendation."dateCreated") AS first_recommendation_date,
      MAX(recommendation."dateCreated") AS last_recommendation_date,
      "user".id AS user_id,
      "user"."canBookFreeOffers"
     FROM "user"
     LEFT JOIN recommendation ON recommendation."userId" = "user".id
     GROUP BY "user".id, "user"."canBookFreeOffers"
    )
    
    SELECT
     recommendation_dates.last_recommendation_date AS "Date de dernière recommandation",
     recommendation_dates.user_id AS user_id
    FROM recommendation_dates
    WHERE recommendation_dates."canBookFreeOffers")
    '''


def _get_number_of_bookings_query() -> str:
    return '''
     SELECT
        booking."userId" AS user_id
        ,count(booking.id) AS number_of_bookings
    FROM booking
    JOIN stock ON stock.id = booking."stockId"
    JOIN offer ON offer.id = stock."offerId"
    AND offer.type != 'ThingType.ACTIVATION'
    AND offer."bookingEmail" != 'jeux-concours@passculture.app'
    GROUP BY user_id
    ORDER BY number_of_bookings ASC
    '''


def _get_number_of_non_cancelled_bookings_query() -> str:
    return '''
    SELECT
    booking."userId" AS user_id
    ,count(booking.id) AS number_of_bookings
    FROM booking
    JOIN stock ON stock.id = booking."stockId"
    JOIN offer ON offer.id = stock."offerId"
    AND offer.type != 'ThingType.ACTIVATION'
    AND offer."bookingEmail" != 'jeux-concours@passculture.app'
    AND NOT booking."isCancelled"
    GROUP BY user_id
    ORDER BY number_of_bookings ASC
    '''


def _get_users_seniority_query() -> str:
    return f'''
    (WITH validated_activation_booking AS 
    ( SELECT booking."dateUsed" AS date_used, booking."userId", booking."isUsed" AS is_used
     FROM booking
     JOIN stock
      ON stock.id = booking."stockId"
     JOIN offer
      ON stock."offerId" = offer.id
      AND offer.type = 'ThingType.ACTIVATION'
     WHERE booking."isUsed" 
    ),
    
    activation_date AS
    ( SELECT
     CASE
      WHEN validated_activation_booking.is_used THEN validated_activation_booking.date_used
      ELSE "user"."dateCreated"
     END AS "Date d'activation",
     "user".id as user_id
    FROM "user"
    LEFT JOIN validated_activation_booking
     ON validated_activation_booking."userId" = "user".id
    WHERE "user"."canBookFreeOffers")
    
    SELECT 
     DATE_PART('day', '{datetime.now()}' - activation_date."Date d'activation") AS "Ancienneté en jours",
     "user".id as user_id
    FROM "user"
    LEFT JOIN activation_date ON "user".id = activation_date.user_id
    )
    '''


def _get_actual_amount_spent_query() -> str:
    return '''
    (SELECT 
     "user".id AS user_id, 
     COALESCE(SUM(booking.amount * booking.quantity), 0) AS "Montant réél dépensé"
    FROM "user"
    LEFT JOIN booking ON "user".id = booking."userId" AND booking."isUsed" IS TRUE AND booking."isCancelled" IS FALSE
    WHERE "user"."canBookFreeOffers"
    GROUP BY "user".id)
    '''


def _get_theoric_amount_spent_query() -> str:
    return '''
    (SELECT 
     "user".id AS user_id, 
     COALESCE(SUM(booking.amount * booking.quantity), 0) AS "Montant théorique dépensé"
    FROM "user"
    LEFT JOIN booking ON "user".id = booking."userId" AND booking."isCancelled" IS FALSE
    WHERE "user"."canBookFreeOffers"
    GROUP BY "user".id)
    '''


def _get_theoric_amount_spent_in_digital_goods_query() -> str:
    return '''
    (WITH eligible_booking AS (
     SELECT booking."userId", booking.amount, booking.quantity
     FROM booking
     LEFT JOIN stock ON booking."stockId" = stock.id
     LEFT JOIN offer ON stock."offerId" = offer.id
     WHERE offer.type IN ('ThingType.AUDIOVISUEL', 'ThingType.JEUX_VIDEO', 'ThingType.JEUX_VIDEO_ABO', 
     'ThingType.LIVRE_AUDIO', 'ThingType.LIVRE_EDITION', 'ThingType.MUSIQUE', 'ThingType.PRESSE_ABO')
     AND offer.url IS NOT NULL
     AND booking."isCancelled" IS FALSE
     )

    SELECT 
    "user".id AS user_id, 
    COALESCE(SUM(eligible_booking.amount * eligible_booking.quantity),0.) AS "Dépenses numériques"
    FROM "user"
    LEFT JOIN eligible_booking ON "user".id = eligible_booking."userId"
    WHERE "user"."canBookFreeOffers" IS TRUE
    GROUP BY "user".id)
    '''


def _get_theoric_amount_spent_in_physical_goods_query() -> str:
    return '''
    (WITH eligible_booking AS (
     SELECT booking."userId", booking.amount, booking.quantity
     FROM booking
     LEFT JOIN stock ON booking."stockId" = stock.id
     LEFT JOIN offer ON stock."offerId" = offer.id
     WHERE offer.type IN ('ThingType.INSTRUMENT','ThingType.JEUX','ThingType.LIVRE_EDITION','ThingType.MUSIQUE','ThingType.OEUVRE_ART','ThingType.AUDIOVISUEL')
     AND offer.url IS NULL
     AND booking."isCancelled" IS FALSE
    )

    SELECT "user".id AS user_id, COALESCE(SUM(eligible_booking.amount * eligible_booking.quantity),0.) AS "Dépenses physiques"
    FROM "user"
    LEFT JOIN eligible_booking ON "user".id = eligible_booking."userId"
    WHERE "user"."canBookFreeOffers" IS TRUE
    GROUP BY "user".id)
    '''


def _get_theoric_amount_spent_in_outings_query() -> str:
    return '''
    (WITH eligible_booking AS (
     SELECT booking."userId", booking.amount, booking.quantity
     FROM booking
     LEFT JOIN stock ON booking."stockId" = stock.id
     LEFT JOIN offer ON stock."offerId" = offer.id
     WHERE offer.type IN ('EventType.SPECTACLE_VIVANT'
                             ,'EventType.CINEMA'
                             ,'EventType.JEUX'
                             ,'ThingType.SPECTACLE_VIVANT_ABO'
                             ,'EventType.MUSIQUE'
                             ,'ThingType.MUSEES_PATRIMOINE_ABO'
                             ,'ThingType.CINEMA_CARD'
                             ,'ThingType.PRATIQUE_ARTISTIQUE_ABO'
                             ,'ThingType.CINEMA_ABO'
                             ,'EventType.MUSEES_PATRIMOINE'
                             ,'EventType.PRATIQUE_ARTISTIQUE'
                             ,'EventType.CONFERENCE_DEBAT_DEDICACE')
     AND booking."isCancelled" IS FALSE
    )
    
    SELECT "user".id AS user_id, COALESCE(SUM(eligible_booking.amount * eligible_booking.quantity),0.) AS "Dépenses sorties"
    FROM "user"
    LEFT JOIN eligible_booking ON "user".id = eligible_booking."userId"
    WHERE "user"."canBookFreeOffers" IS TRUE
    GROUP BY "user".id)
    '''


def create_experimentation_sessions_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW experimentation_sessions AS {_get_experimentation_sessions_query()}
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_activation_dates_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW activation_dates AS {_get_activation_dates_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_first_connection_dates_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW first_connection_dates AS {_get_first_connection_dates_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_date_of_first_bookings_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW date_of_first_bookings AS {_get_date_of_first_bookings_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_date_of_second_bookings_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW date_of_second_bookings AS {_get_date_of_second_bookings_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_date_of_bookings_on_third_product_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW date_of_bookings_on_third_product AS {_get_date_of_bookings_on_third_product_type_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_last_recommendation_dates_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW last_recommendation_dates AS {_get_last_recommendation_dates_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_number_of_bookings_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW number_of_bookings AS {_get_number_of_bookings_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_number_of_non_cancelled_bookings_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW number_of_non_cancelled_bookings AS {_get_number_of_non_cancelled_bookings_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_users_seniority_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW users_seniority AS {_get_users_seniority_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_actual_amount_spent_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW actual_amount_spent AS {_get_actual_amount_spent_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_theoric_amount_spent_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW theoric_amount_spent AS {_get_theoric_amount_spent_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_theoric_amount_spent_in_digital_goods_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW theoric_amount_spent_in_digital_goods AS {_get_theoric_amount_spent_in_digital_goods_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_theoric_amount_spent_in_physical_goods_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW theoric_amount_spent_in_physical_goods AS {_get_theoric_amount_spent_in_physical_goods_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_theoric_amount_spent_in_outings_view(ENGINE) -> None:
    view_query = f'''
        CREATE OR REPLACE VIEW theoric_amount_spent_in_outings AS {_get_theoric_amount_spent_in_outings_query()} 
        '''
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_materialized_enriched_user_view(ENGINE) -> None:
    query = '''
        CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_user_data AS
        (SELECT
         "user".id AS user_id,
         experimentation_sessions."Vague d'expérimentation",
         "user"."departementCode" AS "Département",
         "user"."postalCode" AS "Code postal",
         "user".activity AS "Statut",
         activation_dates."Date d'activation",
         "user"."culturalSurveyFilledDate" AS "Date de remplissage du typeform",
         first_connection_dates."Date de première connexion",
         date_of_first_bookings."Date de première réservation",
         date_of_second_bookings."Date de deuxième réservation",
         date_of_bookings_on_third_product."Date de première réservation dans 3 catégories différentes",
         last_recommendation_dates."Date de dernière recommandation",
         COALESCE(number_of_bookings.number_of_bookings,0) AS "Nombre de réservations totales",
         COALESCE(number_of_non_cancelled_bookings.number_of_bookings,0) AS "Nombre de réservations non annulées",
         users_seniority."Ancienneté en jours",
         actual_amount_spent."Montant réél dépensé",
         theoric_amount_spent."Montant théorique dépensé",
         theoric_amount_spent_in_digital_goods."Dépenses numériques",
         theoric_amount_spent_in_physical_goods."Dépenses physiques",
         theoric_amount_spent_in_outings."Dépenses sorties"
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
        LEFT JOIN theoric_amount_spent_in_outings ON theoric_amount_spent_in_physical_goods.user_id = theoric_amount_spent_in_outings.user_id
        WHERE "user"."canBookFreeOffers");
        '''
    with ENGINE.connect() as connection:
        connection.execute(query)
