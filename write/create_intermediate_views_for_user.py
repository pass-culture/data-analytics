from datetime import datetime

from db import ENGINE


def _get_experimentation_sessions_query() -> str:
    return """
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
    """


def _get_activation_dates_query() -> str:
    return """
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
    """


def _get_date_of_first_bookings_query() -> str:
    return """
     SELECT
        booking."userId" AS user_id
        ,min(booking."dateCreated") AS "Date de première réservation"
    FROM booking
    JOIN stock ON stock.id = booking."stockId"
    JOIN offer ON offer.id = stock."offerId"
    AND offer.type != 'ThingType.ACTIVATION'
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' OR offer."bookingEmail" IS NULL)
    GROUP BY user_id
    """


def _get_date_of_second_bookings_query() -> str:
    return """
     WITH ranked_booking_data AS (
         SELECT
            booking."userId" AS user_id
            ,booking."dateCreated" AS date_creation_booking
            ,rank() over (partition by booking."userId" order by booking."dateCreated" asc) AS rank_booking
           FROM booking
           JOIN stock ON stock.id = booking."stockId"
           JOIN offer ON offer.id = stock."offerId"
           WHERE offer.type != 'ThingType.ACTIVATION'
           AND (offer."bookingEmail" != 'jeux-concours@passculture.app' OR offer."bookingEmail" IS NULL)
    )
    SELECT
        user_id
        ,date_creation_booking AS "Date de deuxième réservation"
    FROM ranked_booking_data
    WHERE rank_booking = 2
    """


def _get_date_of_bookings_on_third_product_type_query() -> str:
    return """
    WITH dat AS (
    SELECT
        booking.*
        ,offer."type" AS offer_type
        ,offer."name" AS offer_name
        ,offer.id AS offer_id
        ,rank() over (partition by booking."userId",offer."type" order by booking."dateCreated") AS rank_booking_in_cat
    FROM booking
    JOIN stock
    ON booking."stockId" = stock.id
    JOIN offer
    ON offer.id = stock."offerId"
    WHERE offer."type" NOT IN ('ThingType.ACTIVATION','EventType.ACTIVATION')
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' OR offer."bookingEmail" IS NULL)
    ),

    ranked_data AS (

    SELECT
        *
        ,rank() over (partition by "userId" order by "dateCreated") AS rank_cat
    FROM dat
    WHERE rank_booking_in_cat = 1
    )

    SELECT
        "userId" AS user_id
        ,"dateCreated" AS "Date de première réservation dans 3 catégories différentes"
    FROM ranked_data
    WHERE rank_cat = 3
    """


def _get_number_of_bookings_query() -> str:
    return """
    SELECT
    booking."userId" AS user_id
    ,count(booking.id) AS number_of_bookings
    FROM booking
    JOIN stock ON stock.id = booking."stockId"
    JOIN offer ON offer.id = stock."offerId"
    AND offer.type != 'ThingType.ACTIVATION'
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' OR offer."bookingEmail" IS NULL)
    GROUP BY user_id
    ORDER BY number_of_bookings ASC
    """


def _get_number_of_non_cancelled_bookings_query() -> str:
    return """
    SELECT
        booking."userId" AS user_id
        ,count(booking.id) AS number_of_bookings
    FROM booking
    JOIN stock ON stock.id = booking."stockId"
    JOIN offer ON offer.id = stock."offerId"
    AND offer.type != 'ThingType.ACTIVATION'
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' OR offer."bookingEmail" IS NULL)
    AND NOT booking."isCancelled"
    GROUP BY user_id
    ORDER BY number_of_bookings ASC
    """


def _get_users_seniority_query() -> str:
    return f"""
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
    """


def _get_actual_amount_spent_query() -> str:
    return """
    (SELECT
     "user".id AS user_id,
     COALESCE(SUM(booking.amount * booking.quantity), 0) AS "Montant réél dépensé"
    FROM "user"
    LEFT JOIN booking ON "user".id = booking."userId" AND booking."isUsed" IS TRUE AND booking."isCancelled" IS FALSE
    WHERE "user"."canBookFreeOffers"
    GROUP BY "user".id)
    """


def _get_theoric_amount_spent_query() -> str:
    return """
    (SELECT
     "user".id AS user_id,
     COALESCE(SUM(booking.amount * booking.quantity), 0) AS "Montant théorique dépensé"
    FROM "user"
    LEFT JOIN booking ON "user".id = booking."userId" AND booking."isCancelled" IS FALSE
    WHERE "user"."canBookFreeOffers"
    GROUP BY "user".id)
    """


def _get_theoric_amount_spent_in_digital_goods_query() -> str:
    return """
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
    """


def _get_theoric_amount_spent_in_physical_goods_query() -> str:
    return """
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
    """


def _get_theoric_amount_spent_in_outings_query() -> str:
    return """
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
    """


def _get_last_booking_date_query() -> str:
    return """
     SELECT
        booking."userId" AS user_id
        ,max(booking."dateCreated") AS "Date de dernière réservation"
    FROM booking
    JOIN stock ON stock.id = booking."stockId"
    JOIN offer ON offer.id = stock."offerId"
    AND offer.type != 'ThingType.ACTIVATION'
    AND (offer."bookingEmail" != 'jeux-concours@passculture.app' OR offer."bookingEmail" IS NULL)
    GROUP BY user_id
    """


def create_experimentation_sessions_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW experimentation_sessions AS {_get_experimentation_sessions_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_activation_dates_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW activation_dates AS {_get_activation_dates_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_date_of_first_bookings_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW date_of_first_bookings AS {_get_date_of_first_bookings_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_date_of_second_bookings_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW date_of_second_bookings AS {_get_date_of_second_bookings_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_date_of_bookings_on_third_product_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW date_of_bookings_on_third_product AS {_get_date_of_bookings_on_third_product_type_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_number_of_bookings_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW number_of_bookings AS {_get_number_of_bookings_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_number_of_non_cancelled_bookings_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW number_of_non_cancelled_bookings AS {_get_number_of_non_cancelled_bookings_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_users_seniority_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW users_seniority AS {_get_users_seniority_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_actual_amount_spent_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW actual_amount_spent AS {_get_actual_amount_spent_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_theoric_amount_spent_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW theoric_amount_spent AS {_get_theoric_amount_spent_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_theoric_amount_spent_in_digital_goods_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW theoric_amount_spent_in_digital_goods AS {_get_theoric_amount_spent_in_digital_goods_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_theoric_amount_spent_in_physical_goods_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW theoric_amount_spent_in_physical_goods AS {_get_theoric_amount_spent_in_physical_goods_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_theoric_amount_spent_in_outings_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW theoric_amount_spent_in_outings AS {_get_theoric_amount_spent_in_outings_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_date_of_last_booking_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW last_booking_date AS {_get_last_booking_date_query()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_materialized_enriched_user_view(ENGINE) -> None:
    query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_user_data AS
        (SELECT
         "user".id AS user_id,
         experimentation_sessions."Vague d'expérimentation",
         "user"."departementCode" AS "Département",
         "user"."postalCode" AS "Code postal",
         "user".activity AS "Statut",
         activation_dates."Date d'activation",
         CASE WHEN "user"."hasSeenTutorials" THEN "user"."culturalSurveyFilledDate" ELSE NULL END AS "Date de première connexion",
         date_of_first_bookings."Date de première réservation",
         date_of_second_bookings."Date de deuxième réservation",
         date_of_bookings_on_third_product."Date de première réservation dans 3 catégories différentes",
         COALESCE(number_of_bookings.number_of_bookings,0) AS "Nombre de réservations totales",
         COALESCE(number_of_non_cancelled_bookings.number_of_bookings,0) AS "Nombre de réservations non annulées",
         users_seniority."Ancienneté en jours",
         actual_amount_spent."Montant réél dépensé",
         theoric_amount_spent."Montant théorique dépensé",
         theoric_amount_spent_in_digital_goods."Dépenses numériques",
         theoric_amount_spent_in_physical_goods."Dépenses physiques",
         theoric_amount_spent_in_outings."Dépenses sorties",
         user_humanized_id.humanized_id AS "user_humanized_id",
         last_booking_date."Date de dernière réservation"
        FROM "user"
        LEFT JOIN experimentation_sessions ON "user".id = experimentation_sessions."user_id"
        LEFT JOIN activation_dates ON "user".id  = activation_dates.user_id
        LEFT JOIN date_of_first_bookings ON "user".id  = date_of_first_bookings.user_id
        LEFT JOIN date_of_second_bookings ON "user".id  = date_of_second_bookings.user_id
        LEFT JOIN date_of_bookings_on_third_product ON "user".id = date_of_bookings_on_third_product.user_id
        LEFT JOIN number_of_bookings ON "user".id = number_of_bookings.user_id
        LEFT JOIN number_of_non_cancelled_bookings ON "user".id = number_of_non_cancelled_bookings.user_id
        LEFT JOIN users_seniority ON "user".id = users_seniority.user_id
        LEFT JOIN actual_amount_spent ON "user".id = actual_amount_spent.user_id
        LEFT JOIN theoric_amount_spent ON "user".id = theoric_amount_spent.user_id
        LEFT JOIN theoric_amount_spent_in_digital_goods ON "user".id  = theoric_amount_spent_in_digital_goods.user_id
        LEFT JOIN theoric_amount_spent_in_physical_goods ON "user".id = theoric_amount_spent_in_physical_goods.user_id
        LEFT JOIN theoric_amount_spent_in_outings ON "user".id = theoric_amount_spent_in_outings.user_id
        LEFT JOIN last_booking_date ON last_booking_date.user_id = "user".id
        LEFT JOIN user_humanized_id ON user_humanized_id.id = "user".id
        WHERE "user"."canBookFreeOffers");
        """
    with ENGINE.connect() as connection:
        connection.execute(query)
