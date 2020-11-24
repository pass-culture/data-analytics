def get_booking_amount() -> str:
    return """
    SELECT
        booking.id AS booking_id,
        coalesce(booking.amount, 0) * coalesce(booking.quantity, 0) AS "Montant de la réservation"
    FROM booking;
    """


def get_booking_payment_status() -> str:
    return """
    SELECT
        booking.id AS booking_id,
        'Remboursé' AS "Remboursé"
    FROM booking
    INNER JOIN payment
    ON payment."bookingId" = booking.id
        AND payment.author IS NOT NULL
    INNER JOIN payment_status
    ON payment.id = payment_status."paymentId"
        AND payment_status.status = 'SENT';
    """


def get_booking_ranking() -> str:
    return """
    SELECT
        booking.id AS booking_id,
        rank() OVER (PARTITION BY booking."userId" ORDER BY booking."dateCreated") AS "Classement de la réservation"
    FROM booking;
    """


def get_booking_ranking_in_category() -> str:
    return """
    SELECT
        booking.id AS booking_id,
        rank() OVER (PARTITION BY booking."userId", offer."type" ORDER BY booking."dateCreated") AS "Classement de la réservation dans la même catégorie"
    FROM booking
    INNER JOIN stock ON booking."stockId" = stock.id
    INNER JOIN offer ON stock."offerId" = offer.id
    ORDER BY booking.id;
    """


def create_booking_amount_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW booking_amount_view AS {get_booking_amount()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_booking_payment_status_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW booking_payment_status_view  AS {get_booking_payment_status()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_booking_ranking_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW booking_ranking_view AS {get_booking_ranking()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_booking_ranking_in_category_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW booking_ranking_in_category_view  AS { get_booking_ranking_in_category()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_materialized_booking_intermediary_view(ENGINE) -> None:
    query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS booking_intermediary_view AS (
               SELECT booking.id,
                      booking_amount_view."Montant de la réservation",
                      booking_payment_status_view."Remboursé",
                      booking_ranking_view."Classement de la réservation",
                      booking_ranking_in_category_view."Classement de la réservation dans la même catégorie"
                 FROM booking
            LEFT JOIN booking_amount_view ON booking_amount_view.booking_id = booking.id
            LEFT JOIN booking_payment_status_view ON booking_payment_status_view.booking_id = booking.id
            LEFT JOIN booking_ranking_view ON booking_ranking_view.booking_id = booking.id
            LEFT JOIN booking_ranking_in_category_view ON booking_ranking_in_category_view.booking_id = booking.id
        );
    """
    with ENGINE.connect() as connection:
        connection.execute(query)


def create_materialized_enriched_booking_view(ENGINE) -> None:
    query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_booking_data AS (
            SELECT
                booking.id,
                booking."dateCreated" AS "Date de réservation",
                booking."quantity",
                booking."amount",
                booking."isCancelled",
                booking."isUsed",
                booking."cancellationDate" AS "Date d'annulation",
                stock."beginningDatetime" AS "Date de début de l'événement",
                offer."type" AS "Type d'offre",
                offer."name" AS "Nom de l'offre",
                coalesce(venue."publicName", venue."name") AS "Nom du lieu",
                venue_label."label" AS "Label du lieu",
                venue_type."label" AS "Type de lieu",
                venue."departementCode" AS "Département du lieu",
                offerer."name" AS "Nom de la structure",
                "user"."departementCode" AS "Département de l'utilisateur",
                "user"."dateCreated" AS "Date de création de l'utilisateur",
                booking_intermediary_view."Montant de la réservation",
                CASE WHEN booking_intermediary_view."Remboursé" = 'Remboursé'
                    THEN True ELSE False END AS "Remboursé",
                CASE WHEN
                    offer.type IN ('ThingType.INSTRUMENT','ThingType.JEUX','ThingType.LIVRE_EDITION','ThingType.MUSIQUE','ThingType.OEUVRE_ART','ThingType.AUDIOVISUEL')
                    AND venue."name" <> 'Offre numérique'
                        THEN true else false end as "Réservation de bien physique",
                CASE WHEN venue."name" = 'Offre numérique'
                    THEN true else false end as "Réservation de bien numérique",
                CASE WHEN
                    offer.type NOT IN ('ThingType.INSTRUMENT','ThingType.JEUX','ThingType.LIVRE_EDITION','ThingType.MUSIQUE','ThingType.OEUVRE_ART','ThingType.AUDIOVISUEL')
                    AND venue."name" <> 'Offre numérique'
                        THEN true else false end as "Réservation de sortie",
                booking_intermediary_view."Classement de la réservation",
                booking_intermediary_view."Classement de la réservation dans la même catégorie"
            FROM booking
            INNER JOIN stock
                ON booking."stockId" = stock.id
            INNER JOIN offer
                ON offer.id = stock."offerId"
                AND offer."type" NOT IN ('ThingType.ACTIVATION','EventType.ACTIVATION')
            INNER JOIN venue
                ON venue.id = offer."venueId"
            INNER JOIN offerer
                ON venue."managingOffererId" = offerer.id
            INNER JOIN "user"
                ON "user".id = booking."userId"
            LEFT JOIN venue_type
                ON venue."venueTypeId" = venue_type.id
            LEFT JOIN venue_label
                ON venue."venueLabelId" = venue_label.id
            LEFT JOIN booking_intermediary_view ON booking_intermediary_view.id = booking.id
        );
        """
    with ENGINE.connect() as connection:
        connection.execute(query)
