def _get_booking_amount() -> str:
    return """
    SELECT
        booking.id AS booking_id
        ,coalesce(booking.amount,0)*coalesce(booking.quantity,0) AS "Montant de la réservation"
    FROM booking
    """


def _get_booking_payment_status() -> str:
    return """
    SELECT
        booking.id AS booking_id
        ,case when payment_status.status = 'SENT' and payment.author IS NOT NULL then true else false end as "Remboursé"
    FROM booking
    LEFT JOIN payment
    ON payment."bookingId" = booking.id
    AND payment.author IS NOT NULL
    LEFT JOIN payment_status
    ON payment.id = payment_status."paymentId"
    AND payment_status.status = 'SENT'
    """


def create_booking_amount_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW booking_amount_view  AS {_get_booking_amount()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)


def create_booking_payment_status_view(ENGINE) -> None:
    view_query = f"""
        CREATE OR REPLACE VIEW booking_payment_status_view  AS {_get_booking_payment_status()}
        """
    with ENGINE.connect() as connection:
        connection.execute(view_query)
