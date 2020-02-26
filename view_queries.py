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
