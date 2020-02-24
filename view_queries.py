from db import SESSION


def create_enriched_stock_view() -> None:
    query = f'''
        CREATE OR REPLACE VIEW enriched_stock_data AS
        (SELECT
         stock_information.stock_id, "Identifiant de l'offre", "Nom de l'offre", 
         "offerer_id", "Type d'offre", "Département", "Date de création du stock", "Date limite de réservation", 
         "Date de début de l'évènement", "Stock disponible brut de réservations", "Nombre total de réservations", "Nombre de réservations annulées", 
         "Nombre de réservations ayant un paiement" 
         FROM stock_information
         LEFT JOIN stock_offer_information ON stock_information.stock_id = stock_offer_information.stock_id
         LEFT JOIN stock_venue_information ON stock_offer_information.stock_id = stock_venue_information.stock_id
         LEFT JOIN stock_booking_information ON stock_venue_information.stock_id = stock_booking_information.stock_id);
        '''
    SESSION.execute(query)
    SESSION.commit()
