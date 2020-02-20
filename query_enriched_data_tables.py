from sqlalchemy.engine import Connection

from db import SESSION
from offerer_queries import get_offerers_details
from user_queries import get_beneficiary_users_details


def create_enriched_offerer_data(connection: Connection):
    enriched_offerer_data = get_offerers_details(connection)
    enriched_offerer_data.to_sql(name='enriched_offerer_data',
                                 con=connection,
                                 if_exists='replace')


def create_enriched_user_data(connection: Connection):
    get_beneficiary_users_details(connection)\
        .sample(frac=1)\
        .reset_index(drop=True)\
        .to_sql(name='enriched_user_data',
                con=connection,
                if_exists='replace')


def create_enriched_stock_data():
    query = f'''
        CREATE OR REPLACE VIEW enriched_stock_data_v2 AS
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
