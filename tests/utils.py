from db import ENGINE


def create_user(id=1, email='test@email.com', can_book_free_offers=True, is_admin=False, postal_code='93100',
                departement_code='93', public_name='Test', date_created='2018-11-20',
                needs_to_fill_cultural_survey=True):
    password = '\x2432622431322445425a572e6d484754383242375558366e31576f43655044647671467875634b4173327873' \
               '666c6b69675061793347397968527561'
    ENGINE.execute(f'''
    INSERT INTO "user" (id, email, "publicName", "canBookFreeOffers", "isAdmin", password, "postalCode", 
    "departementCode", "dateCreated", "needsToFillCulturalSurvey")
    VALUES ({id}, '{email}', '{public_name}', {can_book_free_offers}, {is_admin}, '{password}', '{postal_code}', 
    '{departement_code}', '{date_created}', {needs_to_fill_cultural_survey})
    ''')


def create_offerer(id=1, thumb_count=0, is_active=True, postal_code='93100', city='Montreuil',
                   date_created='2019-11-20',
                   name='Test Offerer', siren='123456789'):
    ENGINE.execute(f'''
    INSERT INTO offerer (id, "thumbCount", "isActive", "postalCode", city, "dateCreated", name, siren)
    VALUES ({id}, {thumb_count}, {is_active}, '{postal_code}', '{city}', '{date_created}', '{name}', '{siren}')
    ''')


def create_venue(offerer_id, id=1, thumb_count=0, name='Test Venue', siret='12345678912345', postal_code='93',
                 city='Montreuil',
                 departement_code='93', is_virtual=False):
    ENGINE.execute(f'''
    INSERT INTO venue (id, "thumbCount", "name", "siret", "postalCode", city, "departementCode", "managingOffererId", "isVirtual")
    VALUES ({id}, {thumb_count}, '{name}', '{siret}', '{postal_code}', '{city}', '{departement_code}', {offerer_id}, {is_virtual})
    ''')


def create_product(id=1, thumb_count=0, product_type='ThingType.LIVRE_EDITION', name='Livre',
                   media_urls=['https://url.test', 'https://someurl.test'],
                   url=None, is_national=False):
    ENGINE.execute(f'''
            INSERT INTO product (id, "thumbCount", type, name, "mediaUrls", url, "isNational")
            VALUES ({id}, {thumb_count}, '{product_type}', '{name}', ARRAY{media_urls}, '{url}', {is_national})
            ''')


def create_offer(venue_id, product_id, id=1, is_active=True, product_type='ThingType.LIVRE_EDITION',
                 name='Livre',
                 media_urls=['https://url.test', 'https://someurl.test'],
                 url=None, is_national=False, date_created='2019-11-20', is_duo=False):
    ENGINE.execute(f'''
                INSERT INTO offer (id, "venueId", "productId", "isActive", type, name, "mediaUrls", url, "isNational", "dateCreated", "isDuo")
                VALUES ({id}, {venue_id}, {product_id}, {is_active}, '{product_type}', '{name}', ARRAY{media_urls}, '{url}', {is_national}, '{date_created}', {is_duo})
                ''')


def create_stock(offer_id, id=1, is_soft_deleted=False, date_modified='2019-11-20', price=0, available=10,
                 groupe_size=1):
    ENGINE.execute(f'''
        INSERT INTO stock (id, "isSoftDeleted", "dateModified", "offerId", price, available, "groupSize")
        VALUES ({id}, {is_soft_deleted}, '{date_modified}', {offer_id}, {price}, {available}, {groupe_size})
        ''')


def create_booking(user_id, stock_id, id=1, date_created='2019-11-20', quantity=1, token='ABC123', amount=0,
                   is_cancelled=False, is_used=False):
    ENGINE.execute(f'''
            INSERT INTO booking (id, "dateCreated", "stockId", quantity, token, "userId", amount, "isCancelled", "isUsed")
            VALUES ({id}, '{date_created}', {stock_id}, {quantity}, '{token}', {user_id}, {amount}, {is_cancelled}, {is_used})
            ''')


def create_mediation(offer_id, id=1, thumb_count=0, is_active=True, date_created='2019-11-20'):
    ENGINE.execute(f'''
                INSERT INTO mediation (id, "thumbCount", "offerId", "isActive", "dateCreated")
                VALUES ({id}, {thumb_count}, {offer_id}, {is_active}, '{date_created}')
                ''')


def create_recommendation(offer_id, user_id, mediation_id='NULL', id=1, date_created='2019-11-20',
                          date_updated='2019-11-20', is_clicked=False, is_first=False):
    ENGINE.execute(f'''
                INSERT INTO recommendation (id, "offerId", "userId", "mediationId", "dateCreated", "dateUpdated", "isClicked", "isFirst")
                VALUES ({id}, {offer_id}, {user_id}, {mediation_id}, '{date_created}', '{date_updated}', {is_clicked}, {is_first})
                ''')


def update_table_column(id, table_name, column, value):
    ENGINE.execute(f'''
            UPDATE {table_name} 
            SET {column} = ({value})
            WHERE id={id}
            ''')
