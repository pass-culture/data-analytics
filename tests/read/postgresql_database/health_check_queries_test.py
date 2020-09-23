from typing import Tuple
from unittest.mock import patch, MagicMock

import pytest
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker, session

from db import db, ENGINE
from read.postgresql_database.health_check_queries import (
    is_enriched_materialized_view_queryable,
    does_enriched_offerer_contain_data,
    does_enriched_stock_contain_data,
    does_enriched_users_contains_data,
    does_enriched_offer_contain_data,
    does_materialize_view_exist,
    does_view_exist,
    does_view_have_data,
    is_enriched_view_queryable,
)
from utils.database_cleaners import (
    clean_database,
    clean_views,
    drop_offerer_cultural_activity_table,
)
from tests.data_creators import (
    create_offerer,
    create_venue,
    create_product,
    create_offer,
    create_stock,
)
from write.create_views import create_enriched_offerer_data, create_enriched_stock_data


def _get_mocked_session() -> Tuple[sessionmaker, session.Session]:
    Session = MagicMock()
    local_session = MagicMock()
    Session.return_value = local_session
    return Session, local_session


class DoesMaterializeViewExistTest:
    def setup_method(self):
        clean_database()
        clean_views()
        drop_offerer_cultural_activity_table()

    def test_should_return_false_if_materialized_view_does_not_exist(self, app):
        with app.app_context():
            result = does_materialize_view_exist(db.session, "enriched_offerer_data")

            # Then
            assert result is False

    def test_should_return_true_if_materialized_view_exists(self, app):
        # Given

        with app.app_context():
            create_enriched_offerer_data(db.engine)
            # When
            result = does_materialize_view_exist(db.session, "enriched_offerer_data")

            # Then
            assert result is True


class DoesViewExistTest:
    def teardown_method(self):
        clean_database()
        clean_views()
        drop_offerer_cultural_activity_table()

    def test_should_return_false_if_view_does_not_exist(self, app):
        with app.app_context():
            result = does_view_exist(db.session, "enriched_stock_data")

            # Then
            assert result is False

    def test_should_return_true_if_view_exists(self, app):
        # Given

        with app.app_context():
            create_enriched_stock_data(db.engine)
            # When
            result = does_view_exist(db.session, "enriched_stock_data")

            # Then
            assert result is True


class IsEnrichedMaterializedViewQueryableTest:
    def teardown_method(self):
        clean_database()
        clean_views()
        drop_offerer_cultural_activity_table()

    @patch("read.postgresql_database.health_check_queries.does_materialize_view_exist")
    def test_should_close_session_if_query_did_not_end_on_an_exception(
        self, does_materialize_view_exist_mock
    ):
        # Given
        does_materialize_view_exist_mock.return_value = True
        Session, local_session = _get_mocked_session()

        # When
        result = is_enriched_materialized_view_queryable(
            Session, "enriched_offerer_data"
        )

        # Then
        assert result is True
        local_session.close.assert_called_once()

    @patch("read.postgresql_database.health_check_queries.does_materialize_view_exist")
    def test_should_look_for_given_materialized_view_in_database(
        self, does_materialize_view_exist_mock
    ):
        # Given
        does_materialize_view_exist_mock.return_value = True
        Session, local_session = _get_mocked_session()

        # When
        result = is_enriched_materialized_view_queryable(Session, "enriched_user_data")

        # Then
        does_materialize_view_exist_mock.assert_called_once_with(
            local_session, "enriched_user_data"
        )

    @patch("read.postgresql_database.health_check_queries.does_materialize_view_exist")
    @patch("read.postgresql_database.health_check_queries.logger")
    def test_should_log_error_and_close_session_when_operational_error_is_raised(
        self, logger_mock, does_materialize_view_exist_mock
    ):
        # Given
        does_materialize_view_exist_mock.side_effect = OperationalError("", "", "")
        Session, local_session = _get_mocked_session()

        # When
        result = is_enriched_materialized_view_queryable(
            Session, "enriched_offerer_data"
        )

        # Then
        assert result is False
        local_session.close.assert_called_once()
        logger_mock.error.assert_called_once()

    @patch("read.postgresql_database.health_check_queries.does_materialize_view_exist")
    @patch("read.postgresql_database.health_check_queries.logger")
    def test_should_log_error_and_close_session_when_an_sql_alchemy_error_is_raised(
        self, logger_mock, does_materialize_view_exist_mock
    ):
        # Given
        does_materialize_view_exist_mock.side_effect = SQLAlchemyError("", "", "")
        Session, local_session = _get_mocked_session()

        # When
        result = is_enriched_materialized_view_queryable(
            Session, "enriched_offerer_data"
        )

        # Then
        assert result is False
        local_session.close.assert_called_once()
        logger_mock.error.assert_called_once()

    @patch("read.postgresql_database.health_check_queries.does_materialize_view_exist")
    @patch("read.postgresql_database.health_check_queries.logger")
    def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
        self, logger_mock, does_materialize_view_exist_mock
    ):
        # Given
        does_materialize_view_exist_mock.side_effect = Exception
        Session, local_session = _get_mocked_session()

        # When
        with pytest.raises(Exception):
            is_enriched_materialized_view_queryable(Session, "enriched_offerer_data")

        # Then
        local_session.close.assert_not_called()
        logger_mock.error.assert_not_called()


class IsEnrichedViewQueryableTest:
    @patch("read.postgresql_database.health_check_queries.does_view_exist")
    def test_should_close_session_if_query_did_not_end_on_an_exception(
        self, does_view_exist_mock
    ):
        # Given
        does_view_exist_mock.return_value = True
        Session, local_session = _get_mocked_session()

        # When
        result = is_enriched_view_queryable(Session, "enriched_offer_data")

        # Then
        assert result is True
        local_session.close.assert_called_once()

    @patch("read.postgresql_database.health_check_queries.does_view_exist")
    def test_should_look_for_given_view_in_database(self, does_view_exist_mock):
        # Given
        does_view_exist_mock.return_value = True
        Session, local_session = _get_mocked_session()

        # When
        result = is_enriched_view_queryable(Session, "enriched_stock_data")

        # Then
        does_view_exist_mock.assert_called_once_with(
            local_session, "enriched_stock_data"
        )

    @patch("read.postgresql_database.health_check_queries.does_view_exist")
    @patch("read.postgresql_database.health_check_queries.logger")
    def test_should_log_error_and_close_session_when_operational_error_is_raised(
        self, logger_mock, does_view_exist_mock
    ):
        # Given
        does_view_exist_mock.side_effect = OperationalError("", "", "")
        Session, local_session = _get_mocked_session()

        # When
        result = is_enriched_view_queryable(Session, "enriched_offer_data")

        # Then
        assert result is False
        local_session.close.assert_called_once()
        logger_mock.error.assert_called_once()

    @patch("read.postgresql_database.health_check_queries.does_view_exist")
    @patch("read.postgresql_database.health_check_queries.logger")
    def test_should_log_error_and_close_session_when_an_sql_alchemy_error_is_raised(
        self, logger_mock, does_view_exist_mock
    ):
        # Given
        does_view_exist_mock.side_effect = SQLAlchemyError("", "", "")
        Session, local_session = _get_mocked_session()

        # When
        result = is_enriched_view_queryable(Session, "enriched_offer_data")

        # Then
        assert result is False
        local_session.close.assert_called_once()
        logger_mock.error.assert_called_once()

    @patch("read.postgresql_database.health_check_queries.does_view_exist")
    @patch("read.postgresql_database.health_check_queries.logger")
    def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
        self, logger_mock, does_view_exist_mock
    ):
        # Given
        does_view_exist_mock.side_effect = Exception
        Session, local_session = _get_mocked_session()

        # When
        with pytest.raises(Exception):
            is_enriched_view_queryable(Session, "enriched_offer_data")

        # Then
        local_session.close.assert_not_called()
        logger_mock.error.assert_not_called()


class DoesViewHaveDataTest:
    def setup_class(self):
        clean_database()
        clean_views()

    def test_should_return_false_if_view_exists_but_is_empty(self, app):
        # Given

        with app.app_context():
            create_enriched_offerer_data(db.engine)
            # When
            result = does_view_have_data(db.session, "enriched_offerer_data")
            db.session.close()

            # Then
            assert result is False

    def test_should_return_true_if_view_has_entries(self, app):
        # Given
        create_offerer(id=1)
        create_venue(offerer_id=1, id=1)
        create_product(id=1)
        create_offer(venue_id=1, product_id=1, id=1)
        create_stock(offer_id=1, date_created="2019-12-01")

        with app.app_context():
            create_enriched_stock_data(db.engine)
            # When
            result = does_view_have_data(db.session, "enriched_stock_data")
            db.session.close()

            # Then
            assert result is True


class DoesEnrichedOffererSourceContainDataTest:
    def setup_class(self):
        clean_database()
        clean_views()

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_when_the_view_is_not_found(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = False
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_offerer_contain_data(Session)

        # Then
        assert result is False
        is_enriched_materialized_view_queryable_mock.assert_called_once_with(
            Session, "enriched_offerer_data"
        )
        does_view_have_data_mock.assert_not_called()

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_if_view_is_empty(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = False
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_offerer_contain_data(Session)

        # Then
        assert result is False
        local_session.close.assert_called_once()
        is_enriched_materialized_view_queryable_mock.assert_called_once_with(
            Session, "enriched_offerer_data"
        )
        does_view_have_data_mock.assert_called_once_with(
            local_session, "enriched_offerer_data"
        )

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_true_if_view_has_data(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = True
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_offerer_contain_data(Session)

        # Then
        assert result is True
        local_session.close.assert_called_once()
        is_enriched_materialized_view_queryable_mock.assert_called_once_with(
            Session, "enriched_offerer_data"
        )
        does_view_have_data_mock.assert_called_once_with(
            local_session, "enriched_offerer_data"
        )

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_when_there_is_an_sql_alchemy_error_on_query(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = SQLAlchemyError
        create_enriched_offerer_data(ENGINE)
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_offerer_contain_data(Session)

        # Then
        assert result is False
        local_session.close.assert_called_once()

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = Exception
        Session, local_session = _get_mocked_session()

        # When
        with pytest.raises(Exception):
            result = does_enriched_offerer_contain_data(Session)
            # Then
            assert result is None

        local_session.close.assert_not_called()


class DoesEnrichedUserSourceContainsDataTest:
    def setup_class(self):
        clean_database()
        clean_views()

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_when_the_view_is_not_found(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = False
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_users_contains_data(Session)

        # Then
        assert result is False
        is_enriched_materialized_view_queryable_mock.assert_called_once_with(
            Session, "enriched_user_data"
        )
        does_view_have_data_mock.assert_not_called()

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_if_view_is_empty(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = False
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_users_contains_data(Session)

        # Then
        assert result is False
        local_session.close.assert_called_once()
        is_enriched_materialized_view_queryable_mock.assert_called_once_with(
            Session, "enriched_user_data"
        )
        does_view_have_data_mock.assert_called_once_with(
            local_session, "enriched_user_data"
        )

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_true_if_view_has_data(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = True
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_users_contains_data(Session)

        # Then
        assert result is True
        local_session.close.assert_called_once()
        is_enriched_materialized_view_queryable_mock.assert_called_once_with(
            Session, "enriched_user_data"
        )
        does_view_have_data_mock.assert_called_once_with(
            local_session, "enriched_user_data"
        )

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_when_there_is_an_sql_alchemy_error_on_query(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = SQLAlchemyError
        create_enriched_offerer_data(ENGINE)
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_users_contains_data(Session)

        # Then
        assert result is False
        local_session.close.assert_called_once()

    @patch(
        "read.postgresql_database.health_check_queries.is_enriched_materialized_view_queryable"
    )
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
        self, does_view_have_data_mock, is_enriched_materialized_view_queryable_mock
    ):
        # Given
        is_enriched_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = Exception
        Session, local_session = _get_mocked_session()

        # When
        with pytest.raises(Exception):
            result = does_enriched_users_contains_data(Session)
            # Then
            assert result is None

        local_session.close.assert_not_called()


class DoesEnrichedStocksSourceContainsDataTest:
    def setup_class(self):
        clean_database()
        clean_views()

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_when_the_view_is_not_found(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = False
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_stock_contain_data(Session)

        # Then
        assert result is False
        is_enriched_view_queryable_mock.assert_called_once_with(
            Session, "enriched_stock_data"
        )
        does_view_have_data_mock.assert_not_called()

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_if_view_is_empty(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = False
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_stock_contain_data(Session)

        # Then
        assert result is False
        local_session.close.assert_called_once()
        is_enriched_view_queryable_mock.assert_called_once_with(
            Session, "enriched_stock_data"
        )
        does_view_have_data_mock.assert_called_once_with(
            local_session, "enriched_stock_data"
        )

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_true_if_view_has_data(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = True
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_stock_contain_data(Session)

        # Then
        assert result is True
        local_session.close.assert_called_once()
        is_enriched_view_queryable_mock.assert_called_once_with(
            Session, "enriched_stock_data"
        )
        does_view_have_data_mock.assert_called_once_with(
            local_session, "enriched_stock_data"
        )

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_when_there_is_an_sql_alchemy_error_on_query(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = SQLAlchemyError
        create_enriched_offerer_data(ENGINE)
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_stock_contain_data(Session)

        # Then
        assert result is False
        local_session.close.assert_called_once()

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = Exception
        Session, local_session = _get_mocked_session()

        # When
        with pytest.raises(Exception):
            result = does_enriched_stock_contain_data(Session)
            # Then
            assert result is None

        local_session.close.assert_not_called()


class DoesEnrichedOfferSourceContainsDataTest:
    def setup_class(self):
        clean_database()
        clean_views()

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_when_the_view_is_not_found(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = False
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_offer_contain_data(Session)

        # Then
        assert result is False
        is_enriched_view_queryable_mock.assert_called_once_with(
            Session, "enriched_offer_data"
        )
        does_view_have_data_mock.assert_not_called()

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_if_view_is_empty(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = False
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_offer_contain_data(Session)

        # Then
        assert result is False
        local_session.close.assert_called_once()
        is_enriched_view_queryable_mock.assert_called_once_with(
            Session, "enriched_offer_data"
        )
        does_view_have_data_mock.assert_called_once_with(
            local_session, "enriched_offer_data"
        )

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_true_if_view_has_data(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = True
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_offer_contain_data(Session)

        # Then
        assert result is True
        local_session.close.assert_called_once()
        is_enriched_view_queryable_mock.assert_called_once_with(
            Session, "enriched_offer_data"
        )
        does_view_have_data_mock.assert_called_once_with(
            local_session, "enriched_offer_data"
        )

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_return_false_when_there_is_an_sql_alchemy_error_on_query(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = SQLAlchemyError
        create_enriched_offerer_data(ENGINE)
        Session, local_session = _get_mocked_session()

        # When
        result = does_enriched_offer_contain_data(Session)

        # Then
        assert result is False
        local_session.close.assert_called_once()

    @patch("read.postgresql_database.health_check_queries.is_enriched_view_queryable")
    @patch("read.postgresql_database.health_check_queries.does_view_have_data")
    def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
        self, does_view_have_data_mock, is_enriched_view_queryable_mock
    ):
        # Given
        is_enriched_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = Exception
        Session, local_session = _get_mocked_session()

        # When
        with pytest.raises(Exception):
            result = does_enriched_offer_contain_data(Session)
            # Then
            assert result is None

        local_session.close.assert_not_called()
