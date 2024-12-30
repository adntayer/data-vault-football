import json
import math
from datetime import datetime

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from src import settings
from src.logger import SetupLogger

_log = SetupLogger('src.db.io_utils')

DEFAULT_DECIMAL_NUMBERS = 30
DEFAULT_ROUND_DECIMAL_FLOAT = 8
DEFAULT_FORMAT_DATE_CAST = 'YYYY-MM-DD HH24:MI:SS'
DEFAULT_FORMAT_DATE_CAST_PANDAS = '%Y-%m-%d %H:%M:%S'
DEFAULT_MISSING_DATE = '2000-01-01'
DEFAULT_MISSING_DATETIME = '2000-01-01 00:00:00'
DEFAULT_MISSING_STR = 'ThisIsNull'
DEFAULT_MISSING_FLOAT = 0.00
DEFAULT_MISSING_BOOL = False
DEFAULT_KEYWORDS_DB = ['limit', 'key', 'type', 'period', 'name']


def load_to_table(df, model, method, logger=None, id_log=None):
    dict_func = {
        'on_conflict_do_update': on_conflict_do_update_sqlite,
        'on_conflict_do_nothing': on_conflict_do_nothing_sqlite,
        'compare_hashdiff': compare_hashdiff,
    }

    func = dict_func.get(method, on_conflict_do_update_append)
    _log.info(f'({id_log}) load_to_table |  {method=}')
    if logger is not None:
        logger.info(f'({id_log}) load_to_table | {method=}')

    # load timestamp
    df['ldts'] = pd.to_datetime('now').strftime('%Y-%m-%d %H:%M:%S')

    func(df, model, logger, id_log)


def on_conflict_do_update_append(df, model, logger=None, id_log=None):
    _log = SetupLogger('src.db.io_utils')

    if logger is not None:
        _log = logger
    model_obj = model()
    table_name = model_obj.__tablename__
    table_schema = model_obj.__table_args__['schema']
    _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] | enter')

    # cretea a engine forcing the table schema
    _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] | creating engine DB setting schema')
    engine = create_engine(
        settings.DB_URI_OLTP,
        # connect_args={'options': f'-csearch_path={table_schema}'},  # https://stackoverflow.com/a/49930672
    )

    try:
        # perform upsert of df DataFrame values to a table `table_name` and Postgres connection defined at `engine`
        _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] |  reindexing')
        list_columns = [c.name for c in model_obj.__table__.columns]
        df = df.reindex(columns=list_columns)

        _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] | dropping duplicates')
        list_pks = [c.name for c in model_obj.__table__.columns if c.primary_key]
        df.drop_duplicates(subset=list_pks, inplace=True)

        _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] | to_sql')
        chunksize = 2_000
        div = df.shape[0] / chunksize
        _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] | rows on this round {df.shape[0]:_} --- running {math.ceil(div)} times // {chunksize=:_}')
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            for i in range(0, df.shape[0], chunksize):
                chunk = df.iloc[i : i + chunksize]
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    schema=table_schema,
                    index=False,
                    if_exists='append',
                )
                session.commit()
        except SQLAlchemyError as e:
            import sys
            import os

            exc_type, exc_obj, exc_tb = sys.exc_info()
            _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] |  ERROR [line = {exc_tb.tb_lineno}] // `{exc_type} ------ {str(e)}')
            raise e
        finally:
            session.close()

        _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] |  exiting')
    except Exception as e:
        import sys
        import os

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] |  ERROR >>>> {exc_type}, {fname}, {exc_tb.tb_lineno}  -- {str(e)}')
        raise e
    finally:
        _log.info(f'({id_log}) on_conflict_do_update_append | [{table_name}] | finally: closing connection and db engine')
        engine.dispose()


def on_conflict_do_update_sqlite(df, model, logger=None, id_log=None):
    _log = SetupLogger('src.db.io_utils')

    if logger is not None:
        _log = logger
    model_obj = model()
    table_name = model_obj.__tablename__
    table_schema = model_obj.__table_args__['schema']
    _log.info(f'({id_log}) on_conflict_do_update_sqlite | [{table_name}] | enter')

    # cretea a engine forcing the table schema
    _log.info(f'({id_log}) on_conflict_do_update_sqlite | [{table_name}] | creating engine DB setting schema')
    engine = create_engine(
        settings.DB_URI_OLTP,
    )

    try:
        # perform upsert of df DataFrame values to a table `table_name` and Postgres connection defined at `engine`
        _log.info(f'({id_log}) on_conflict_do_update_sqlite | [{table_name}] | reindexing')
        list_columns = [c.name for c in model_obj.__table__.columns]
        df = df.reindex(columns=list_columns)

        try:
            df = df.drop([None], axis=1)
            df = df.drop([''], axis=1)
        except KeyError:
            pass

        df = df.loc[:, ~df.columns.duplicated()]
        df = df.reindex(columns=list_columns)

        _log.info(f'({id_log}) on_conflict_do_update_sqlite | [{table_name}] | dropping duplicates')
        list_pks = [c.name for c in model_obj.__table__.columns if c.primary_key]
        df.drop_duplicates(subset=list_pks, inplace=True)

        _log.info(f'({id_log}) on_conflict_do_update_sqlite | [{table_name}] | to_sql')
        chunksize = 2_000
        div = df.shape[0] / chunksize
        _log.info(
            f'({id_log}) on_conflict_do_update_sqlite | [{table_name}] | rows on this round {df.shape[0]:_} --- running {math.ceil(div)} times // {chunksize=:_}',
        )

        list_where_clause = []
        for _column in model.__table__.columns:
            column_type, _column_name = _column.type, _column.name
            if isinstance(column_type, sa.sql.sqltypes.Float) or isinstance(column_type, sa.sql.sqltypes.Integer):
                list_where_clause.append(
                    f'COALESCE({table_name}.{_column_name}, {DEFAULT_MISSING_FLOAT})  IS NOT COALESCE(EXCLUDED.{_column_name}, COALESCE({table_name}.{_column_name}, {DEFAULT_MISSING_FLOAT}))',
                )
            elif isinstance(column_type, sa.sql.sqltypes.DateTime):
                df[_column_name] = pd.to_datetime(df[_column_name])
                list_where_clause.append(
                    f"COALESCE({table_name}.{_column_name}, DATETIME '{DEFAULT_MISSING_DATETIME}')  IS NOT COALESCE(EXCLUDED.{_column_name}, COALESCE({table_name}.{_column_name}, DATE '{DEFAULT_MISSING_DATETIME}'))",
                )
            elif isinstance(column_type, sa.sql.sqltypes.Date):
                df[_column_name] = pd.to_datetime(df[_column_name])
                list_where_clause.append(
                    f"COALESCE({table_name}.{_column_name}, DATE '{DEFAULT_MISSING_DATE}')  IS NOT COALESCE(EXCLUDED.{_column_name}, COALESCE({table_name}.{_column_name}, DATE '{DEFAULT_MISSING_DATE}'))",
                )
            elif isinstance(column_type, sa.sql.sqltypes.Text) or isinstance(column_type, sa.sql.sqltypes.String):
                list_where_clause.append(
                    f"COALESCE({table_name}.{_column_name}, '{DEFAULT_MISSING_STR}')  IS NOT COALESCE(EXCLUDED.{_column_name}, COALESCE({table_name}.{_column_name}, '{DEFAULT_MISSING_STR}'))",
                )
            elif isinstance(column_type, sa.sql.sqltypes.Boolean) or isinstance(
                column_type,
                sa.sql.sqltypes.LargeBinary,
            ):
                list_where_clause.append(
                    f'COALESCE({table_name}.{_column_name}, {DEFAULT_MISSING_BOOL})  IS NOT COALESCE(EXCLUDED.{_column_name}, COALESCE({table_name}.{_column_name}, {DEFAULT_MISSING_BOOL}) )',
                )
            else:
                raise ValueError(_column_name)
        str_where_clause = ' OR '.join(list_where_clause)
        where_condition_sql = str_where_clause

        def insert_on_duplicate(table, conn, keys, data_iter):
            from sqlalchemy.dialects.sqlite import insert

            insert_stmt = insert(table.table).values(list(data_iter))
            on_duplicate_key_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[col.name for col in model_obj.__table__.columns if col.primary_key],
                set_={exc_k.key: exc_k for exc_k in insert_stmt.excluded},
                where=text(where_condition_sql),
            )

            result = conn.execute(on_duplicate_key_stmt)
            _log.info(f'({id_log}) on_conflict_do_update_sqlite | [{table_name}] | upsert {result.rowcount} rows')

        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            for i in range(0, df.shape[0], chunksize):
                chunk = df.iloc[i : i + chunksize]
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    schema=table_schema,
                    index=False,
                    if_exists='append',
                    method=insert_on_duplicate,
                )
                session.commit()
        except SQLAlchemyError as e:
            import sys
            import os

            exc_type, exc_obj, exc_tb = sys.exc_info()
            _log.error(f'({id_log}) on_conflict_do_update_append | [{table_name}] | ERROR [line = {exc_tb.tb_lineno}] // `{exc_type} ------ {str(e)}')
            raise e
        finally:
            session.close()
    except Exception as e:
        import sys
        import os

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        _log.error(f'({id_log}) on_conflict_do_update_sqlite | [{table_name}] | ERROR >>>> {exc_type}, {fname}, {exc_tb.tb_lineno}  -- {str(e)}')
        raise e
    finally:
        _log.info(f'({id_log}) on_conflict_do_update_sqlite | [{table_name}] | finally: closing connection and db engine')
        engine.dispose()


def on_conflict_do_nothing_sqlite(df, model, logger=None, id_log=None):
    _log = SetupLogger('src.db.io_utils')

    if logger is not None:
        _log = logger
    model_obj = model()
    table_name = model_obj.__tablename__
    table_schema = model_obj.__table_args__['schema']
    _log.info(f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] | enter')

    # cretea a engine forcing the table schema
    _log.info(f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] | creating engine DB setting schema')
    engine = create_engine(
        settings.DB_URI_OLTP,
    )

    try:
        # perform upsert of df DataFrame values to a table `table_name` and Postgres connection defined at `engine`
        _log.info(f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] |  reindexing')
        list_columns = [c.name for c in model_obj.__table__.columns]
        df = df.reindex(columns=list_columns)

        try:
            df = df.drop([None], axis=1)
            df = df.drop([''], axis=1)
        except KeyError:
            pass

        df = df.loc[:, ~df.columns.duplicated()]
        df = df.reindex(columns=list_columns)

        _log.info(f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] |  dropping duplicates')
        list_pks = [c.name for c in model_obj.__table__.columns if c.primary_key]
        df.drop_duplicates(subset=list_pks, inplace=True)

        _log.info(f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] |  to_sql')
        chunksize = 2_000
        div = df.shape[0] / chunksize
        _log.info(
            f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] |  rows on this round {df.shape[0]:_} --- running {math.ceil(div)} times // {chunksize=:_}',
        )

        def insert_do_nothing(table, conn, keys, data_iter):
            from sqlalchemy.dialects.sqlite import insert

            insert_stmt = insert(table.table).values(list(data_iter))
            on_duplicate_key_stmt = insert_stmt.on_conflict_do_nothing(
                index_elements=[col.name for col in model_obj.__table__.columns if col.primary_key],
            )
            result = conn.execute(on_duplicate_key_stmt)
            _log.info(f'\t({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] | inserted {result.rowcount} rows')

        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            for i in range(0, df.shape[0], chunksize):
                chunk = df.iloc[i : i + chunksize]
                _log.info(f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] | loop {i/chunksize+1:3.0f}/{math.ceil(div):3}')
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    schema=table_schema,
                    index=False,
                    if_exists='append',
                    method=insert_do_nothing,
                )
                session.commit()
        except SQLAlchemyError as e:
            import sys
            import os

            exc_type, exc_obj, exc_tb = sys.exc_info()
            _log.error(f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] | ERROR [line = {exc_tb.tb_lineno}] // `{exc_type} ------ {str(e)}')
            raise e
        finally:
            session.close()
    except Exception as e:
        import sys
        import os

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        _log.error(f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] |  ERROR >>>> {exc_type}, {fname}, {exc_tb.tb_lineno}  -- {str(e)}')
        raise e
    finally:
        _log.info(f'({id_log}) on_conflict_do_nothing_sqlite | [{table_name}] |  finally: closing connection and db engine')
        engine.dispose()


def compare_hashdiff(df, model, logger=None, id_log=None):
    _log = SetupLogger('src.db.io_utils')

    if logger is not None:
        _log = logger
    from ulid import ULID

    model_obj = model()
    table_name = model_obj.__tablename__
    table_schema = model_obj.__table_args__['schema']
    list_columns = [c.name for c in model_obj.__table__.columns]
    _log.info(f'({id_log}) compare_hashdiff | [{table_name}] | enter')

    # cretea a engine forcing the table schema
    _log.info(f'({id_log}) compare_hashdiff | [{table_name}] | creating engine DB setting schema')
    engine = create_engine(settings.DB_URI_OLTP)

    try:
        df['ulid'] = [str(ULID()) for _ in range(df.shape[0])]

        query = f"select hk_hub, hash_diff from {table_name} where hk_hub in {tuple(df['hk_hub'].tolist())}"
        df_db = pd.read_sql(query, engine)
        if df_db.empty:
            _log.info(f'({id_log}) compare_hashdiff | [{table_name}] | df_db is empty... no hk_hubs found.. loading all rows')
            df.to_sql(name=table_name, con=engine, schema=table_schema, index=False, if_exists='append')
            return
        else:
            _log.info(f'({id_log}) compare_hashdiff | [{table_name}] | df_db is with results')
            df_m = df.merge(df_db, on=['hk_hub', 'hash_diff'], how='outer', indicator=True)
            df_new = df_m.loc[df_m['_merge'] == 'left_only']
            if df_new.empty:
                _log.info(f'({id_log}) compare_hashdiff | [{table_name}] | df new incoming is equals to db.. nothing to do.. exiting')
                return
            else:
                _log.info(f'({id_log}) compare_hashdiff | [{table_name}] | df new incoming have {df_new.shape[0]} new values.. inserting them')
                df_to_db = df_new.reindex(columns=list_columns)
                df_to_db.to_sql(name=table_name, con=engine, schema=table_schema, index=False, if_exists='append')
                return
    except Exception as e:
        import sys
        import os

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        _log.error(f'({id_log}) compare_hashdiff | [{table_name}] | ERROR >>>> {exc_type}, {fname}, {exc_tb.tb_lineno}  -- {str(e)}')
        raise e
    finally:
        _log.info(f'({id_log}) compare_hashdiff | [{table_name}] | finally: closing connection and db engine')
        engine.dispose()


def on_conflict_do_update_postgres(df, model, logger=None, id_log=None):
    _log = SetupLogger('src.db.io_utils')

    if logger is not None:
        _log = logger
    import inspect
    import os
    from ulid import ULID

    execution_id = str(ULID())

    script_path, function_name = inspect.stack()[1][1], inspect.stack()[1][3]
    script_name = os.path.basename(script_path)
    engine_meta = create_engine(
        settings.DB_URI_OLTP,
        connect_args={'options': '-csearch_path=backstage'},
    )  # https://stackoverflow.com/a/49930672

    stack = inspect.stack()
    dict_args = {}
    for frame in stack:
        caller_code = frame.frame.f_code
        if caller_code.co_name != 'output_table_upsert':
            args, _, _, values = inspect.getargvalues(frame.frame)
            for arg in args:
                dict_args[arg] = values[arg]
            break

    def create_upsert_method(meta: sa.MetaData, extra_update_fields=None):
        """
        Create upsert method that satisfied the pandas's to_sql API.
        """

        def method(table, conn, keys, data_iter):
            # select table that data is being inserted to (from pandas's context)
            _log.info(f'({id_log}) on_conflict_do_update_pg | ### NEW CHUNCK')
            _log.info(f'({id_log}) on_conflict_do_update_pg | \tgo for table')
            sql_table = sa.Table(table.name, meta, autoload=True)

            # list of dictionaries {col_name: value} of data to insert
            _log.info(f'({id_log}) on_conflict_do_update_pg | \tdata massage data_iter')
            values_to_insert = [dict(zip(keys, data)) for data in data_iter]

            # create insert statement using postgresql dialect.
            # For other dialects, please refer to https://docs.sqlalchemy.org/en/14/dialects/
            _log.info(f'({id_log}) on_conflict_do_update_pg | \tcreating insert statement using postgresql dialect.')
            insert_stmt = sa.dialects.postgresql.insert(sql_table, values_to_insert)

            # create update statement for excluded fields on conflict
            update_stmt = {exc_k.key: exc_k for exc_k in insert_stmt.excluded}
            if extra_update_fields:
                update_stmt.update(extra_update_fields)

            # create where statement
            _log.info(f'({id_log}) on_conflict_do_update_pg | \tcreating where statement')
            # https://stackoverflow.com/a/63747480
            # https://www.postgresql.org/message-id/20161004205648.GV4498@aart.rice.edu
            # NULL <> NULL so each insert with a NULL will create a new row. A NULL value is defined to be
            # an unknown value so two INSERTs of:
            # INSERT INTO test_upsert as tu(name,status,test_field,identifier, count) VALUES ('shaun',1,null,'ident', 1)
            # INSERT INTO test_upsert as tu(name,status,test_field,identifier, count) VALUES ('shaun',1,null,'ident', 1)
            # are inserting different rows. You might want to change your NULL to the empty string
            # or some other fixed token if you actually want them to work as equal.
            list_where_clause = []
            for _column in model.__table__.columns:
                column_type, _column_name = _column.type, _column.name
                if isinstance(column_type, sa.sql.sqltypes.Float) or isinstance(column_type, sa.sql.sqltypes.Integer):
                    list_where_clause.append(
                        f'COALESCE({table_name}.{_column_name}, {DEFAULT_MISSING_FLOAT}) IS DISTINCT FROM COALESCE(EXCLUDED.{_column_name}, COALESCE({table_name}.{_column_name}, {DEFAULT_MISSING_FLOAT}))',
                    )
                elif isinstance(column_type, sa.sql.sqltypes.DateTime) or isinstance(column_type, sa.sql.sqltypes.Date):
                    df[_column_name] = pd.to_datetime(df[_column_name])
                    list_where_clause.append(
                        f"COALESCE({table_name}.{_column_name}, DATE '{DEFAULT_MISSING_DATE}') IS DISTINCT FROM COALESCE(EXCLUDED.{_column_name}, COALESCE({table_name}.{_column_name}, DATE '{DEFAULT_MISSING_DATE}'))",
                    )
                elif isinstance(column_type, sa.sql.sqltypes.Text) or isinstance(column_type, sa.sql.sqltypes.String):
                    list_where_clause.append(
                        f"COALESCE({table_name}.{_column_name}, '{DEFAULT_MISSING_STR}') IS DISTINCT FROM COALESCE(EXCLUDED.{_column_name}, COALESCE({table_name}.{_column_name}, '{DEFAULT_MISSING_STR}'))",
                    )
                elif isinstance(column_type, sa.sql.sqltypes.Boolean) or isinstance(
                    column_type,
                    sa.sql.sqltypes.LargeBinary,
                ):
                    list_where_clause.append(
                        f'COALESCE({table_name}.{_column_name}, {DEFAULT_MISSING_BOOL}) IS DISTINCT FROM COALESCE(EXCLUDED.{_column_name}, COALESCE({table_name}.{_column_name}, {DEFAULT_MISSING_BOOL}) )',
                    )
                else:
                    raise ValueError(_column_name)
            str_where_clause = ' OR '.join(list_where_clause)
            where_condition_sql = str_where_clause

            _log.info(f'({id_log}) on_conflict_do_update_pg | \tcreating upsert_stmt with on_conflict_do_update')
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=sql_table.primary_key.columns,
                set_=update_stmt,
                where=text(where_condition_sql),  # from sqlalchemy import text
            ).returning(sa.column('xmax') == 0, model)
            # sa.column("xmax") == 0 -> https://stackoverflow.com/a/70160393 :  check if is update // False = update
            # returning --- Up (False, 'row_val1',  etc) // Ins (True, 'row_val1',  etc)

            _log.info(f'({id_log}) on_conflict_do_update_pg | \trunning upsert_stmt')
            result = conn.execute(upsert_stmt)
            list_result = result.all()
            count_insert = 0
            count_update = 0
            if len(list_result) == 0:
                _log.info(f'({id_log}) on_conflict_do_update_pg | \tnothing changed')
            else:
                count_insert = sum([1 for c in list_result if c[0]])
                count_update = len(list_result) - count_insert
                _log.info(f'({id_log}) on_conflict_do_update_pg | \t[total inc] {len(values_to_insert):_}')
                _log.info(f'({id_log}) on_conflict_do_update_pg | \t[total ups] {len(list_result):_}')
                _log.info(f'({id_log}) on_conflict_do_update_pg | \t[ inserted] {count_insert:_}')
                _log.info(f'({id_log}) on_conflict_do_update_pg | \t[ updated] {count_update:_}')
                for row in list_result:
                    if row[0]:  # True
                        _log.debug(f'on_conflict_do_update_pg | \t[row(s) inserted] /// {row[1:]}')
                    else:  # False
                        _log.debug(f'on_conflict_do_update_pg | \t[ row(s) updated] /// {row[1:]}')

            dict_to_meta_db = {
                'execution_id': execution_id,
                'executed_at': datetime.now(),
                'target_schema': table.schema,
                'target_table': table.name,
                'rows_input': len(values_to_insert),
                'rows_inserted': count_insert,
                'rows_updated': count_update,
                'rows_changed': count_insert + count_update,
                'function_name': function_name,
                'function_args': json.dumps(dict_args),
                'script_name': script_name,
                'script_path': script_path,
            }
            pd.DataFrame([dict_to_meta_db]).to_sql(
                name='metadata_upsert',
                con=engine_meta,
                schema='public',
                index=False,
                if_exists='append',
            )

        return method

    _log.info(f'({id_log}) on_conflict_do_update_pg | enter')

    _log.info(f'({id_log}) on_conflict_do_update_pg | creating model obj')
    model_obj = model()
    table_name = model_obj.__tablename__
    table_schema = model_obj.__table_args__['schema']

    # cretea a engine forcing the table schema
    _log.info(f'({id_log}) on_conflict_do_update_pg | creating engine DB setting schema')
    engine = create_engine(
        settings.DB_URI_OLTP,
        connect_args={'options': f'-csearch_path={table_schema}'},  # https://stackoverflow.com/a/49930672
    )
    connection = engine.raw_connection()
    cursor = connection.cursor()
    try:
        # create DB metadata object that can access table names, primary keys, etc.
        meta = sa.MetaData(engine)

        # dictionary which will add additional changes on update statement. I.e. all the columns which are not present in DataFrame,
        # but needed to be updated regardless. The common example is `updated_at`. This column can be updated right on SQL server, instead of in pandas DataFrame
        # extra_update_fields = {"updated_at": "NOW()"}
        extra_update_fields = {}

        _log.info(f'({id_log}) on_conflict_do_update_pg | create_upsert_method')
        # create upsert method that is accepted by pandas API
        upsert_method = create_upsert_method(meta, extra_update_fields=extra_update_fields)

        # perform upsert of df DataFrame values to a table `table_name` and Postgres connection defined at `engine`
        _log.info(f'({id_log}) on_conflict_do_update_pg | reindexing')
        list_columns = [c.name for c in model_obj.__table__.columns]
        df = df.reindex(columns=list_columns)

        _log.info(f'({id_log}) on_conflict_do_update_pg | dropping duplicates')
        list_pks = [c.name for c in model_obj.__table__.columns if c.primary_key]
        df.drop_duplicates(list_pks, inplace=True)

        _log.info(f'({id_log}) on_conflict_do_update_pg | to_sql')
        chunksize = 3_000
        div = df.shape[0] / chunksize
        _log.info(
            f'on_conflict_do_update_pg | rows on this round {df.shape[0]:_} --- running {math.ceil(div)} times // {chunksize=:_}',
        )
        df.to_sql(
            name=table_name,
            con=engine,
            schema=table_schema,
            index=False,
            if_exists='append',
            chunksize=chunksize,
            method=upsert_method,
        )
        connection.commit()
        _log.info(f'({id_log}) on_conflict_do_update_pg | exiting')
    except Exception as e:
        import sys
        import os

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        _log.error(f'({id_log}) on_conflict_do_update_pg | ERROR >>>> {exc_type}, {fname}, {exc_tb.tb_lineno}  -- {str(e)}')
        raise e
    finally:
        _log.info(f'({id_log}) on_conflict_do_update_pg | finally: closing connection and db engine')
        cursor.close()
        engine.dispose()
