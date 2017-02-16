import pandas as pd
from sqlalchemy import Boolean
from sqlalchemy import Date, DateTime, Float, BigInteger, String
from sqlalchemy import MetaData
from sqlalchemy import Table, Column
from sqlalchemy.sql.ddl import CreateSchema

import resources
from run_config import hpo_ids, engine, datetime_tpe

LOG_TABLE_NAME = 'pmi_sprint_reporter_log'
SCHEMA_EXISTS_QUERY = "SELECT 1 FROM information_schema.schemata WHERE schema_name = '%s'"


def create_schema(schema):
    """
    Create schema if it doesn't exist
    :param schema: name of schema
    :return:
    """
    result = engine.execute(SCHEMA_EXISTS_QUERY % schema)
    if result.rowcount == 0:
        engine.execute(CreateSchema(schema))


def drop_tables(schema):
    """
    Drop any existing CDM tables (only PMI-related and logging tables)
    :param schema: Database schema
    :return:
    """
    metadata = MetaData(bind=engine, reflect=True, schema=schema)
    pmi_tables = pd.read_csv(resources.pmi_tables_csv_path).table_name.unique()
    tables_to_drop = filter(lambda t: t.name in pmi_tables or t.name == LOG_TABLE_NAME, metadata.sorted_tables)
    metadata.drop_all(tables=tables_to_drop)


def create_tables(schema):
    """
    Create CDM tables within the specified database schema.
    :param schema: Database schema to create the tables in
    :return:
    """
    metadata = MetaData()
    cdm_df = pd.read_csv(resources.cdm_csv_path)
    tables = cdm_df.groupby(['table_name'])

    for table_name, table_df in tables:
        columns = []
        for index, (_, column_name, is_nullable, data_type, _) in table_df.iterrows():
            if data_type in ('character varying', 'text'):
                tpe = String(500)
            elif data_type == 'integer':
                tpe = BigInteger()
            elif data_type == 'numeric':
                tpe = Float()
            elif data_type == 'date':
                tpe = Date()
            elif data_type == 'datetime':
                tpe = datetime_tpe
            else:
                raise NotImplementedError('Unexpected data_type `%s` in cdm.csv' % data_type)
            nullable = is_nullable == 'yes'
            columns.append(Column(column_name, tpe, nullable=nullable))
        Table(table_name, metadata, *columns, schema=schema)

    Table(LOG_TABLE_NAME,
          metadata,
          Column('hpo_id', String(100), nullable=False),
          Column('log_id', DateTime, nullable=False),
          Column('table_name', String(100), nullable=False),
          Column('file_name', String(200), nullable=False),
          Column('phase', String(200), nullable=False),
          Column('success', Boolean(), nullable=False),
          Column('message', String(500), nullable=True),
          Column('params', String(800), nullable=True),
          schema=schema)

    metadata.create_all(engine)


def create_mapping(hpo_id, table_name, df):
    """
    Creates a mapping dictionary based on the id columb of the specified
    CDM table within the specified schema.
    :param hpo_id: schema to merge into the All of Us schema
    :param table_name: OMOP CDM table to create a mapping dictionary for
    :param df: dataframe of the table_name
    :return: mapping dictionary
    """
    rs = engine.execute("select max(%s_id) from %s.%s" % (table_name, hpo_id, table_name)).first()
    max_id= (rs[0] if len(rs)>0 and rs[0] is not None else 0) + 1
    df = pd.read_sql_table(table_name, engine, hpo_id)
    #create a mapping with the key as the id value of the table and the value as an offset from the max_id
    return dict(zip(df['%s_id' % table_name], range(max_id, max_id + len(df['%s_id' % table_name]))))


def add_hpo_omop(hpo_id, merge_into_schema='aou'):
    """
    Adds the OMOP CDM tables from the individual HPOs into the AllofUs schema. This function
    will merge all the HPO sites' data into one OMOP CDM.
    :param hpo_id: schema to merge into the All of Us schema
    :param merge_into_schema: schema to merge into
    :return: mapping dictionary
    """
    cdm_df = pd.read_csv(resources.cdm_csv_path)
    included_tables = pd.read_csv(resources.pmi_tables_csv_path).table_name.unique()
    tables = cdm_df[cdm_df['table_name'].isin(included_tables)].groupby(['table_name'])
    person_mapping = {}
    visit_mapping = {}
    # The loop assumes person and visit_occurrence tables are first based on the pmi_tables.csv file
    for table_name in included_tables:
        df = pd.read_sql_table(table_name, engine, hpo_id)
        mapping = create_mapping(hpo_id, table_name, df)
        df['%s_id' % table_name].replace(mapping, inplace=True)

        if table_name == 'person':
            person_mapping = mapping
        elif table_name == 'visit_occurrence':
            visit_mapping = mapping
            df['person_id'].replace(person_mapping, inplace=True)
        else:
            df['person_id'].replace(person_mapping, inplace=True)
            df['visit_occurrence_id'].replace(visit_mapping, inplace=True)

        print 'loading %s.%s into %s schema...' % (hpo_id, table_name, merge_into_schema)
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False, schema=merge_into_schema, chunksize=1)


def main():
    aou_schema = 'aou'
    create_schema(aou_schema)
    drop_tables(schema=aou_schema)
    create_tables(schema=aou_schema)
    for hpo_id in hpo_ids:
        print 'Processing %s...' % hpo_id
        add_hpo_omop(hpo_id, aou_schema)


if __name__ == '__main__':
    main()
