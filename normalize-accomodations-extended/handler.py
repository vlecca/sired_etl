import pandas as pd
import logging
import os
import urllib
import boto3



COLUMNS_NAMES = [
    'Provincia',
    'Anno',
    'CodiceStruttura',
    'CodiceRegione',
    'IdStruttura',
    'Id1',
    'Id2',
    'Id3',
    'Id4',
    'Intestazione',
    'Denominazione',
    'IsDipendenza',
    'IsCasaMadre',
    'IdCasaMadre',
    'Tipologia',
    'Stelle',
    'CodiceComune',
    'Comune',
    'IndirizzoStruttura',
    'Telefono',
    'TelefonoStruttura',
    'Fax',
    'Email_1',
    'Email_2',
    'Emailpec',
    'Web',
    'TotaleLetti',
    'TotaleCamere',
    'TotaleBagni',
    'TotaleAppartamenti',
    'NumeroStrutture',
    'NoteStruttura',
    'IsAttivoIstat',
    'IsAttivoTariffe',
    'AttoNumero',
    'AttoDel',
    'AttoComune',
    'Apertura',
    'Apertura1Da',
    'Apertura1A',
    'Apertura2Da',
    'Apertura2A',
    'Apertura3Da',
    'Apertura3A',
    'Gestione',
    'Gestore',
    'RagioneSociale',
    'ComuneGestore',
    'ProvinciaGestore',
    'NomeGestore',
    'IndirizzoGestore',
    'TelefonoGestore',
    'CodiceFiscaleGestore',
    'DataNascitaGestore',
    'ComuneNascitaGestore',
    'TipoPersona',
    'Proprietario',
    'IndirizzoProprietario',
    'ComuneProprietario',
    'ProvinciaProprietario',
    'TelefonoProprietario',
    'Note'
]


def normalize_accomodations_extended(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    bucket_name = urllib.parse.unquote(event['Records'][0]['s3']['bucket']['name'])
    accomodations_extended_key = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])

    s3_client = boto3.client('s3')

    try:
        split_key = accomodations_extended_key.split('/')
        file_name = split_key[-1]
        dest_accomodations_extended_key = accomodations_extended_key.replace('raw', 'norm').replace('.csv', '_normalized.json')
        os.remove('/tmp/' + file_name)
        s3_client.download_file(bucket_name, accomodations_extended_key, '/tmp/' + file_name)
        write_json(read_csv('/tmp/' + file_name), dest_accomodations_extended_key)
    except Exception as e:
        print(e)


def read_csv(csv_path):
    return pd.read_csv(
        csv_path,
        header=None,
        names=COLUMNS_NAMES,
        dtypes={k: str for k in COLUMNS_NAMES},
        index_col=False,
        na_values='NUL',
        keep_default_na=True,
        encoding='utf8'
    )


def write_json(df, json_path):
    df.to_json(json_path, orient='records', lines=True, force_ascii=False)


