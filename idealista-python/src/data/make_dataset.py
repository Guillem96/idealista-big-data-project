# -*- coding: utf-8 -*-
import os
import json
import time
import click
import logging
from pathlib import Path

import tqdm.auto as tqdm

import dotenv
dotenv.load_dotenv()

import idealista_client

COORDINATES = {
    # 'LLEIDA_COORDS':'41.6183731,0.6024253', # Done
    # 'BARNA_COORDS':'41.3948976,2.0787282', # Done
    # 'GIRONA_COORDS': '41.98311,2.82493', # Done
    # 'TARRAGONA_COORDS':'41.1258358,1.2210739' # Done
    
    # 'MADRID_COORDS':'40.4381311,-3.8196196', # Done

    # 'VALENCIA_COORDS':'39.46975,-0.37739', # Done
    
    # 'ZARAGOZA_COORDS':'41.65606,-0.87734', # Done
    # 'MALAGA_COORDS':'36.72016,-4.42034', # Done
    
    # 'MURCIA_COORDS':'37.98704,-1.13004',
    # 'MALLORCA_COORDS':'39.56939,2.65024', # 23/34
    # 'BILBAO_COORDS':'43.26271,-2.92528', # Done
    # 'SEVILLA_COORDS':'37.38283,-5.97317', # Done
    
    # 'CANARIAS_COORDS':'28.09973,-15.41343', # Done
    # 'VALLADOLID_COORDS':'41.65518,-4.72372', # Done
    # 'VIGO_COORDS':'42.23282,-8.72264', # Done
    
    # 'CORUÑA_COORDS':'43.37135,-8.396', # Done
    # 'GRANADA_COORDS':'37.18817,-3.60667', # Done
    # 'OVIEDO_COORDS':'43.36029,-5.84476', # Done
    # 'TENERIFE_COORDS':'28.46824,-16.25462',
    # 'BADALONA_COORDS':'41.45004,2.24741', # Done
    # 'CARTAGENA_COORDS':'37.60512,-0.98623',
    # 'TERRASSA_COORDS':'41.56667,2.01667'

    # 'OURENSE_COORDS': '42.19645029937543,-7.59259790894106',
    # 'TERUEL_COORDS': '40.661261961518825,-0.815532258447',
    # 'SORIA_COORDS': '41.7671522,-2.4920886',

    'NAVARRA_COORDS': '42.66720115092274,-1.64611414442526',
    'CACERES_COORDS': '39.4716684,-6.4082288',
    'CADIZ_COORDS': '36.5164196,-6.2999767',
    'GRANCANARIA_COORDS': '27.9438395,-15.8219667',
    'MELILLA_COORDS': '35.2747902,-2.9612636',
    'CEUTA_COORDS': '35.8890899,-5.3360354'
}


@click.group()
def main():
    pass


@main.command()
@click.option('--operation', 
              type=click.Choice(['rent']), 
              default='rent')
@click.option('--output-dir', '-o', 
              type=click.Path(file_okay=False), 
              required=True)
def download(operation, output_dir):
    logger = logging.getLogger(__name__)
    logger.info('downloading dataset from Idealista API')

    output_dir = Path(output_dir)

    # Create the api client
    cli = idealista_client.IdealistaClient(
        api_key=os.environ['GOVE_API_KEY'], 
        secret=os.environ['GOVE_API_SECRET'])

    # For each city we download all the available flats
    for city, coords in COORDINATES.items():
        city_name = city.replace('_COORDS', '').title()
        out_fname = output_dir / f'{city_name}_{operation}.json'

        logger.info(f'downloading {city_name} to {str(out_fname)}')
        time.sleep(2)

        res = cli.search(
            center=coords,
            country='es',
            distance='50000',
            operation=operation,
            maxItems=1000,
            propertyType='homes',
            numPage=1).json()
        
        current_flats = res['elementList']

        # In some cases, the api calls have multiple pages, we iterate over them
        # to retrieve all the available data
        range_it = range(2, res['totalPages'] + 1)
        for page in tqdm.tqdm(range_it, desc=city_name):

            # We can only perform 1 request per second, just in case we wait for
            # 2 seconds
            time.sleep(2) 

            page += 1
            res = cli.search(
                center=coords,
                country='es',
                distance='50000',
                operation='rent',
                maxItems=100,
                propertyType='homes',
                numPage=page).json()
            current_flats.extend(res['elementList'])
            json.dump(current_flats, out_fname.open('w'))
        
        json.dump(current_flats, out_fname.open('w'))


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    main()
