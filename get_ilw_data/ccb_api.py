"""
ccb_api.py

Handles all interactions with the CCB API for ILW data processing.
"""
from .config import Config
from typing import Set, List, Tuple, Dict, Any
import requests
import json
import csv
import io
import datetime
import logging
from . import util

# Functions to be implemented:
# - get_list_of_ilw_individuals
# - get_list_of_ilw_transactions

# Example stub:
def get_list_of_ilw_individuals(config: Config, set_of_giving_family_ids: Set[int]) -> List[List[str]]:
    """
    Retrieve individual data from CCB API for the given family IDs.
    """
    pass

def get_list_of_ilw_transactions(config: Config) -> Tuple[Set[int], Set[int], List[List[str]]]:
    """
    Retrieve transaction data from CCB API.
    """
    pass

def get_list_of_ilw_individuals(config: Config, set_of_giving_family_ids: Set[int]) -> List[List[str]]:
    """
    Retrieve individual data from CCB API for the given family IDs.
    """
    individual_detail_report_info = {
        'id': '',
        'type': 'export_individuals_change_log',
        'print_type': 'export_individuals',
        'query_id': '',
        'campus_ids': ['1']
    }
    individual_detail_request = {
        'request': json.dumps(individual_detail_report_info),
        'output': 'export'
    }
    with requests.Session() as http_session:
        util.login(http_session, config.ccb_subdomain, config.ccb_app_username, config.ccb_app_password)
        logging.info('Note that it takes CCB a minute or two to pull retrieve all individual information')
        individual_detail_response = http_session.post(
            f'https://{config.ccb_subdomain}.ccbchurch.com/report.php',
            data=individual_detail_request)
        individual_detail_response.encoding = 'utf-8-sig'
        rows = []
        if individual_detail_response.status_code == 200 and \
           individual_detail_response.text[:9] == '"Ind ID",':
            csv_reader = csv.reader(io.StringIO(individual_detail_response.text))
            found_header_row = False
            family_id_column = None
            for row in csv_reader:
                if found_header_row:
                    if int(row[family_id_column]) in set_of_giving_family_ids:
                        rows.append(row)
                else:
                    found_header_row = True
                    family_id_column = row.index('Family ID')
                    rows.append(row)
            logging.info('Individual info successfully retrieved')
        else:
            logging.error('Individual Detail retrieval failed')
            raise RuntimeError('Individual Detail retrieval failed')
        return rows

def get_list_of_ilw_transactions(config: Config) -> Tuple[Set[int], Set[int], List[List[str]]]:
    """
    Retrieve transaction data from CCB API.
    """
    start_date = '01/01/2013'
    end_date = datetime.datetime.now().strftime('%m/%d/%Y')
    list_of_ilw_transactions = []
    set_of_giving_family_ids = set()
    set_of_giving_individual_ids = set()
    ilw_coa_list = [
        'Ingomar Living Waters',
        'Living Water General Donation',
        'Living Water General Donation (Non-TD)',
        'Auctions',
        'Auctions (Non-TD)',
        'Projects',
        'Projects (Non-TD)',
        'Sponsorships & Tickets',
        'Sponsorships & Tickets (Non-TD)',
        'Water Filters',
        'Water Filters (Non-TD)',
        'Old WaterWorks Heading',
        'Old Wine to Water Heading',
        'Old Living Water Donation',
        'Old WtW - Sponsor (TD)',
        'Old WtW Auction TD',
        'Living Waters Event - not used',
        'I-47H6A WaterWorks Donations',
        'Wine to Water - Tickets',
        'Wine to Water - General',
        'Ellis LW Holiday Fundraiser'
    ]
    with requests.Session() as http_session:
        util.login(http_session, config.ccb_subdomain, config.ccb_app_username, config.ccb_app_password)
        coa_column = None
        start_datetime_object = datetime.datetime.strptime(start_date, '%m/%d/%Y')
        end_datetime_object = datetime.datetime.strptime(end_date, '%m/%d/%Y')
        for year in range(start_datetime_object.year, end_datetime_object.year + 1):
            if end_datetime_object.year > start_datetime_object.year:
                if year == start_datetime_object.year:
                    start_date_str = start_datetime_object.strftime('%m/%d/%Y')
                    end_date_str = f'12/31/{year}'
                elif year == end_datetime_object.year:
                    start_date_str = f'01/01/{year}'
                    end_date_str = end_datetime_object.strftime('%m/%d/%Y')
                else:
                    start_date_str = f'01/01/{year}'
                    end_date_str = f'12/31/{year}'
            else:
                start_date_str = start_datetime_object.strftime('%m/%d/%Y')
                end_date_str = end_datetime_object.strftime('%m/%d/%Y')
            transaction_detail_report_info = {
                "id": "",
                "type": "transaction_detail",
                "email_pdf": "0",
                "is_contextual": "1",
                "transaction_detail_type_id": "0",
                "date_range": "",
                "ignore_static_range": "static",
                "start_date": start_date_str,
                "end_date": end_date_str,
                "campus_ids": ["1"],
                "output": "csv"
            }
            transaction_detail_request = {
                'aj': 1,
                'ax': 'run',
                'request': json.dumps(transaction_detail_report_info)
            }
            logging.info(f'Retrieving info from {start_date_str} to {end_date_str}')
            transaction_detail_response = http_session.post(
                f'https://{config.ccb_subdomain}.ccbchurch.com/report.php',
                data=transaction_detail_request)
            transaction_detail_response.encoding = 'utf-8-sig'
            transaction_detail_succeeded = False
            if transaction_detail_response.status_code == 200:
                if transaction_detail_response.text[:12] == 'Name,Campus,':
                    transaction_detail_succeeded = True
                    csv_reader = csv.reader(io.StringIO(transaction_detail_response.text))
                    first_row_already_retrieved = False
                    for row in csv_reader:
                        if first_row_already_retrieved:
                            sub_coas = row[coa_column].split(' : ')
                            if sub_coas[-1] in ilw_coa_list:
                                set_of_giving_family_ids.add(int(row[family_id_column]))
                                set_of_giving_individual_ids.add(int(row[individual_id_column]))
                                list_of_ilw_transactions.append(row)
                        else:
                            first_row_already_retrieved = True
                            if coa_column is None:
                                coa_column = row.index('COA Category')
                                date_column = row.index('Date')
                                family_id_column = row.index('Family ID')
                                individual_id_column = row.index('Ind ID')
                                list_of_ilw_transactions.append(row)
                    if not transaction_detail_succeeded:
                        logging.error('Contribution Detail retrieval failed (will time out if too much data retrieved)')
                        raise RuntimeError('Contribution Detail retrieval failed')
                    else:
                        logging.info('Transaction info successfully retrieved')
                else:
                    logging.info('No results returned...skipping.')
    return set_of_giving_family_ids, set_of_giving_individual_ids, list_of_ilw_transactions 