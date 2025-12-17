import typer
import locale
import datetime
import os
import glob
import pandas as pd
from ansible_vault import Vault
from .config import Config
from .models import LoggingLevel
from .logging_utils import setup_logging
from .email_utils import send_admin_email
from .ccb_api import get_list_of_ilw_individuals, get_list_of_ilw_transactions
from .data_processing import (
    get_lists_from_file, write_lists_to_file, list_to_dataframe, preprocess_deceased_individuals,
    drop_or_remap_children_givers, merge_down_alternate_name, map_transaction_fam_ids, get_mapping_dicts,
    reload_names_and_emails, calculate_follow_ups, get_cell_loc, set_column_number_format, set_column_widths,
    get_pretty_emails_from_fam
)
import numpy as np
import openpyxl
from openpyxl import Workbook
from openpyxl.comments import Comment
from copy import copy
import logging

app = typer.Typer()

TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'

@app.command()
def process(
    xlsx_input_file: str = typer.Option(None, help="Path of XLSX input file, which is normally Input.xlsx in the program directory."),
    xlsx_output_file: str = typer.Option(None, help="Path for XLSX output file. If not specified, defaults to ilw_data_[YYYYMMDDhhmmss].xlsx in the 'tmp' subdirectory."),
    use_file_cache: bool = typer.Option(False, help="Use file cache instead of pulling from CCB API."),
    no_email: bool = typer.Option(False, help="Do not send notification emails."),
    logging_level: str = typer.Option(LoggingLevel.warning.value, case_sensitive=False),
    before_after_csvs: bool = typer.Option(False, help="Create CSVs in before_after_csvs subdirectory capturing state before and after applying overlay and concatenation data.")
):
    """
    Main processing pipeline for ILW data.
    """
    # Set up config and runtime state
    config = Config()
    locale.setlocale(locale.LC_ALL, '')
    config.datetime_start = datetime.datetime.now()
    config.curr_year = config.datetime_start.year
    config.datetime_start_string = config.datetime_start.strftime(TIMESTAMP_FORMAT)
    config.prog_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config.prog_name = os.path.basename(__file__)

    # Set up logging
    setup_logging(config, logging_level)

    # Pull info from vault file to support email notifications and enable CCB access
    secrets_files = glob.glob(os.path.join(config.prog_dir, '.secrets_*'))
    if len(secrets_files) > 1:
        raise RuntimeError(f"Found more than one .secrets_* file: {', '.join(secrets_files)}. Can only be one.")
    with open(secrets_files[0]) as file:
        vault_password = file.readline()
    vault_file_path = os.path.join(config.prog_dir, 'vault.yml')
    vault = Vault(vault_password)
    vault_data = vault.load(open(vault_file_path).read())
    if not no_email:
        if 'gmail' not in vault_data:
            raise RuntimeError(f"'gmail' is a required section in {vault_file_path} file")
        vault_data_gmail = vault_data['gmail']
        config.gmail_user = vault_data_gmail['user']
        config.gmail_password = vault_data_gmail['password']
        config.notification_target_email = vault_data_gmail['notify_target']
    vault_data_ccb = vault_data['ccb']
    config.ccb_app_username = vault_data_ccb['app_username']
    config.ccb_app_password = vault_data_ccb['app_password']
    config.ccb_subdomain = vault_data_ccb['subdomain']

    # Process command-line args
    if xlsx_input_file is None:
        try_xlsx_input_file = os.path.join(config.prog_dir, 'Input.xlsx')
        if os.path.exists(try_xlsx_input_file):
            xlsx_input_file = try_xlsx_input_file
        else:
            raise RuntimeError(f"Default input file, '{try_xlsx_input_file}', does not exist. Therefore, you must specify XLSX input file using --xlsx-input-file and this file must have 4 tabs: 'IndividualUpdate', 'IndividualConcat', 'CoaRemap', and 'ReachOutContacts'.")
    else:
        if not os.path.exists(xlsx_input_file):
            raise RuntimeError(f"Specified XLSX input file, '{xlsx_input_file}', not found.")
    
    # Validate and process xlsx_output_file
    if xlsx_output_file is not None:
        # Ensure it has .xlsx extension
        if not xlsx_output_file.lower().endswith('.xlsx'):
            raise RuntimeError(f"Output file '{xlsx_output_file}' must have a .xlsx extension.")
        
        # Convert to absolute path if relative
        # For relative paths, resolve them relative to the original working directory
        # that was active when the script was called, not the current working directory
        if not os.path.isabs(xlsx_output_file):
            # Get the original working directory from environment variable if set by shell script
            # Otherwise fall back to current working directory
            original_cwd = os.environ.get('ORIGINAL_CWD', os.getcwd())
            xlsx_output_file = os.path.join(original_cwd, xlsx_output_file)
        
        # Validate that the directory exists
        output_dir = os.path.dirname(xlsx_output_file)
        if not os.path.exists(output_dir):
            raise RuntimeError(f"Directory '{output_dir}' does not exist for output file '{xlsx_output_file}'.")
        
        # Check if directory is writable
        if not os.access(output_dir, os.W_OK):
            raise RuntimeError(f"Directory '{output_dir}' is not writable for output file '{xlsx_output_file}'.")

    # Data acquisition
    if use_file_cache:
        config_log = config.prog_dir  # for file cache
        typer.echo('Pulling transactions and individuals from file cache.')
        set_of_giving_family_ids, set_of_giving_individual_ids, list_of_ilw_transactions, list_of_ilw_individuals = get_lists_from_file(config_log)
        with pd.ExcelFile(xlsx_input_file) as xlsx:
            df_add_families = pd.read_excel(xlsx, sheet_name='NonGivingFamilies')
            set_of_giving_family_ids.update(df_add_families['Family ID'].tolist())
        typer.echo('Done pulling transactions and individuals from file cache.')
    else:
        typer.echo('Pulling transactions from CCB API...')
        set_of_giving_family_ids, set_of_giving_individual_ids, list_of_ilw_transactions = get_list_of_ilw_transactions(config)
        typer.echo('Done pulling transactions from CCB API.')
        typer.echo('Pulling individuals from CCB API...')
        with pd.ExcelFile(xlsx_input_file) as xlsx:
            df_add_families = pd.read_excel(xlsx, sheet_name='NonGivingFamilies')
            set_of_giving_family_ids.update(df_add_families['Family ID'].tolist())
        list_of_ilw_individuals = get_list_of_ilw_individuals(config, set_of_giving_family_ids)
        write_lists_to_file(config.prog_dir, set_of_giving_family_ids, set_of_giving_individual_ids, list_of_ilw_transactions, list_of_ilw_individuals)
        typer.echo('Done pulling individuals from CCB API.')

    # --- DataFrame merging and transformation ---
    list_of_individual_columns_to_drop = [
        'Limited Access User', 'Campus', 'Email Privacy Level',
        'General Communication', 'Mailing Area', 'Mailing Carrier Route', 'Mailing Privacy Level',
        'Home Phone Privacy Level', 'Work Phone Privacy Level', 'Mobile Phone Privacy Level', 'Fax',
        'Fax Phone Privacy Level', 'Pager Phone', 'Pager Phone Privacy Level', 'Emergency Phone',
        'Emergency Phone Privacy Level', 'Emergency Contact Name', 'Birthday Privacy Level',
        'Anniversary Privacy Level', 'Gender Privacy Level', 'Giving ID', 'Marital Status Privacy Level',
        'Home Area', 'Home Street', 'Home City', 'Home State', 'Home Zip', 'Home Country', 'Home Privacy Level',
        'Work Area', 'Work Street', 'Work City', 'Work State', 'Work Zip', 'Work Country', 'Work Privacy Level',
        'Other Area', 'Other Street', 'Other City', 'Other State', 'Other Zip', 'Other Country',
        'Other Privacy Level', 'School Name', 'School Grade', 'Family/Household Mailing Name',
        'Preferred Language', 'Ethnicity', 'Homebound Ministry', 'Allergies', 'Confirmed no allergies',
        'Allergies Privacy Level', 'Commitment Date', 'Commitment Story', 'Current Story', 'My Web Site',
        'Work Web Site', 'Military', 'Service(s) usually attended', 'Plugged In Privacy Level',
        'User Defined - Text 1', 'User Defined - Text 2', 'User Defined - Text 3', 'Pastr When Leav',
        'Pastr When Join', 'Transferred To', 'Transferred Frm', 'Baptized By', 'SK Indiv ID', 'Mailbox Number',
        'User Defined - Date 1', 'FBI Fingerprint', 'PA Criminal Chk', 'PA Child Abuse', 'Confirmed Date',
        'PA Sex Offender Registry', 'Mand Rpt Trng', 'Child/Youth Eml', 'Ethnicity', 'Photo Release', 'Confirmed',
        'Spirit Mailing', 'Custom Field Privacy Level', 'Personality Style', 'Spiritual Gifts', 'Passions',
        'Abilities', 'My Fit Privacy Level', 'Last logged in', 'Spiritual Maturity', 'Child Work Date Start',
        'Child Work Date Stop', 'Other ID', 'Sync ID']

    # --- Ensure df_ilw_transactions is initialized and modified as in get_ilw_data_ORIG.py ---
    df_ilw_transactions = list_to_dataframe(list_of_ilw_transactions)
    df_ilw_transactions['Date'] = pd.to_datetime(df_ilw_transactions['Date'], format='%Y-%m-%d')
    df_ilw_transactions['Transaction ID'] = df_ilw_transactions['Transaction ID'].astype(np.int64)
    df_ilw_transactions['Ind ID'] = df_ilw_transactions['Ind ID'].astype(np.int64)
    df_ilw_transactions['Family ID'] = df_ilw_transactions['Family ID'].astype(np.int64)
    df_ilw_transactions['Batch ID'] = df_ilw_transactions['Batch ID'].astype(np.int64)
    df_ilw_transactions['Amount'] = df_ilw_transactions['Amount'].astype(float)
    df_ilw_transactions.sort_values(by=['Date'], ascending=False, inplace=True)

    list_of_ilw_individuals = preprocess_deceased_individuals(list_of_ilw_individuals)
    df_ilw_individuals = list_to_dataframe(list_of_ilw_individuals, list_of_individual_columns_to_drop)
    df_ilw_individuals['Ind ID'] = df_ilw_individuals['Ind ID'].astype(np.int64)
    df_ilw_individuals['Family ID'] = df_ilw_individuals['Family ID'].astype(np.int64)
    df_ilw_transactions, df_ilw_individuals = drop_or_remap_children_givers(df_ilw_transactions,
        df_ilw_individuals, set_of_giving_individual_ids)
    df_ilw_individuals = merge_down_alternate_name(df_ilw_individuals)

    if before_after_csvs:
        df_ilw_individuals.to_csv(os.path.join(config.prog_dir, 'before_after_csvs', 'individuals_before.csv'),
                                  index=False)
        df_ilw_transactions.to_csv(os.path.join(config.prog_dir, 'before_after_csvs', 'transactions_before.csv'),
                                   index=False)
        logging.debug("Saved 'before' CSVs, prior to overlay and concat.")

    # Overlay and concat
    with pd.ExcelFile(xlsx_input_file) as xlsx:
        df_overlay = pd.read_excel(xlsx, sheet_name='IndividualUpdate')
        df_overlay['Ind ID'] = df_overlay['Ind ID'].astype(np.int64)
        df_overlay['Family ID'] = df_overlay['Family ID'].astype('Int64')
        df_ilw_transactions, set_of_giving_family_ids = map_transaction_fam_ids(df_ilw_transactions,
            df_ilw_individuals, df_overlay, set_of_giving_family_ids)
        df_overlay.set_index('Ind ID', inplace=True)
        df_ilw_individuals.set_index('Ind ID', inplace=True)
        df_ilw_individuals.update(df_overlay, overwrite=True)
        df_ilw_individuals.reset_index(inplace=True)
        df_concat = pd.read_excel(xlsx, sheet_name='IndividualConcat').fillna('')
        df_concat['Ind ID'] = df_concat['Ind ID'].astype(np.int64)
        df_concat['Family ID'] = df_concat['Family ID'].astype(np.int64)
        df_ilw_individuals = pd.concat([df_ilw_individuals, df_concat])
        df_coa_remap = pd.read_excel(xlsx, sheet_name='CoaRemap')
        dict_coa_remap = {row['COA']: row['New COA'] for _, row in df_coa_remap.iterrows()}
        df_matched_transactions = pd.read_excel(xlsx, sheet_name='MatchedTransactions')
        df_ilw_transactions = pd.merge(df_ilw_transactions, df_matched_transactions,
            on='Transaction ID', how='left')
        df_ilw_transactions['Override Fam ID'] = df_ilw_transactions['Override Fam ID'].astype('Int64')
        df_ilw_transactions['Override COA Category'] = df_ilw_transactions['Override COA Category'].astype('string')
        df_ilw_transactions['Family ID'] = np.where(df_ilw_transactions['Override Fam ID'].isnull(),
            df_ilw_transactions['Family ID'], df_ilw_transactions['Override Fam ID'])
        df_ilw_transactions['COA Category'] = np.where(df_ilw_transactions['Override COA Category'].isnull(),
            df_ilw_transactions['COA Category'], df_ilw_transactions['Override COA Category'])
        df_ilw_transactions['Family ID'] = df_ilw_transactions['Family ID'].astype('Int64')
        df_ilw_transactions['COA Category'] = df_ilw_transactions['COA Category'].astype('string')
        df_ilw_transactions_w_new_family_id = df_ilw_transactions.loc[df_ilw_transactions['Family ID'] >= 100000]
        set_of_giving_family_ids.update(df_ilw_transactions_w_new_family_id['Family ID'].tolist())

    df_ilw_individuals.sort_values(by=['Last', 'First'], inplace=True)
    
    # Add Full Name column before Email column
    df_ilw_individuals['Full Name'] = df_ilw_individuals['First'] + ' ' + df_ilw_individuals['Last']
    
    # Add Full Email column after Email column
    df_ilw_individuals['Full Email'] = df_ilw_individuals.apply(
        lambda row: '' if (pd.isna(row['Email']) or row['Email'] == '' or 
                          str(row['First']).startswith('[DECEASED]'))
        else f"{row['Full Name']} <{row['Email']}>", axis=1
    )
    
    # Reorder columns to place Full Name before Email and Full Email after Email
    cols = df_ilw_individuals.columns.tolist()
    email_index = cols.index('Email')
    # Insert Full Name before Email
    cols.insert(email_index, cols.pop(cols.index('Full Name')))
    # Insert Full Email after Email (email_index + 1 because Full Name was inserted before)
    cols.insert(email_index + 2, cols.pop(cols.index('Full Email')))
    df_ilw_individuals = df_ilw_individuals[cols]

    if before_after_csvs:
        df_ilw_individuals.to_csv(os.path.join(config.prog_dir, 'before_after_csvs', 'individuals_after.csv'),
                                  index=False)
        df_ilw_transactions.to_csv(os.path.join(config.prog_dir, 'before_after_csvs', 'transactions_after.csv'),
                                   index=False)
        logging.debug("Saved 'after' CSVs, after overlay and concat.")

    # Family DataFrame
    list_families = [['Family ID', 'Name(s)', 'Full Email(s)', 'Primary ID', 'Spouse ID']]
    mapping_dicts = get_mapping_dicts(df_ilw_individuals)
    for family_id in set_of_giving_family_ids:
        couple_names, couple_emails, first_in_couple, second_in_couple = get_pretty_emails_from_fam(
            family_id, mapping_dicts, df_ilw_individuals)
        list_families.append([family_id, couple_names, couple_emails, first_in_couple, second_in_couple])
    df_ilw_families = list_to_dataframe(list_families)

    # DROP "Couple Email(s)" from "Individuals (CCB Overlaid"" tab
    #df_ilw_individuals = pd.merge(df_ilw_individuals, df_ilw_families[['Family ID', 'Email(s)']],
    #    left_on='Family ID', right_on='Family ID', how='left')
    #df_ilw_individuals.rename(columns={'Email(s)': 'Couple Email(s)'}, inplace=True)

    # !!! Add "Individual Email" here in "XYZ <abc@xyz.com>" format

    # Donations DataFrame
    df_ilw_donations = pd.merge(df_ilw_transactions, df_ilw_individuals, left_on='Ind ID', right_on='Ind ID', how='left')
    df_ilw_donations['COA Category'] = df_ilw_donations['COA Category'].map(dict_coa_remap)
    df_ilw_donations['Couple Emails'] = ''
    df_ilw_donations['Couple Names'] = ''
    reload_names_and_emails(df_ilw_donations, df_ilw_individuals)
    df_ilw_donations['Thank You Note'] = ''
    df_ilw_donations['Assigned Project'] = ''
    df_ilw_donations.rename(columns={'Family ID_x': 'Family ID', 'Mailing Zip_x': 'Mailing Zip',
        'Email': 'Donor Email', 'COA Category': 'Simple COA'}, inplace=True)
    df_ilw_donations = df_ilw_donations[['Date', 'Amount', 'First', 'Last', 'Thank You Note', 'Assigned Project',
        'Simple COA', 'Tax Deductible', 'Payment Type', 'Donor Email', 'Couple Emails', 'Couple Names',
        'Mailing Street', 'Mailing City', 'Mailing State', 'Mailing Zip', 'Home Phone', 'Mobile Phone', 'Ind ID',
        'Family ID', 'Comments']]
    calculate_follow_ups(df_ilw_donations)
    df_ilw_donations.sort_values(by=['Date'], ascending=False, inplace=True)
    df_ilw_donations['Year'] = pd.DatetimeIndex(df_ilw_donations['Date']).year

    # --- Excel Output and Formatting ---
    # Summarize donations by year to create Summary tab
    df_ilw_summary = df_ilw_donations.pivot_table(index=['Family ID'], columns=['Year'],
        values=['Amount'], aggfunc='sum')
    df_ilw_summary.columns = [str(c_list[1]) for c_list in df_ilw_summary.columns.values]
    empty_row = pd.Series([None] * len(df_ilw_summary.columns), dtype='float')
    for fam_id in df_add_families['Family ID'].values:
        df_ilw_summary.loc[fam_id] = empty_row
    df_ilw_sponsorships = df_ilw_donations[df_ilw_donations['Simple COA'] == 'Sponsorships & Tickets']. \
        pivot_table(index=['Family ID'], columns=['Year'], values=['Amount'], aggfunc='sum')
    df_ilw_sponsorships.columns = [str(c_list[1]) for c_list in df_ilw_sponsorships.columns.values]
    df_ilw_sponsorships['All-Time Sponsorships'] = df_ilw_sponsorships.select_dtypes(include=['number']).sum(axis=1)
    if config.datetime_start.month < 5 or (config.datetime_start.month == 5 and config.datetime_start.day < 8):
        last_year = config.datetime_start.year - 1
    else:
        last_year = config.datetime_start.year
    str_last_year = str(last_year)
    str_last_year_rename = str_last_year + ' Sponsorships'
    df_ilw_sponsorships = df_ilw_sponsorships[['All-Time Sponsorships', str_last_year]]
    df_ilw_sponsorships.rename(columns={str_last_year: str_last_year_rename}, inplace=True)
    df_ilw_summary = pd.merge(df_ilw_summary.reset_index(), df_ilw_sponsorships, on='Family ID', how='left')
    df_ilw_summary = pd.merge(df_ilw_summary.reset_index(),
        df_ilw_families, on='Family ID', how='left')

    # Add placeholder columns for Reach-Out XXXX sheet (optional, commented out)
    # df_ilw_summary['Reach-Out Completed'] = 'No'
    # df_ilw_summary['Notes / Status'] = ''
    # df_ilw_summary['Expected Sponsor Amount'] = ''
    # df_ilw_summary['Expected Other Amount'] = ''
    # df_ilw_summary['Paid Amount (updated by Andy)'] = ''
    # df_ilw_summary['Drop from Future Call Lists'] = 'No'

    df_ilw_summary['Lifetime Giving'] = ''
    df_ilw_summary['Last 5 Years Giving'] = ''
    df_ilw_summary = df_ilw_summary.merge(df_ilw_individuals, how='left',
        left_on='Primary ID', right_on='Ind ID')
    df_ilw_summary = df_ilw_summary.drop(columns=['Family ID_y'], axis=1)
    df_ilw_summary = df_ilw_summary.rename(columns={'Family ID_x': 'Family ID'})
    year_columns = ['Lifetime Giving', 'Last 5 Years Giving']
    for i in range(config.curr_year, 2012, -1):
        year_columns.append(str(i))
    df_ilw_summary.rename(columns={'Mobile Phone': 'Primary Mobile Phone'}, inplace=True)
    df_ilw_summary['Spouse Mobile Phone'] = df_ilw_summary.apply(
        lambda row: '' if pd.isnull(row['Spouse ID']) else mapping_dicts.ind2row[row['Spouse ID']]['Mobile Phone'],
        axis=1)
    columns_list = ['Name(s)', 'All-Time Sponsorships', str_last_year_rename] + year_columns + \
        ['Last', 'Full Email(s)', 'Home Phone', 'Primary Mobile Phone', 'Spouse Mobile Phone', 'Mailing Street', \
         'Mailing City', 'Mailing State', 'Mailing Zip', 'Family ID', 'Primary ID', 'Spouse ID']
    df_ilw_summary = df_ilw_summary[columns_list]

    # Add Simple COA column using dict_coa_remap
    df_ilw_transactions['Simple COA'] = df_ilw_transactions['COA Category'].map(dict_coa_remap)
    
    df_ilw_transactions = df_ilw_transactions[['Date', 'Amount', 'Name', 'Ind ID', 'Family ID',
        'Family Position', 'Gender', 'Age', 'Transaction ID', 'Batch ID', 'Batch Name',
        'Transaction Grouping', 'COA Category', 'Simple COA', 'Payment Type', 'Check Number', 'Memo', 'Tax Deductible',
        'Comments']]
    df_ilw_transactions.sort_values(by=['Date'], ascending=False, inplace=True)

    # Create memory-based summary of giving by family and year
    overall_giving = {}
    for index, row in df_ilw_donations.iterrows():
        if not row['Family ID'] in overall_giving:
            overall_giving[row['Family ID']] = {}
        if not row['Year'] in overall_giving[row['Family ID']]:
            overall_giving[row['Family ID']][row['Year']] = {}
        if not row['Simple COA'] in overall_giving[row['Family ID']][row['Year']]:
            overall_giving[row['Family ID']][row['Year']][row['Simple COA']] = 0
        overall_giving[row['Family ID']][row['Year']][row['Simple COA']] += row['Amount']

    # Drop Year column from Donations tab
    df_ilw_donations = df_ilw_donations.drop(columns=['Year'], axis=1)

    # Write to output file
    if xlsx_output_file is not None:
        output_filename = xlsx_output_file
    else:
        # Ensure tmp directory exists
        tmp_dir = os.path.join(config.prog_dir, 'tmp')
        os.makedirs(tmp_dir, exist_ok=True)
        output_filename = os.path.join(tmp_dir, f'ilw_data_{config.datetime_start_string}.xlsx')
    with pd.ExcelWriter(output_filename) as writer:
        df_ilw_summary.to_excel(writer, sheet_name='Summary By Year', index=False)
        df_ilw_donations.to_excel(writer, sheet_name='Donations', index=False)
        df_ilw_individuals.to_excel(writer, sheet_name='Individuals', index=False)
        df_ilw_transactions.to_excel(writer, sheet_name='Transactions', index=False)
        df_ilw_families.to_excel(writer, sheet_name='Families', index=False)

    # Reload workbook for formatting
    workbook = openpyxl.load_workbook(output_filename)

    # Donations sheet formatting
    worksheet = workbook['Donations']
    filters = worksheet.auto_filter
    filters.ref = 'A1:U' + str(worksheet.max_row)
    set_column_number_format(worksheet, 'A', 'm/d/yy')
    set_column_number_format(worksheet, 'B', '$#,##0.00')
    column_widths = {
        'A': 10, 'B': 13, 'C': 18, 'D': 19, 'E': 18, 'F': 19, 'G': 19, 'H': 17, 'I': 17, 'J': 45,
        'K': 110, 'L': 30, 'M': 69, 'N': 19, 'O': 16, 'P': 15, 'Q': 22, 'R': 22, 'S': 10, 'T': 14, 'U': 39
    }
    set_column_widths(worksheet, column_widths)

    # Summary sheet formatting
    worksheet = workbook['Summary By Year']
    filters = worksheet.auto_filter
    filters.ref = 'A1:AD' + str(worksheet.max_row)
    for c in range(ord('B'), ord('B') + len(year_columns)):
        set_column_number_format(worksheet, chr(c), '$#,##0.00')
    column_widths = {
        'A': 37, 'B': 16, 'C': 14, 'D': 12, 'E': 13, 'F': 10, 'G': 10, 'H': 10, 'I': 10, 'J': 10, 'K': 10,
        'L': 10, 'M': 10, 'N': 10, 'O': 10, 'P': 10, 'Q': 10, 'R': 10, 'S': 19, 'T': 108, 'U': 22, 'V': 24,
        'W': 23, 'X': 70, 'Y': 19, 'Z': 17, 'AA': 15, 'AB': 13, 'AC': 14, 'AD': 14
    }
    set_column_widths(worksheet, column_widths)
    # Insert "Lifetime" (column) giving SUM() formula
    for i in range(2, worksheet.max_row+1):
        worksheet['D' + str(i)] = '=SUM(F' + str(i) + ':' + chr(67 + len(year_columns)) + str(i) + ')'
    # Insert "Last 5 Years" (column) giving SUM() formula
    for i in range(2, worksheet.max_row+1):
        worksheet['E' + str(i)] = '=SUM(F' + str(i) + ':' + 'K' + str(i) + ')'

    # Insert giving data as comments in Summary tab
    for fam_id in overall_giving:
        for year in overall_giving[fam_id]:
            comment_string = ''
            for coa_cat in overall_giving[fam_id][year]:
                if comment_string != '':
                    comment_string += '\n'
                comment_string += f'{coa_cat}: {overall_giving[fam_id][year][coa_cat]}'
            # get_cell_loc expects curr_year, so pass config.curr_year
            col_num, row_num = get_cell_loc(df_ilw_summary, fam_id, year, config.curr_year)
            worksheet.cell(row=row_num, column=col_num).comment =  Comment(comment_string, config.prog_name)

    # Wrap column headers for columns B-E
    for col in ['B', 'C', 'D', 'E']:
        new_align = copy(worksheet[col+'1'].alignment)
        new_align.wrap_text = True
        worksheet[col+'1'].alignment = new_align
    # Align header column bottom
    for cell in worksheet["1:1"]:
        new_align = copy(cell.alignment)
        new_align.vertical = 'bottom'
        cell.alignment = new_align

    # Individuals sheet formatting
    worksheet = workbook['Individuals']
    filters = worksheet.auto_filter
    filters.ref = 'A1:AQ' + str(worksheet.max_row)
    column_widths = {
        'A': 11, 'B': 14, 'C': 18, 'D': 11, 'E': 17, 'F': 19, 'G': 17, 'H': 20, 'I': 11, 'J': 33, 'K': 30, 'L': 47, 
        'M': 70, 'N': 20, 'O': 17, 'P': 15, 'Q': 19, 'R': 19, 'S': 21, 'T': 22, 'U': 16, 'V': 22, 'W': 18, 'X': 13, 
        'Y': 16, 'Z': 13, 'AA': 17, 'AB': 28, 'AC': 18, 'AD': 17, 'AE': 22, 'AF': 23, 'AG': 17, 'AH': 18, 'AI': 11, 
        'AJ': 13, 'AK': 25, 'AL': 25, 'AM': 21, 'AN': 13, 'AO': 18, 'AP': 19, 'AQ': 24
    }
    set_column_widths(worksheet, column_widths)
    
    # Color the Full Name and Full Email column headers light blue
    from openpyxl.styles import PatternFill
    light_blue_fill = PatternFill(start_color='ADD8E6', end_color='ADD8E6', fill_type='solid')
    worksheet['J1'].fill = light_blue_fill  # Full Name column
    worksheet['L1'].fill = light_blue_fill  # Full Email column

    # Transactions sheet formatting
    worksheet = workbook['Transactions']
    filters = worksheet.auto_filter
    filters.ref = 'A1:S' + str(worksheet.max_row)
    set_column_number_format(worksheet, 'A', 'm/d/yy')
    set_column_number_format(worksheet, 'B', '$#,##0.00')
    column_widths = {
        'A': 10, 'B': 13, 'C': 33, 'D': 11, 'E': 14, 'F': 18, 'G': 13, 'H': 10, 'I': 18, 'J': 13, 'K': 38, 'L': 23, 
        'M': 59, 'N': 19, 'O': 18, 'P': 18, 'Q': 50, 'R': 18, 'S': 44
    }
    set_column_widths(worksheet, column_widths)
    
    # Color the Simple COA column header light blue
    worksheet['N1'].fill = light_blue_fill  # Simple COA column

    # Families sheet formatting
    worksheet = workbook['Families']
    filters = worksheet.auto_filter
    filters.ref = 'A1:E' + str(worksheet.max_row)
    column_widths = {
        'A': 14, 'B': 38, 'C': 109, 'D': 15, 'E': 15
    }
    set_column_widths(worksheet, column_widths)
    
    # Color the Name(s) and Full Email(s) column headers light blue
    worksheet['B1'].fill = light_blue_fill  # Name(s) column
    worksheet['C1'].fill = light_blue_fill  # Full Email(s) column

    workbook.save(output_filename)
    typer.echo(f'Wrote ILW data to {output_filename}')

if __name__ == "__main__":
    app() 
