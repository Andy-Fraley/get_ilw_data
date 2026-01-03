"""
data_processing.py

Handles all data processing, dataframe, and CSV logic for ILW data.
"""
from typing import List, Optional, Any, Dict, Set, Tuple
import pandas as pd
import numpy as np
import csv
import logging
import os

# Functions to be implemented:
# - list_to_dataframe
# - merge_down_alternate_name
# - drop_or_remap_children_givers
# - etc.

def safe_str(value: Any) -> str:
    """Convert a value to string, treating NaN as empty string."""
    if pd.isna(value):
        return ''
    return str(value)

def get_cell_loc(df_ilw_summary: pd.DataFrame, fam_id: int, year: int, curr_year: int) -> Tuple[int, int]:
    """
    Get the Excel cell location for a family/year in the summary DataFrame.
    """
    index = df_ilw_summary.index[df_ilw_summary['Family ID'] == fam_id]
    assert len(index) == 1, f'Expected 1 matching row but found {len(index)}: {index}'
    row_num = int(index[0]) + 2
    col_num = curr_year - year + 6
    return (col_num, row_num)

def preprocess_deceased_individuals(list_of_ilw_individuals: List[List[Any]]) -> List[List[Any]]:
    """
    Prefix '[DECEASED]' to first names of deceased individuals in the list.
    """
    header = list_of_ilw_individuals[0]
    first_name_index = header.index('First')
    deceased_date_index = header.index('Deceased Date')
    reason_left_church_index = header.index('Reason Left Church')
    for index, individual in enumerate(list_of_ilw_individuals):
        if index == 0:
            continue
        reason_left_church = individual[reason_left_church_index]
        deceased_date = individual[deceased_date_index]
        if reason_left_church == 'Deceased' or not (
            deceased_date is None or deceased_date == '-' or deceased_date == ''):
            first_name = individual[first_name_index]
            list_of_ilw_individuals[index][first_name_index] = '[DECEASED] ' + first_name
    return list_of_ilw_individuals

def map_transaction_fam_ids(df_ilw_transactions: pd.DataFrame, df_ilw_individuals: pd.DataFrame, df_overlay: pd.DataFrame, set_of_giving_family_ids: Set[int]) -> Tuple[pd.DataFrame, Set[int]]:
    """
    Remap family IDs in transactions and update the set of giving family IDs.
    """
    dict_remap_fam_ids = {}
    df_overlay_fam_changes = df_overlay.loc[df_overlay['Family ID'].notnull()]
    df_remap_matrix = pd.merge(df_overlay_fam_changes, df_ilw_individuals, on='Ind ID', how='inner')
    for index, row in df_remap_matrix.iterrows():
        num_in_family = number_family_members(row['Family ID_y'], df_ilw_individuals)
        if num_in_family <= 1:
            dict_remap_fam_ids[row['Family ID_y']] = int(row['Family ID_x'])
            df_ilw_transactions['Family ID'] = df_ilw_transactions['Family ID'].replace(dict_remap_fam_ids)
            for fam_id in dict_remap_fam_ids.keys():
                if fam_id in set_of_giving_family_ids:
                    set_of_giving_family_ids.remove(fam_id)
                    logging.debug(f"After update/concat, removed 'Family ID' {fam_id}")
                if dict_remap_fam_ids[fam_id] not in set_of_giving_family_ids:
                    set_of_giving_family_ids.add(dict_remap_fam_ids[fam_id])
                    logging.debug(f"After update/concat, adding 'Family ID' {fam_id}")
    return (df_ilw_transactions, set_of_giving_family_ids)

def number_family_members(fam_id: int, df_ilw_individuals: pd.DataFrame) -> int:
    return df_ilw_individuals.loc[df_ilw_individuals['Family ID'] == fam_id].shape[0]

def calculate_follow_ups(df_ilw_donations: pd.DataFrame) -> None:
    """
    Mark donations for follow-up based on amount and COA category.
    """
    for index, row in df_ilw_donations.iterrows():
        if float(row['Amount']) >= 100.0:
            thank_you_note = 'TBD'
        else:
            thank_you_note = ''
        if float(row['Amount']) >= 1000.0 or str(row['Simple COA']) == 'Missions : Ingomar Living Waters : Projects':
            assigned_project = 'TBD'
        else:
            assigned_project = ''
        df_ilw_donations.at[index, 'Thank You Note'] = thank_you_note
        df_ilw_donations.at[index, 'Assigned Project'] = assigned_project

def set_column_number_format(worksheet, column: str, number_format: str) -> None:
    cell_range = column + '2:' + column + str(worksheet.max_row)
    apply_to_cell_range(worksheet, cell_range, number_format=number_format)

def set_column_widths(worksheet, column_widths: Dict[str, int]) -> None:
    for key, value in column_widths.items():
        worksheet.column_dimensions[key].width = value

def apply_to_cell_range(worksheet, cell_range: str, border=None, alignment=None, font=None, fill=None, number_format=None) -> None:
    for row in worksheet[cell_range]:
        for cell in row:
            if border is not None:
                cell.border = border
            if alignment is not None:
                cell.alignment = alignment
            if font is not None:
                cell.font = font
            if fill is not None:
                cell.fill = fill
            if number_format is not None:
                cell.number_format = number_format

def reload_names_and_emails(df_ilw_donations: pd.DataFrame, df_ilw_individuals: pd.DataFrame) -> None:
    mapping_dicts = get_mapping_dicts(df_ilw_individuals)
    for index, row in df_ilw_donations.iterrows():
        single_name, single_email, first_in_couple, second_in_couple = get_pretty_emails_from_ind(
            row['Ind ID'], mapping_dicts, df_ilw_individuals, False)
        couple_names, couple_emails, first_in_couple, second_in_couple = get_pretty_emails_from_ind(
            row['Ind ID'], mapping_dicts, df_ilw_individuals, True)
        df_ilw_donations.at[index, 'Email'] = single_email
        df_ilw_donations.at[index, 'Couple Emails'] = couple_emails
        df_ilw_donations.at[index, 'Couple Names'] = couple_names

def list_to_dataframe(list_to_convert: List[List[Any]], list_of_columns_to_drop: Optional[List[str]] = None, name_of_index_column: Optional[str] = None) -> pd.DataFrame:
    """
    Convert a list of lists to a pandas DataFrame, optionally dropping columns and setting index.
    """
    df = pd.DataFrame(list_to_convert[1:], columns=list_to_convert[0])
    if list_of_columns_to_drop is not None:
        df = df.drop(list_of_columns_to_drop, axis='columns')
    if name_of_index_column is not None:
        df.set_index(name_of_index_column, inplace=True)
    return df

def merge_down_alternate_name(df_ilw_individuals: pd.DataFrame) -> pd.DataFrame:
    for i, row in df_ilw_individuals.iterrows():
        if row['Alternate Name'] != '':
            if row['Legal first'] == '':
                df_ilw_individuals.at[i, 'Legal first'] = row['First']
                df_ilw_individuals.at[i, 'First'] = row['Alternate Name']
                df_ilw_individuals.at[i, 'Alternate Name'] = ''
    return df_ilw_individuals.drop(['Alternate Name'], axis='columns')

class MappingDicts:
    def __init__(self):
        self.fam2inds = {}
        self.ind2fam = {}
        self.email2ind = {}
        self.ind2row = {}

def get_mapping_dicts(df_ilw_individuals: pd.DataFrame) -> MappingDicts:
    mapping_dicts = MappingDicts()
    for index, row in df_ilw_individuals.iterrows():
        ind_id = row['Ind ID']
        fam_id = row['Family ID']
        if fam_id not in mapping_dicts.fam2inds:
            mapping_dicts.fam2inds[fam_id] = []
        mapping_dicts.fam2inds[fam_id].append(ind_id)
        mapping_dicts.ind2fam[ind_id] = fam_id
        if pd.notna(row['Email']):
            mapping_dicts.email2ind[row['Email'].lower()] = ind_id
        mapping_dicts.ind2row[ind_id] = row
    return mapping_dicts

def get_pretty_emails_from_ind(ind_id: int, mapping_dicts: MappingDicts, ind_df: pd.DataFrame, include_spouse: bool = True):
    if not include_spouse:
        only_include = ind_id
    else:
        only_include = None
    return get_pretty_emails_from_fam(mapping_dicts.ind2row[ind_id]['Family ID'], mapping_dicts, ind_df, only_include)

def get_pretty_emails_from_fam(fam_id: int, mapping_dicts: MappingDicts, ind_df: pd.DataFrame, only_include: Optional[int] = None):
    first_in_couple = None
    second_in_couple = None
    both_spouses_deceased = False
    slot1 = None
    slot2 = None
    group_name = ''
    group_email = ''
    if fam_id not in mapping_dicts.fam2inds:
        logging.error(f'Family ID {fam_id} specified in NonGivingFamilies tab does not exist in CCB')
        return ('', '', None, None)
    inds_in_family = mapping_dicts.fam2inds[fam_id]
    if only_include is not None:
        reason_left_church = mapping_dicts.ind2row[only_include]['Reason Left Church']
        deceased_date = mapping_dicts.ind2row[only_include]['Deceased Date']
        if reason_left_church != 'Deceased' and \
           (deceased_date is None or deceased_date == '-' or deceased_date ==''):
            first_in_couple=only_include
        else:
            first_in_couple = None
        second_in_couple=None
    else:
        # First look for traditional family (male and female primary and spouse)
        for ind in inds_in_family:
            family_position = mapping_dicts.ind2row[ind]['Family Position']
            gender = mapping_dicts.ind2row[ind]['Gender']
            reason_left_church = mapping_dicts.ind2row[ind]['Reason Left Church']
            deceased_date = mapping_dicts.ind2row[ind]['Deceased Date']
            if family_position == 'Primary Contact' or family_position == 'Spouse': # or family_position == 'Other':
                if gender == 'Male' and reason_left_church != 'Deceased' and \
                       (deceased_date is None or deceased_date == '-' or deceased_date ==''):
                        first_in_couple = ind
                elif gender == 'Female' and reason_left_church != 'Deceased' and \
                       (deceased_date is None or deceased_date == '-' or deceased_date == ''):
                        second_in_couple = ind

        # If not a traditional family, grab the first 'Primary Contact' irregardless of gender
        if first_in_couple is None and second_in_couple is None:
            for ind in inds_in_family:
                family_position = mapping_dicts.ind2row[ind]['Family Position']
                reason_left_church = mapping_dicts.ind2row[ind]['Reason Left Church']
                deceased_date = mapping_dicts.ind2row[ind]['Deceased Date']
                if family_position == 'Primary Contact' and reason_left_church != 'Deceased' and \
                       (deceased_date is None or deceased_date == '-' or deceased_date ==''):
                    first_in_couple = ind
                    break

        # If not an organization (above), grab the first 'Other' irregardless of gender
        if first_in_couple is None and second_in_couple is None:
            for ind in inds_in_family:
                family_position = mapping_dicts.ind2row[ind]['Family Position']
                reason_left_church = mapping_dicts.ind2row[ind]['Reason Left Church']
                deceased_date = mapping_dicts.ind2row[ind]['Deceased Date']
                if family_position == 'Other' and reason_left_church != 'Deceased' and \
                       (deceased_date is None or deceased_date == '-' or deceased_date ==''):
                    first_in_couple = ind
                    logging.debug(f"Spouse deceased, so grabbed 'Other' {ind} with gender " \
                        f"'{mapping_dicts.ind2row[ind]['Gender']}'")
                    break

        # If still cannot find attributable family member, both spouses must be deceased.  In this case,
        # mark the case, allow DECEASED spouses to appear, and then delete their emails later
        if first_in_couple is None and second_in_couple is None:
            both_spouses_deceased = True
            for ind in inds_in_family:
                family_position = mapping_dicts.ind2row[ind]['Family Position']
                gender = mapping_dicts.ind2row[ind]['Gender']
                reason_left_church = mapping_dicts.ind2row[ind]['Reason Left Church']
                deceased_date = mapping_dicts.ind2row[ind]['Deceased Date']
                if family_position == 'Primary Contact' or family_position == 'Spouse':
                    if gender == 'Male':
                        first_in_couple = ind
                    elif gender == 'Female':
                        second_in_couple = ind


    if first_in_couple and second_in_couple:
        if safe_str(mapping_dicts.ind2row[first_in_couple]['Last']) == safe_str(mapping_dicts.ind2row[second_in_couple]['Last']):
            if safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == '':
                slot1 = None
            else:
                slot1 = \
                    safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + '* & ' + \
                    safe_str(mapping_dicts.ind2row[second_in_couple]['First']) + ' ' + \
                    safe_str(mapping_dicts.ind2row[first_in_couple]['Last']) + ' <' + \
                    safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) + '>'
            if only_include is None:
                if safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == \
                   safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) or \
                   safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == '' or \
                   safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) == '':
                    if safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == '':
                        email = safe_str(mapping_dicts.ind2row[second_in_couple]['Email'])
                    elif safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) == '':
                        email = safe_str(mapping_dicts.ind2row[first_in_couple]['Email'])
                    elif safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == \
                         safe_str(mapping_dicts.ind2row[second_in_couple]['Email']):
                        email = safe_str(mapping_dicts.ind2row[first_in_couple]['Email'])
                    else:
                        assert(False)
                    if email == '':
                        slot1 = None
                    else:
                        slot1 = \
                            safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + ' & ' + \
                            safe_str(mapping_dicts.ind2row[second_in_couple]['First']) + ' ' + \
                            safe_str(mapping_dicts.ind2row[first_in_couple]['Last']) + ' <' + \
                            email + '>'
                else:
                    if safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) == '':
                        slot2 = None
                    else:
                        slot2 = \
                            safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + ' & ' + \
                            safe_str(mapping_dicts.ind2row[second_in_couple]['First']) + '* ' + \
                            safe_str(mapping_dicts.ind2row[second_in_couple]['Last']) + ' <' + \
                            safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) + '>'
            group_name = safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + ' & ' + \
                        safe_str(mapping_dicts.ind2row[second_in_couple]['First']) + ' ' + \
                        safe_str(mapping_dicts.ind2row[first_in_couple]['Last'])

        else:
            if safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == '':
                slot1 = None
            else:
                slot1 = \
                    safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + '* ' + \
                    safe_str(mapping_dicts.ind2row[first_in_couple]['Last']) + ' & ' + \
                    safe_str(mapping_dicts.ind2row[second_in_couple]['First']) + ' ' + \
                    safe_str(mapping_dicts.ind2row[second_in_couple]['Last']) + ' <' + \
                    safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) + '>'
            if only_include is None:
                if safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == \
                   safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) or \
                   safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == '' or \
                   safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) == '':
                    if safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == '':
                        email = safe_str(mapping_dicts.ind2row[second_in_couple]['Email'])
                    elif safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) == '':
                        email = safe_str(mapping_dicts.ind2row[first_in_couple]['Email'])
                    elif safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == \
                         safe_str(mapping_dicts.ind2row[second_in_couple]['Email']):
                        email = safe_str(mapping_dicts.ind2row[first_in_couple]['Email'])
                    else:
                        assert(False)
                    if email == '':
                        slot1 = None
                    else: 
                        slot1 = \
                            safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + ' ' + \
                            safe_str(mapping_dicts.ind2row[first_in_couple]['Last']) + ' & ' + \
                            safe_str(mapping_dicts.ind2row[second_in_couple]['First']) + ' ' + \
                            safe_str(mapping_dicts.ind2row[second_in_couple]['Last']) + ' <' + \
                            email + '>'
                else:
                    if safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) == '':
                        slot2 = None
                    else:
                        slot2 = \
                            safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + ' ' + \
                            safe_str(mapping_dicts.ind2row[first_in_couple]['Last']) + ' & ' + \
                            safe_str(mapping_dicts.ind2row[second_in_couple]['First']) + '* ' + \
                            safe_str(mapping_dicts.ind2row[second_in_couple]['Last']) + ' <' + \
                            safe_str(mapping_dicts.ind2row[second_in_couple]['Email']) + '>'

            group_name = safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + ' ' + \
                        safe_str(mapping_dicts.ind2row[first_in_couple]['Last']) + ' & ' + \
                        safe_str(mapping_dicts.ind2row[second_in_couple]['First']) + ' ' + \
                        safe_str(mapping_dicts.ind2row[second_in_couple]['Last'])

    elif first_in_couple or second_in_couple:
        if second_in_couple:
            first_in_couple = second_in_couple
            second_in_couple = None
        if safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) == '':
            slot1 = None
        else:
            slot1 = \
                safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + ' ' + \
                safe_str(mapping_dicts.ind2row[first_in_couple]['Last']) + ' <' + \
                safe_str(mapping_dicts.ind2row[first_in_couple]['Email']) + '>'
        group_name = safe_str(mapping_dicts.ind2row[first_in_couple]['First']) + ' ' + \
            safe_str(mapping_dicts.ind2row[first_in_couple]['Last'])

    if slot1 is not None:
        if slot2 is not None:
            group_email = delete_bad_chars(slot1) + ', ' + delete_bad_chars(slot2)
        else:
            group_email = delete_bad_chars(slot1)
    else:
        if slot2 is not None:
            group_email = delete_bad_chars(slot2)
        else:
            group_email = ''

    if both_spouses_deceased:
        group_email = ''

    return (delete_bad_chars(group_name), group_email, first_in_couple, second_in_couple)

def delete_bad_chars(s: str) -> str:
    return s.replace(',', '')

def drop_or_remap_children_givers(df_ilw_transactions: pd.DataFrame, df_ilw_individuals: pd.DataFrame, set_of_giving_individual_ids: Set[int]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    indices_to_drop = set()
    indices_to_remap = {}
    for index, row in df_ilw_individuals.iterrows():
        if row['Family Position'] == 'Child':
            ind_id = row['Ind ID']
            if ind_id in set_of_giving_individual_ids:
                if ind_id not in indices_to_remap:
                    indices_to_remap[ind_id] = row['Family ID']
            else:
                if ind_id not in indices_to_drop:
                    indices_to_drop.add(ind_id)
    for index, row in df_ilw_transactions.iterrows():
        ind_id = row['Ind ID']
        if ind_id in indices_to_remap:
            primary_parent_id = get_primary_parent_id(indices_to_remap[ind_id], df_ilw_individuals)
            assert primary_parent_id is not None, f"'Child' with Individual ID {ind_id} made a contribution and there is not parent to map it to."
            logging.debug(f"Replacing child {ind_id} (index {index}) with parent {primary_parent_id}")
            df_ilw_transactions.at[index, 'Ind ID'] = primary_parent_id
            if ind_id not in indices_to_drop:
                indices_to_drop.add(ind_id)
    true_indices = []
    for index in indices_to_drop:
        ind_id_to_drop = df_ilw_individuals[df_ilw_individuals['Ind ID'] == index].index.values[0]
        true_indices.append(ind_id_to_drop)
    df_ilw_individuals.drop(true_indices, inplace=True)
    return (df_ilw_transactions, df_ilw_individuals)

def get_primary_parent_id(family_id: int, df_ilw_individuals: pd.DataFrame) -> Optional[int]:
    df_family_members = df_ilw_individuals[df_ilw_individuals['Family ID'] == family_id]
    if not df_family_members.empty:
        for index, row in df_family_members.iterrows():
            if row['Family Position'] == 'Primary Contact':
                return row['Ind ID']
    return None

def write_list_of_items_to_csv(list_of_items: List[Any], filename: str) -> None:
    with open(filename, 'w') as csv_output_file:
        csv_writer = csv.writer(csv_output_file)
        for item in list_of_items:
            csv_writer.writerow(item)

def get_dict_by_individuals(list_of_individuals: List[List[Any]]) -> Dict[Any, Dict[str, Any]]:
    dict_by_individuals = {}
    header_row = list_of_individuals[0]
    ind_id_index = header_row.index('Ind ID')
    for individual in list_of_individuals[1:]:
        for column_name in header_row:
            if individual[ind_id_index] not in dict_by_individuals:
                dict_by_individuals[individual[ind_id_index]] = {}
            dict_by_individuals[individual[ind_id_index]][column_name] = individual[header_row.index(column_name)]
    return dict_by_individuals

def get_dict_by_families(list_of_individuals: List[List[Any]]) -> Dict[Any, Dict[Any, Dict[str, Any]]]:
    dict_by_families = {}
    header_row = list_of_individuals[0]
    ind_id_index = header_row.index('Ind ID')
    fam_id_index = header_row.index('Family ID')
    for individual in list_of_individuals[1:]:
        for column_name in header_row:
            if individual[fam_id_index] not in dict_by_families:
                dict_by_families[individual[fam_id_index]] = {}
            if individual[ind_id_index] not in dict_by_families[individual[fam_id_index]]:
                dict_by_families[individual[fam_id_index]][individual[ind_id_index]] = {}
            dict_by_families[individual[fam_id_index]][individual[ind_id_index]][column_name] = individual[header_row.index(column_name)]
    return dict_by_families

def get_lists_from_file(prog_dir: str) -> Tuple[Set[int], Set[int], List[List[Any]], List[List[Any]]]:
    """
    Load cached family IDs, individual IDs, transactions, and individuals from CSV files in file_cache.
    """
    set_of_giving_family_ids = set()
    set_of_giving_individual_ids = set()
    list_of_ilw_transactions = []
    list_of_ilw_individuals = []
    with open(os.path.join(prog_dir, 'file_cache/fam_ids.csv')) as csvfile:
        header = False
        for row in csv.reader(csvfile):
            if not header:
                header = True
                continue
            set_of_giving_family_ids.add(int(row[0]))
    with open(os.path.join(prog_dir, 'file_cache/ind_ids.csv')) as csvfile:
        header = False
        for row in csv.reader(csvfile):
            if not header:
                header = True
                continue
            set_of_giving_individual_ids.add(int(row[0]))
    with open(os.path.join(prog_dir, 'file_cache/transactions.csv')) as csvfile:
        for row in csv.reader(csvfile):
            list_of_ilw_transactions.append(row)
    with open(os.path.join(prog_dir, 'file_cache/individuals.csv')) as csvfile:
        for row in csv.reader(csvfile):
            list_of_ilw_individuals.append(row)
    return (set_of_giving_family_ids, set_of_giving_individual_ids, list_of_ilw_transactions, list_of_ilw_individuals)

def write_lists_to_file(prog_dir: str, set_of_giving_family_ids: Set[int], set_of_giving_individual_ids: Set[int], list_of_ilw_transactions: List[List[Any]], list_of_ilw_individuals: List[List[Any]]) -> None:
    """
    Write family IDs, individual IDs, transactions, and individuals to CSV files in file_cache.
    """
    with open(os.path.join(prog_dir, 'file_cache/fam_ids.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Family ID'])
        for item in set_of_giving_family_ids:
            writer.writerow([item])
    with open(os.path.join(prog_dir, 'file_cache/ind_ids.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Individual ID'])
        for item in set_of_giving_individual_ids:
            writer.writerow([item])
    with open(os.path.join(prog_dir, 'file_cache/transactions.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(list_of_ilw_transactions)
    with open(os.path.join(prog_dir, 'file_cache/individuals.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(list_of_ilw_individuals) 