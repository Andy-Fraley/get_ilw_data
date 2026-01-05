# ILW Data Processing System - Code Analysis Notes

## Overview
This is a Python application for processing "Ingomar Living Waters" (ILW) donation data from a CCB (Church Community Builder) church management system. The system extracts transaction and individual data via CCB's API, processes and transforms the data, and outputs formatted Excel reports for analysis and follow-up activities.

## Project Structure

### Core Files
- `__init__.py` - Empty package initializer
- `cli.py` - Main command-line interface and processing pipeline (402 lines)
- `config.py` - Configuration dataclass (21 lines)
- `ccb_api.py` - CCB API interaction functions (177 lines)
- `data_processing.py` - Data transformation and processing logic (486 lines)
- `email_utils.py` - Email notification utilities (35 lines)
- `logging_utils.py` - Logging configuration and filters (46 lines)
- `models.py` - Data models and enums (11 lines)
- `util.py` - Utility functions for CCB login and REST API calls (195 lines)

## Detailed File Analysis

### 1. `cli.py` - Main Application Entry Point
**Purpose**: Command-line interface using Typer framework that orchestrates the entire data processing pipeline.

**Key Functions**:
- `process()` - Main command with options for input file, caching, email notifications, logging level, and project assignments retrieval
- Handles vault-based credential management for CCB and Gmail access
- Processes Excel input files with multiple sheets (IndividualUpdate, IndividualConcat, CoaRemap, etc.)
- Retrieves `project_assignments.xlsx` from remote source before processing
- Generates comprehensive Excel output with multiple formatted sheets

**Command-Line Options**:
- `--xlsx-input-file` - Path to input Excel file (defaults to Input.xlsx)
- `--xlsx-output-file` - Path for output Excel file (defaults to timestamped file in tmp/)
- `--use-file-cache` - Use cached data instead of CCB API
- `--no-email` - Skip email notifications
- `--logging-level` - Set logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--before-after-csvs` - Generate before/after CSV snapshots
- `--get-now` - Force fresh retrieval of project_assignments.xlsx from remote source (ignores 24-hour cache)

**Data Flow**:
1. Load configuration and credentials from vault
2. Retrieve `project_assignments.xlsx` from remote source via `pull_latest_project_assignments.sh`
3. Extract data from CCB API or file cache
4. Process and transform DataFrames
5. Apply overlays and concatenations from input Excel
6. Generate family groupings and donation summaries
7. Create formatted Excel output with multiple sheets

**Output Sheets**:
- Summary By Year - Pivot table of donations by family/year
- Original Donations - Individual donation records with follow-up flags (before recharacterization)
- Recharacterized Donations - Donation records after applying project assignment recharacterizations
- Individuals - Person records with manual updates applied
- Transactions - Raw transaction data
- Families - Family contact information

### 2. `config.py` - Configuration Management
**Purpose**: Dataclass holding runtime configuration and state.

**Key Fields**:
- Datetime tracking (start time, formatted strings)
- Gmail credentials for notifications
- CCB API credentials (username, password, subdomain)
- Program metadata (name, directory, current year)
- String stream for log aggregation

### 3. `ccb_api.py` - CCB API Integration
**Purpose**: Handles all interactions with Church Community Builder API.

**Key Functions**:
- `get_list_of_ilw_transactions()` - Retrieves donation transactions for ILW-related Chart of Accounts categories
- `get_list_of_ilw_individuals()` - Gets individual/family data for donors
- Uses session-based authentication via `util.login()`
- Processes data year-by-year to handle large datasets
- Filters transactions by specific COA categories related to Living Waters ministry

**ILW COA Categories**:
- Ingomar Living Waters, Living Water General Donation, Auctions, Projects, Sponsorships & Tickets, Water Filters, and various legacy categories

### 4. `data_processing.py` - Data Transformation Engine
**Purpose**: Core data processing, DataFrame operations, and business logic.

**Key Functions**:
- `safe_str()` - Helper function that converts values to strings, treating NaN as empty string (handles `<None>` replacements)
- `preprocess_deceased_individuals()` - Marks deceased individuals with [DECEASED] prefix
- `drop_or_remap_children_givers()` - Maps child donations to parents, removes non-giving children
- `merge_down_alternate_name()` - Handles preferred vs legal names
- `calculate_follow_ups()` - Flags donations requiring thank you notes (≥$100) or project assignments (≥$1000)
- `get_pretty_emails_from_fam()` - Complex logic for generating family email strings with proper formatting (uses `safe_str()` to handle NaN values)
- `get_mapping_dicts()` - Builds lookup dictionaries for individuals/families (skips NaN email values)
- `reload_names_and_emails()` - Updates donation records with formatted family contact info
- File caching system for avoiding repeated API calls

**Complex Logic Areas**:
- Family email formatting handles various scenarios (couples with same/different last names, deceased spouses, single individuals)
- Child donation remapping ensures gifts are attributed to parents
- Excel cell location calculations for comment placement
- NaN value handling throughout to support `<None>` property clearing in IndividualUpdate tab

### 5. `email_utils.py` - Email Notifications
**Purpose**: Gmail-based email notifications for administrators.

**Key Functions**:
- `send_email()` - Generic email sending via Gmail SMTP
- `send_admin_email()` - Sends completion/error notifications to configured recipient
- Uses Gmail app passwords for authentication

### 6. `logging_utils.py` - Logging Infrastructure
**Purpose**: Multi-handler logging system with filtering.

**Key Features**:
- File logging (messages.log)
- Console logging with configurable levels
- String stream logging for email aggregation
- `EmailFilter` class captures only relevant messages for notifications
- Supports DEBUG, INFO, WARNING, ERROR, CRITICAL levels

### 7. `models.py` - Data Models
**Purpose**: Defines enums and data structures.

**Contents**:
- `LoggingLevel` enum for logging configuration

### 8. `util.py` - Utility Functions
**Purpose**: Shared utilities, primarily for CCB API interactions.

**Key Functions**:
- `login()` - Authenticates with CCB web interface using session cookies
- `ccb_rest_xml_to_temp_file()` - Downloads CCB REST API responses with retry logic
- `get_errors_from_rest_xml()` - Parses XML error responses
- Legacy functions for INI-based configuration (appears to be from older version)

## Data Processing Pipeline

### Input Sources
1. **Project Assignments File**: Retrieved from Google Drive via `pull_latest_project_assignments.sh`
   - Location: `input/project_assignments.xlsx`
   - Contains project assignment data for augmenting donation processing
   - Uses 24-hour caching by default; force refresh with `--get-now` flag
   - Integrated with `/Users/afraley/Documents/src/sh/pull_latest_ilw_data/` utilities
2. **CCB API**: Transaction and individual data via authenticated sessions
3. **Excel Input File**: Manual overrides and configurations with sheets:
   - **IndividualUpdate**: Manual corrections to individual records
     - Use `<None>` in any cell to clear/delete that property value in the output
     - Empty cells leave the original CCB value unchanged
     - `<None>` values are converted to blank cells after the overlay merge
   - **IndividualConcat**: Additional individuals to include
   - **CoaRemap**: Chart of Accounts category remapping
   - **MatchedTransactions**: Transaction overrides
   - **NonGivingFamilies**: Families to include even without donations

### Processing Steps
1. **Project Assignments Retrieval**: Fetch latest `project_assignments.xlsx` from Google Drive
   - Uses shared cache directory with other ILW utilities
   - Respects 24-hour freshness check unless `--get-now` is specified
   - Logs INFO message upon successful retrieval
2. **Data Extraction**: Pull transactions and individuals from CCB API or cache
3. **Data Cleaning**: Handle deceased individuals, remap child donations
4. **Manual Overlays**: Apply Excel-based corrections and additions
5. **Donation Recharacterization**: Apply project assignment recharacterizations
   - Parses "Project Assignments" tab from `project_assignments.xlsx`
   - Matches donations using format: `Last-First-YYYYMMDD-$Amount-COA_Abbrev`
   - Ignores `*AUTO MATCH*` entries and donations already categorized as Projects (P)
   - For non-Projects donations (GD, WF, S&T) that need recharacterization:
     - **Full recharacterization**: If recharacterization amount equals donation amount, changes COA to "Projects"
     - **Partial recharacterization**: If recharacterization amount is less than donation amount, splits into two rows (one for remaining original COA, one for Projects portion)
   - Validates that recharacterization amounts don't exceed donation amounts
   - Validates total amounts match between Original and Recharacterized Donations tabs
   - Logs errors for unmatched recharacterization entries
   - Supported COA abbreviations: P (Projects), WF (Water Filters), GD (General Donation), S&T (Sponsorships & Tickets)
6. **Family Grouping**: Generate family contact information and email formatting
7. **Donation Analysis**: Calculate follow-up requirements and yearly summaries
8. **Excel Generation**: Create formatted multi-sheet workbook with formulas and comments

### Output Features
- Auto-filtering on all sheets
- Custom column formatting (dates, currency)
- Embedded formulas for lifetime and recent giving calculations
- Comments showing donation breakdowns by category
- Follow-up flags for thank you notes and project assignments

## Security & Configuration
- Credentials stored in Ansible Vault encrypted file
- Vault password in separate `.secrets_*` file
- Gmail app passwords for email notifications
- CCB subdomain, username, password configuration

## Caching System
- File-based caching in `file_cache/` directory
- Stores family IDs, individual IDs, transactions, and individuals as CSV files
- Allows development/testing without repeated API calls

## Business Logic Notes
- Focuses on "Living Waters" ministry donation tracking
- Handles complex family structures (couples, deceased spouses, single individuals)
- Automatically flags high-value donations for follow-up
- Supports manual overrides for data corrections
- Generates yearly giving summaries for fundraising analysis

## Dependencies
- pandas, numpy: Data processing
- openpyxl: Excel file generation and formatting
- typer: Command-line interface
- requests: HTTP API calls
- ansible-vault: Credential encryption
- smtplib: Email notifications

## Usage Pattern
Typically run periodically (monthly/quarterly) to:
1. Retrieve latest project assignments from Google Drive
2. Extract latest donation data from CCB
3. Apply any manual corrections via Excel input
4. Generate formatted reports for ministry leadership
5. Send email notifications upon completion

### Example Usage

**Standard run (uses cached project_assignments.xlsx if less than 24 hours old)**:
```bash
./get_ilw_data.sh
```

**Force fresh project assignments retrieval**:
```bash
./get_ilw_data.sh --get-now
```

**With custom output file and logging**:
```bash
./get_ilw_data.sh --get-now --xlsx-output-file report.xlsx --logging-level INFO
```

**Use cached CCB data (for testing)**:
```bash
./get_ilw_data.sh --use-file-cache --logging-level DEBUG
```
