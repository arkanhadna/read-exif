import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

# 1. Read the data from Google Sheets
def read_google_sheet(sheet_id, worksheet_name, creds_json):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)
    df_sheet = get_as_dataframe(worksheet, evaluate_formulas=True)
    df_sheet = df_sheet.dropna(how='all')  # Remove empty rows
    return df_sheet

# 2. Compare the pulled data with df1 from main(). Return new rows (by UniqueID)
def get_new_rows(local_df, sheet_df):
    sheet_unique_id = set(sheet_df['UniqueID'].dropna().astype(str))
    new_rows = local_df[~local_df['UniqueID'].astype(str).isin(sheet_unique_id)]
    return new_rows

# 3. Append new rows to Google Sheets data
def append_rows_to_sheet(sheet_id, worksheet_name, creds_json, new_rows):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)
    # Get current data
    df_sheet = get_as_dataframe(worksheet, evaluate_formulas=True)
    df_sheet = df_sheet.dropna(how='all')
    # Concatenate
    updated_df = pd.concat([df_sheet, new_rows], ignore_index=True)
    # 4. Clear the data in Google Sheets
    worksheet.clear()
    # 5. Push the new data to Google Sheets
    set_with_dataframe(worksheet, updated_df)
    return updated_df