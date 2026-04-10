import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    "c:/Users/nguye/sign-up projects goaffpro/credentials.json", scope
)
client = gspread.authorize(creds)

# Doc sheet Profiles
spreadsheet_id = "1pgYxEiW-W1nYJaeSkOb7Hkweipa1dsJ8U_jdpJTUi0E"
profiles_sheet = client.open_by_key(spreadsheet_id).worksheet("Profiles")
rows = profiles_sheet.get_all_values()

print("=== PROFILES SHEET ===")
for i, row in enumerate(rows):
    a = row[0] if len(row) > 0 else ""
    b = row[1] if len(row) > 1 else ""
    print(f"  Row {i+1}: '{a}' = '{b}'")

print(f"\nTong cong {len(rows)} rows")
