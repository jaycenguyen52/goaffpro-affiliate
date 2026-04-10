"""
================================================================================
  check_email_approval.py  v3.0
  Chi fetch email co keyword APPROVE/REJECT/VERIF trong subject
  Khong fetch tat ca 1000+ email nua

  CHAY:
    python check_email_approval.py

  SCHEDULE (3 phut):
    python loop check_email_approval.py 3m
================================================================================
"""

import sys
import io
import re
from datetime import datetime, timezone, timedelta

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import imaplib
import email
import email.utils
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from email.header import decode_header

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

SERVICE_ACCOUNT_JSON = "credentials.json"
SPREADSHEET_ID       = "1pgYxEiW-W1nYJaeSkOb7Hkweipa1dsJ8U_jdpJTUi0E"
PROFILES_SHEET_NAME  = "Profiles"
LINKS_SHEET_NAME     = "Links"

GMAIL_IMAP_HOST      = "imap.gmail.com"
GMAIL_IMAP_PORT      = 993
CHECK_HOURS_BACK     = 2
DRY_RUN              = False

# ═══════════════════════════════════════════════════════════════════════════════
# MAU SAC
# ═══════════════════════════════════════════════════════════════════════════════

C_RESET   = "\033[0m"
C_RED     = "\033[91m"
C_GREEN   = "\033[92m"
C_YELLOW  = "\033[93m"
C_CYAN    = "\033[96m"
C_BOLD    = "\033[1m"

def _p(msg, color="", bold=False):
    p = f"{C_BOLD}{color}" if bold else color
    print(f"{p}{msg}{C_RESET}")

def log(msg):   _p(f"[INFO]  {msg}", C_CYAN)
def ok(msg):    _p(f"[OK]    {msg}", C_GREEN, bold=True)
def warn(msg):  _p(f"[WARN]  {msg}", C_YELLOW)
def err(msg):   _p(f"[ERROR] {msg}", C_RED, bold=True)

# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEET
# ═══════════════════════════════════════════════════════════════════════════════

def get_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_JSON, scopes=scope)
    return gspread.authorize(creds)

def read_profiles(client) -> dict:
    ss = client.open_by_key(SPREADSHEET_ID)
    sheet = ss.worksheet(PROFILES_SHEET_NAME)
    raw = sheet.get_all_values()
    data = {}
    for row in raw:
        if len(row) >= 2:
            key = str(row[0]).strip()
            val = str(row[1]).strip() if row[1] else ""
            if key:
                data[key] = val
    return data

def read_pending_links(client) -> list[tuple[int, str, str]]:
    """Doc sheet Links → tra ve cac row co Status = Pending."""
    ss = client.open_by_key(SPREADSHEET_ID)
    ws = ss.worksheet(LINKS_SHEET_NAME)
    raw = ws.get_all_values()
    pending = []
    for i, row in enumerate(raw):
        if i == 0:
            continue
        if len(row) < 7:
            continue
        # Brand=col0(A) SignUpLink=col5(F) Status=col6(G)
        brand = row[0].strip()
        signup_link = row[5].strip()
        status = row[6].strip()
        if signup_link.startswith("http") and status.upper() == "PENDING":
            pending.append((i + 1, brand, signup_link))
    return pending

def update_link_status(client, row_num: int, status: str, error_msg: str = ""):
    if DRY_RUN:
        log(f"[DRY] Row {row_num} -> {status}: {error_msg}")
        return
    ss = client.open_by_key(SPREADSHEET_ID)
    ws = ss.worksheet(LINKS_SHEET_NAME)
    # Status=col7 ErrMsg=col11
    ws.update_cell(row_num, 7, status)
    if error_msg:
        ws.update_cell(row_num, 11, error_msg)

# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _decode(raw) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            try:
                result.append(part.decode(enc or "utf-8", errors="replace"))
            except Exception:
                result.append(part.decode("utf-8", errors="replace"))
        else:
            result.append(part)
    return "".join(result)

def _extract_brand(sender: str) -> str:
    """Extract brand tu sender, VD: 'Hair Shop <no-reply@goaffpro.com>' → 'hairshop'"""
    name, addr = email.utils.parseaddr(sender)
    if name:
        return re.sub(r"[^a-z0-9]", "", name.lower())
    return addr.split("@")[0].lower()

def _get_body(msg) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try:
                    charset = part.get_content_charset() or "utf-8"
                    return part.get_payload(decode=True).decode(charset, errors="replace")
                except Exception:
                    pass
    else:
        try:
            charset = msg.get_content_charset() or "utf-8"
            return msg.get_payload(decode=True).decode(charset, errors="replace")
        except Exception:
            pass
    return ""

# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL PARSING
# ═══════════════════════════════════════════════════════════════════════════════

# Chi can keyword APPROVE/REJECT/VERIF (khong phan biet hoa thuong)
APPROVE_KWS_RE = re.compile(
    r"approved|su cuenta ha sido aprobada|votre compte a été approuvé|"
    r"uw account is goedgekeurd|ihr konto wurde genehmigt|"
    r"il tuo account è stato approvato|conta foi aprovada|"
    r"account has been approved|aprobada",
    re.IGNORECASE
)
REJECT_KWS_RE = re.compile(
    r"rejected|declined|denied|"
    r"application has been rejected|"
    r"ihr affiliate-konto| compte affiliate| blocked",
    re.IGNORECASE
)
VERIFY_KWS_RE = re.compile(
    r"verify|verifique|verifier|verifica|überprüfen|bestaätigen",
    re.IGNORECASE
)

def parse_goaffpro_email(subject: str, sender: str, body: str) -> tuple[str, str, str]:
    """Tra ve (brand, status, detail)."""
    subject_lower = subject.lower()
    brand = _extract_brand(sender)

    if APPROVE_KWS_RE.search(subject_lower):
        return brand, "Approved", subject
    if REJECT_KWS_RE.search(subject_lower):
        return brand, "Rejected", subject
    if VERIFY_KWS_RE.search(subject_lower):
        return brand, "Pending", f"[Verify] {subject}"
    return brand, "Pending", subject

# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL FETCHING — CHI LAY EMAIL CO KEYWORD TRONG SUBJECT
# ═══════════════════════════════════════════════════════════════════════════════

# IMAP search chi lay email co subject chua keyword
IMAP_SUBJECT_SEARCH = 'SUBJECT "approved" OR SUBJECT "rejected" OR SUBJECT "verify" OR SUBJECT "verifique" OR SUBJECT "verifier"'


def fetch_goaffpro_emails(
    gmail_email: str,
    app_password: str,
    hours_back: int = CHECK_HOURS_BACK,
) -> list[tuple[str, str, str]]:
    """
    Chi fetch email tu goaffpro.com co subject chua approve/reject/verify.
    Tra ve [(brand, status, subject)].
    """
    if not gmail_email or not app_password:
        return []

    try:
        log("Ket noi Gmail...")
        mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST, GMAIL_IMAP_PORT)
        mail.login(gmail_email, app_password)
        mail.select('INBOX')

        # Lay cutoff date
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        cutoff_str = cutoff.strftime("%d-%b-%Y")

        # Tim email tu goaffpro.com trong khoang thoi gian
        search_query = f'FROM goaffpro.com SINCE "{cutoff_str}"'

        status, msg_ids = mail.search(None, search_query)
        if status != "OK":
            mail.logout()
            return []

        all_ids = msg_ids[0].split()
        if not all_ids:
            mail.logout()
            return []

        log(f"Tim thay {len(all_ids)} email GoAffPro trong {hours_back}h")

        results = []
        for mid in all_ids:
            try:
                # Chi fetch header
                status, msg_data = mail.fetch(mid, '(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)])')
                if status != "OK":
                    continue

                raw_headers = msg_data[0][1]
                if isinstance(raw_headers, bytes):
                    raw_str = raw_headers.decode("utf-8", errors="replace")
                else:
                    raw_str = str(raw_headers)

                headers = {}
                for line in raw_str.split("\r\n"):
                    if ": " in line:
                        key, val = line.split(": ", 1)
                        headers[key.strip().lower()] = val.strip()

                raw_from = headers.get("from", "")
                raw_subject = headers.get("subject", "")

                sender = _decode(raw_from)
                subject = _decode(raw_subject)
                subject_lower = subject.lower()

                # Loc: chi lay email co keyword trong subject
                if not (
                    APPROVE_KWS_RE.search(subject_lower) or
                    REJECT_KWS_RE.search(subject_lower) or
                    VERIFY_KWS_RE.search(subject_lower)
                ):
                    continue

                brand, email_status, detail = parse_goaffpro_email(subject, sender, "")
                results.append((brand, email_status, detail))

            except Exception:
                continue

        mail.logout()
        return results

    except Exception as e:
        log(f"Loi IMAP: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    _p("=" * 55, C_BOLD)
    _p("  GoAffPro Email Checker v3.0", C_CYAN, bold=True)
    _p("=" * 55, C_BOLD)

    if DRY_RUN:
        warn("[DRY RUN] Chi in, khong ghi sheet!")

    log("Ket noi Google Sheet...")
    try:
        client = get_client()
        ok("OK!")
    except Exception as e:
        err(f"Loi: {e}")
        return

    profile = read_profiles(client)
    gmail_email    = profile.get("Gmail", "")
    gmail_password = profile.get("GmailPassword", "")

    if not gmail_email or not gmail_password:
        err("Khong co Gmail/GmailPassword!")
        return

    ok(f"Gmail: {gmail_email}")

    # ── Pending links ────────────────────────────────────────────────────────────
    pending_links = read_pending_links(client)
    if not pending_links:
        ok("Khong co link Pending — Thoat.")
        return

    # Tao dict: brand_norm -> (row_num, original_brand)
    brand_to_row: dict[str, tuple[int, str]] = {}
    for row_num, brand, link in pending_links:
        brand_norm = re.sub(r"[^a-z0-9]", "", brand.lower())
        if brand_norm:
            brand_to_row[brand_norm] = (row_num, brand)
        log(f"  Row {row_num}: [{brand}] | {link}")

    ok(f"{len(pending_links)} link Pending, {len(brand_to_row)} brand de check")

    # ── Fetch email ─────────────────────────────────────────────────────────────
    email_results = fetch_goaffpro_emails(gmail_email, gmail_password, hours_back=CHECK_HOURS_BACK)
    if not email_results:
        warn("Khong co email approve/reject/verify moi — van Pending.")
        return

    ok(f"Tim thay {len(email_results)} email lien quan:")
    for brand, status, detail in email_results[:15]:
        log(f"  [{status}] {brand}: {detail[:70]}")
    if len(email_results) > 15:
        log(f"  ... +{len(email_results) - 15} email nua")

    # ── Map brand -> (status, detail) ───────────────────────────────────────────
    brand_map: dict[str, tuple[str, str]] = {}
    for brand, email_status, detail in email_results:
        brand_norm = re.sub(r"[^a-z0-9]", "", brand.lower())
        if not brand_norm:
            continue
        if brand_norm not in brand_map:
            brand_map[brand_norm] = (email_status, detail)
        else:
            old_status, _ = brand_map[brand_norm]
            if email_status in ("Approved", "Rejected") and old_status == "Pending":
                brand_map[brand_norm] = (email_status, detail)

    # ── Cap nhat sheet ──────────────────────────────────────────────────────────
    approved_count = 0
    rejected_count = 0
    still_pending  = 0

    for brand_norm, (row_num, original_brand) in brand_to_row.items():
        if brand_norm in brand_map:
            email_status, detail = brand_map[brand_norm]
            if email_status in ("Approved", "Rejected"):
                update_link_status(client, row_num, email_status, detail)
                if email_status == "Approved":
                    ok(f"Row {row_num} [{original_brand}]: APPROVED!")
                    approved_count += 1
                else:
                    warn(f"Row {row_num} [{original_brand}]: REJECTED!")
                    rejected_count += 1
            else:
                still_pending += 1
        else:
            still_pending += 1

    # ── Ket qua ────────────────────────────────────────────────────────────────
    _p(f"\n{'─' * 55}", C_BOLD)
    _p(f"  APPROVED:  {approved_count}", C_GREEN, bold=True)
    _p(f"  REJECTED:  {rejected_count}", C_RED)
    _p(f"  PENDING:   {still_pending}", C_YELLOW)
    _p(f"{'─' * 55}", C_BOLD)
    ok("Hoan thanh!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        err("Dung boi Ctrl+C")
        sys.exit(0)
    except Exception as e:
        err(f"Loi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
