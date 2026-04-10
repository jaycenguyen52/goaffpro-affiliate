"""
================================================================================
  goaffpro_auto_register.py  v4.1
  Auto-register GoAffPro affiliates via Genlogin + Dynamic Form Detection

  SETUP:
    pip install playwright gspread oauth2client requests
    playwright install chromium

  CHAY:
    python goaffpro_auto_register.py
================================================================================
"""

import asyncio
import io
import os
import random
import sys
from datetime import datetime

# ── UTF-8 encoding cho Windows console ────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
if os.name == "nt":
    try:
        os.system("")
    except Exception:
        pass

# ── Third-party ───────────────────────────────────────────────────────────────
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

SERVICE_ACCOUNT_JSON = "credentials.json"
SPREADSHEET_ID      = "1pgYxEiW-W1nYJaeSkOb7Hkweipa1dsJ8U_jdpJTUi0E"
PROFILES_SHEET_NAME = "Profiles"
LINKS_SHEET_NAME    = "Links"

# Genlogin
GENLOGIN_BASE   = "http://localhost:55550"
GENLOGIN_EMAIL  = "mikevu999@gmail.com"
GENLOGIN_PASS   = "vippro@123"

MIN_DELAY = 1.5
MAX_DELAY = 4.0

# ── Email check sau khi register (noi trong script) ────────────────────────
GMAIL_IMAP_HOST = "imap.gmail.com"
GMAIL_IMAP_PORT = 993
EMAIL_CHECK_DELAY_MIN  = 3   # doi truoc lan check 1 (phut)
EMAIL_CHECK_RETRY_MIN = 3   # doi truoc lan check 2 (phut)
EMAIL_CHECK_MAX_TRIES = 2   # so lan check toi da
CHECK_HOURS_BACK      = 2   # chi check email trong 2h gan nhat (link moi)

MAX_LINKS = 5        # test thu 5 links truoc, xong roi tang len

# ═══════════════════════════════════════════════════════════════════════════════
# MAU SAC CONSOLE
# ═══════════════════════════════════════════════════════════════════════════════

C_RESET   = "\033[0m"
C_RED     = "\033[91m"
C_GREEN   = "\033[92m"
C_YELLOW  = "\033[93m"
C_BLUE    = "\033[94m"
C_MAGENTA = "\033[95m"
C_CYAN    = "\033[96m"
C_BOLD    = "\033[1m"


def _p(msg, color="", bold=False):
    p = f"{C_BOLD}{color}" if bold else color
    print(f"{p}{msg}{C_RESET}")


def log(msg):   _p(f"[INFO]  {msg}", C_CYAN)
def ok(msg):    _p(f"[OK]    {msg}", C_GREEN, bold=True)
def warn(msg):  _p(f"[WARN]  {msg}", C_YELLOW)
def err(msg):   _p(f"[ERROR] {msg}", C_RED, bold=True)
def step(msg):  _p(f"[STEP]  {msg}", C_MAGENTA, bold=True)


# ═══════════════════════════════════════════════════════════════════════════════
# GENLOGIN
# ═══════════════════════════════════════════════════════════════════════════════

def genlogin_auth() -> str:
    """Dang nhap Genlogin, tra ve access_token."""
    res = requests.post(
        f"{GENLOGIN_BASE}/backend/auth/login",
        json={"username": GENLOGIN_EMAIL, "password": GENLOGIN_PASS},
        timeout=10,
    )
    res.raise_for_status()
    return res.json()["data"]["access_token"]


def genlogin_find_profile(token: str, email_keyword: str, profile: dict):
    """
    Tim Genlogin profile theo profile_data.name = email_keyword.
    Strategy:
      1. Proxy match: lay proxy host tu sheet Profiles → tim profile co proxy trung khop
      2. Exact name match: duyet pages tim profile_data.name == email
      3. Substring name match: tim profile_data.name chua email keyword
    Tra ve profile_id hoac None.
    """
    import urllib.parse
    headers = {"Authorization": f"Bearer {token}"}
    email_lower = email_keyword.lower()

    # Lay proxy host tu profile (row 1 trong sheet Profiles)
    proxy_str = profile.get("Proxy", "") if profile else ""
    proxy_host = proxy_str.split(":")[0] if proxy_str else ""

    # Duyet pages
    for page_num in range(1, 100):
        res = requests.get(
            f"{GENLOGIN_BASE}/backend/profiles?limit=500&page={page_num}",
            headers=headers, timeout=15,
        )
        if res.status_code != 200:
            break
        items = res.json().get("data", {})
        if isinstance(items, dict):
            items = items.get("items", [])
        elif not isinstance(items, list):
            items = []
        if not items:
            break
        for p in items:
            name  = (p.get("profile_data", {}).get("name") or "").strip()
            proxy = p.get("profile_data", {}).get("proxy", {})
            p_host = (proxy.get("host", "") or "") if isinstance(proxy, dict) else ""

            # Proxy match (uu tien 1)
            if proxy_host and p_host == proxy_host:
                return p["id"]
            # Exact name match (uu tien 2)
            if name.lower() == email_lower:
                return p["id"]

    # Thu substring match
    for page_num in range(1, 100):
        res = requests.get(
            f"{GENLOGIN_BASE}/backend/profiles?limit=500&page={page_num}",
            headers=headers, timeout=15,
        )
        if res.status_code != 200:
            break
        items = res.json().get("data", {})
        if isinstance(items, dict):
            items = items.get("items", [])
        elif not isinstance(items, list):
            items = []
        if not items:
            break
        for p in items:
            name = (p.get("profile_data", {}).get("name") or "").strip()
            if email_lower in name.lower():
                return p["id"]

    # Fallback: lay profile dau tien cua mikevu999@gmail.com
    for page_num in range(1, 100):
        res = requests.get(
            f"{GENLOGIN_BASE}/backend/profiles?limit=500&page={page_num}",
            headers=headers, timeout=15,
        )
        if res.status_code != 200:
            break
        items = res.json().get("data", {})
        if isinstance(items, dict):
            items = items.get("items", [])
        elif not isinstance(items, list):
            items = []
        if not items:
            break
        for p in items:
            sub_email = (
                p.get("profile_metadata", {})
                .get("sub_account", {})
                .get("sub_user_email", "")
            )
            if sub_email and sub_email.lower() == email_lower:
                return p["id"]

    # Emergency fallback: tra ve profile dau tien co sub_account = mikevu999
    for page_num in range(1, 100):
        res = requests.get(
            f"{GENLOGIN_BASE}/backend/profiles?limit=500&page={page_num}",
            headers=headers, timeout=15,
        )
        if res.status_code != 200:
            break
        items = res.json().get("data", {})
        if isinstance(items, dict):
            items = items.get("items", [])
        elif not isinstance(items, list):
            items = []
        if not items:
            break
        for p in items:
            sub_email = (
                p.get("profile_metadata", {})
                .get("sub_account", {})
                .get("sub_user_email", "")
            )
            if sub_email:
                return p["id"]

    return None


def genlogin_start(token: str, profile_id: int) -> dict:
    """Start profile, tra ve wsEndpoint."""
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.put(
        f"{GENLOGIN_BASE}/backend/profiles/{profile_id}/start",
        headers=headers, timeout=15,
    )
    res.raise_for_status()
    return res.json()["data"]


def genlogin_stop(token: str, profile_id: int):
    """Stop Genlogin profile."""
    headers = {"Authorization": f"Bearer {token}"}
    requests.put(
        f"{GENLOGIN_BASE}/backend/profiles/{profile_id}/stop",
        headers=headers, timeout=10,
    )


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
    """Doc sheet Profiles (col A=key, col B=value) → dict."""
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
    log(f"Doc {len(data)} truong tu sheet Profiles")
    return data


def read_links(client) -> list[tuple[int, str, str]]:
    """Doc sheet Links (col A=URL, col B=Status), tra ve [(row_number, url, status), ...]."""
    ss = client.open_by_key(SPREADSHEET_ID)
    ws = ss.worksheet(LINKS_SHEET_NAME)
    raw = ws.get_all_values()

    # Tao / kiem tra header chuan 11 cot
    # Header dung: Brand|Domain|Link Check Ads|Currency|Commission|SignUpLink|Status|Registered Email|Account Link|Registered At|Error Message
    expected_header = ["Brand", "Domain", "Link Check Ads", "Currency", "Commission",
                        "SignUpLink", "Status", "Registered Email", "Account Link",
                        "Registered At", "Error Message"]
    if not raw or raw[0][0].strip() != "Brand":
        ws.update(values=[expected_header], range_name="A1:K1")
        raw = ws.get_all_values()

    links = []
    for i, row in enumerate(raw):
        if i == 0:
            continue
        # Brand=col0(A) Domain=col1(B) LinkCheckAds=col2(C) Currency=col3(D)
        # Commission=col4(E) SignUpLink=col5(F) Status=col6(G)
        brand       = row[0].strip() if len(row) > 0 else ""
        signup_link = row[5].strip() if len(row) > 5 else ""
        status      = row[6].strip() if len(row) > 6 else ""
        if signup_link and signup_link.startswith("http"):
            links.append((i + 1, brand, signup_link, status))   # row, brand, url, status
    return links


def update_result(client, row_num: int, result: dict):
    """Ghi ket qua vao Google Sheet tai dong row_num."""
    ss = client.open_by_key(SPREADSHEET_ID)
    ws = ss.worksheet(LINKS_SHEET_NAME)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status    = result.get("status", "Failed")
    email_r   = result.get("email", "")
    acc_link  = result.get("account_link", "")
    err_msg   = result.get("error", "")

    # Status=col7 Registered Email=col8 Account Link=col9 RegAt=col10 ErrMsg=col11
    ws.update_cell(row_num, 7, status)
    ws.update_cell(row_num, 8, email_r)
    ws.update_cell(row_num, 9, acc_link)
    ws.update_cell(row_num, 10, now)
    ws.update_cell(row_num, 11, err_msg)


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL CHECK — GOAFFPRO
# ═══════════════════════════════════════════════════════════════════════════════

import imaplib
import email
import re
import email.utils
from email.header import decode_header
from datetime import datetime, timezone, timedelta


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


APPROVE_RE = re.compile(
    r"approved|su cuenta ha sido aprobada|votre compte a été approuvé|"
    r"uw account is goedgekeurd|ihr konto wurde genehmigt|"
    r"il tuo account è stato approvato|conta foi aprovada",
    re.IGNORECASE
)
REJECT_RE = re.compile(
    r"rejected|declined|denied|application has been rejected",
    re.IGNORECASE
)
VERIFY_RE = re.compile(
    r"verify|verifique|verifier|verifica|überprüfen",
    re.IGNORECASE
)


def _check_brand_email(
    gmail_email: str,
    app_password: str,
    brand_norm: str,
    hours_back: int = CHECK_HOURS_BACK,
) -> tuple[str, str]:
    """
    Check email cho 1 brand cu the.
    Tra ve (status, subject):
      'Approved'  — co email approved
      'Rejected'   — co email rejected
      'Pending'    — chua co email
    """
    if not gmail_email or not app_password:
        return "Pending", "Khong co Gmail"

    try:
        mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST, GMAIL_IMAP_PORT)
        mail.login(gmail_email, app_password)
        mail.select('INBOX')

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        cutoff_str = cutoff.strftime("%d-%b-%Y")

        # Chi lay email tu goaffpro.com trong khoang thoi gian
        status, msg_ids = mail.search(None, f'FROM goaffpro.com SINCE "{cutoff_str}"')
        if status != "OK":
            mail.logout()
            return "Pending", "IMAP search loi"

        all_ids = list(reversed(msg_ids[0].split()))
        if not all_ids:
            mail.logout()
            return "Pending", "Khong co email GoAffPro"

        for mid in all_ids:
            try:
                status, msg_data = mail.fetch(mid, '(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT)])')
                if status != "OK":
                    continue
                raw_headers = msg_data[0][1]
                raw_str = raw_headers.decode("utf-8", errors="replace") if isinstance(raw_headers, bytes) else str(raw_headers)
                headers = {}
                for line in raw_str.split("\r\n"):
                    if ": " in line:
                        k, v = line.split(": ", 1)
                        headers[k.strip().lower()] = v.strip()

                sender = _decode(headers.get("from", ""))
                subject = _decode(headers.get("subject", ""))
                subject_lower = subject.lower()

                # Kiem tra brand co trong sender khong
                email_brand = _extract_brand(sender)
                email_brand_norm = re.sub(r"[^a-z0-9]", "", email_brand)

                if brand_norm != email_brand_norm:
                    continue

                # Kiem tra keyword trong subject
                if APPROVE_RE.search(subject_lower):
                    mail.logout()
                    return "Approved", subject
                if REJECT_RE.search(subject_lower):
                    mail.logout()
                    return "Rejected", subject
                if VERIFY_RE.search(subject_lower):
                    # Co verify nhung chua approved/rejected → van Pending
                    continue

            except Exception:
                continue

        mail.logout()
        return "Pending", "Chua co email approve/reject"

    except Exception as e:
        return "Pending", f"Loi: {e}"


async def _wait_and_check_email(
    gmail_email: str,
    app_password: str,
    brand_norm: str,
    row_num: int,
    client,
) -> tuple[str, str]:
    """
    DOI + CHECK email cho 1 brand.
    Check toi da EMAIL_CHECK_MAX_TRIES lan, moi lan cach nhau EMAIL_CHECK_DELAY_MIN / EMAIL_CHECK_RETRY_MIN.
    Tra ve (final_status, message).
    """
    delays = [EMAIL_CHECK_DELAY_MIN * 60]
    if EMAIL_CHECK_MAX_TRIES >= 2:
        delays.append(EMAIL_CHECK_RETRY_MIN * 60)

    for attempt, delay_sec in enumerate(delays, 1):
        step(f"  Lan {attempt}: doi {delay_sec // 60} phut roi check email...")
        await asyncio.sleep(delay_sec)

        email_status, email_msg = _check_brand_email(
            gmail_email, app_password, brand_norm, hours_back=CHECK_HOURS_BACK
        )
        log(f"  Lan {attempt}: {email_status} — {email_msg}")

        if email_status in ("Approved", "Rejected"):
            # Cap nhat sheet ngay
            ss = client.open_by_key(SPREADSHEET_ID)
            ws = ss.worksheet(LINKS_SHEET_NAME)
            ws.update_cell(row_num, 2, email_status)
            ws.update_cell(row_num, 6, email_msg)
            return email_status, email_msg

    # Het luot check → van Pending
    return "Pending", "Da check 2 lan, chua co email tu Brand"


# ═══════════════════════════════════════════════════════════════════════════════
# CLOUDFLARE TURNSTILE
# ═══════════════════════════════════════════════════════════════════════════════

async def handle_turnstile(page: Page) -> bool:
    """
    Tim va xu ly Cloudflare Turnstile (an + hien, tat ca kich thuoc).
    Thu tu:
      1. Token da co san
      2. frame_locator click (CDP, khong can visible)
      3. mouse.click() tren iframe bbox
      4. JavaScript dispatchEvent (neu iframe an)
      5. Doc iframe content + submit token thu cong
      6. Cho token xuat hien
    Tra ve True neu OK hoac khong co Turnstile.
    """
    # ── Helper: check token ───────────────────────────────────────────────
    async def _has_token() -> bool:
        try:
            return await page.evaluate(r"""
                () => {
                    const h = document.querySelector('input[name="cf-turnstile-response"]');
                    if (h && h.value && h.value.length > 0) return true;
                    const all = document.querySelectorAll('input[type="hidden"]');
                    for (const x of all) {
                        if (x.value && x.value.length > 50 &&
                            (x.name.toLowerCase().includes('turnstile') ||
                             x.id.toLowerCase().includes('turnstile'))) return true;
                    }
                    return false;
                }
            """)
        except Exception:
            return False

    # ── Buoc 1: Token da co san ───────────────────────────────────────────
    if await _has_token():
        ok("Turnstile da xong (token co san)")
        return True

    # ── Buoc 2: Tim tat ca iframe lien quan (khong loc theo kich thuoc) ───
    all_iframes = page.locator("iframe")
    try:
        cnt = await all_iframes.count()
    except Exception:
        cnt = 0

    # Chi lay iframe CF/Turnstile, bo creative ad nhung van giu Tat ca
    # iframe nho (Turnstile invisible) — khong loc theo kich thuoc
    ts_handles = []   # (iframe_locator_handle, src, box_or_none)
    for idx in range(cnt):
        try:
            iframe_el = all_iframes.nth(idx)
            src = await iframe_el.get_attribute("src") or ""
            # Chi loc theo src (CF/Turnstile), khong loc theo size
            is_cf = any(x in src for x in (
                "turnstile", "cloudflare-challenge",
                "challenges.cloudflare", "cdn-cgi",
            ))
            if not is_cf:
                continue
            box = await iframe_el.bounding_box()
            # Bo creative ad nhung giu Tat ca iframe CF (an hay hien)
            if "creatives.goaffpro.com" in src:
                log(f"  [SKIP creative ad] {src[-60:]}")
                continue
            ts_handles.append((iframe_el, src, box))
        except Exception:
            pass

    if not ts_handles:
        log("Khong co iframe Turnstile nao")
        return True

    log(f"Tim thay {len(ts_handles)} iframe Turnstile:")
    for _, src, box in ts_handles:
        b_str = f"{box['width']:.0f}x{box['height']:.0f}" if box else "no-bbox"
        log(f"  {src[-80:]} [{b_str}]")

    # ── Buoc 3: Proactive — thu tu nhien click vao body/overlay trang ─────
    # Mot so Turnstile tu dong verify khi nguoi dung tuong tac
    for _ in range(3):
        try:
            await page.mouse.move(
                400 + random.uniform(-100, 100),
                300 + random.uniform(-50, 50),
            )
            await asyncio.sleep(0.5)
        except Exception:
            pass

    # ── Buoc 4: Thu click tung iframe (frame_locator + mouse) ──────────────
    for idx, (iframe_el, src, box) in enumerate(ts_handles):
        log(f"Dang xu ly Turnstile iframe #{idx}...")

        # 4a. frame_locator → click checkbox ben trong iframe (CDP, khong can visible)
        cb_selectors = [
            "input[type='checkbox']",
            ".ctp-checkbox",
            "[aria-checked='false']",
            "[aria-checked='true']",
            "[role='checkbox']",
            ".challenge-button",
            "#turnstile-container input",
            "[class*='turnstile'] input",
            ".cf-turnstile-widget input",
            "#cf-turnstile-response-widget",
            "[data-turnstile-widget]",
            ".bg-success",
            "div[aria-label*='verified']",
        ]
        try:
            fl = page.frame_locator(iframe_el)
            for sel in cb_selectors:
                try:
                    cb_el = fl.locator(sel).first
                    await cb_el.wait_for(state="attached", timeout=3000)
                    # Doc text cua element truoc
                    try:
                        txt = (await cb_el.inner_text()).strip()[:40]
                        log(f"  Iframe #{idx} [{sel}]: text='{txt}'")
                    except Exception:
                        pass
                    await cb_el.click(timeout=3000)
                    ok(f"  Da click iframe #{idx} [{sel}]!")
                    await asyncio.sleep(2)
                    if await _has_token():
                        ok("Token xuat hien sau frame_locator click!")
                        return True
                except PlaywrightTimeout:
                    pass
                except Exception:
                    pass
        except Exception as e:
            log(f"  frame_locator iframe #{idx} loi: {e}")

        # 4b. mouse.click() tren bbox (neu co)
        if box and box["width"] > 0 and box["height"] > 0:
            try:
                cx = box["x"] + box["width"] / 2
                cy = box["y"] + box["height"] * 0.65
                await page.mouse.move(
                    cx + random.uniform(-10, 10),
                    cy + random.uniform(-5, 5),
                )
                await asyncio.sleep(random.uniform(0.3, 0.8))
                await page.mouse.click(cx, cy)
                ok(f"  Da mouse.click() iframe #{idx} ({box['width']:.0f}x{box['height']:.0f})")
                await asyncio.sleep(2)
                if await _has_token():
                    ok("Token xuat hien sau mouse.click()!")
                    return True
            except Exception as e:
                log(f"  mouse.click() iframe #{idx} loi: {e}")
        else:
            # 4c. Iframe an hoan toan (0x0) — thu scrollIntoView + click
            for attempt in range(2):
                try:
                    await iframe_el.evaluate(
                        "el => el.style.cssText += '; visibility:visible !important; display:block !important; opacity:1 !important; position:fixed !important; z-index:999999 !important; left:0 !important; top:0 !important; width:300px !important; height:200px !important;'"
                    )
                    await asyncio.sleep(1)
                    box2 = await iframe_el.bounding_box()
                    if box2 and box2["width"] > 0 and box2["height"] > 0:
                        cx = box2["x"] + box2["width"] / 2
                        cy = box2["y"] + box2["height"] * 0.65
                        await page.mouse.click(cx, cy)
                        ok(f"  Da mouse.click() (after style inject) iframe #{idx}")
                        await asyncio.sleep(2)
                        if await _has_token():
                            return True
                    await asyncio.sleep(1)
                except Exception as e:
                    log(f"  style-inject iframe #{idx} loi: {e}")

    # ── Buoc 5: Doc iframe content — lay sitekey + widget-id de submit thu cong ─
    try:
        sitekey_info = await page.evaluate(r"""
            () => {
                // Tim Turnstile widget data
                const result = {};
                // Tim data-sitekey attribute
                const el = document.querySelector('[data-sitekey]')
                    || document.querySelector('[data-cf-turnstile]')
                    || document.querySelector('.cf-turnstile')
                    || document.querySelector('[class*="turnstile"]');
                if (el) {
                    result.sitekey = el.getAttribute('data-sitekey')
                        || el.getAttribute('data-cf-turnstile');
                    result.widgetId = el.getAttribute('data-widget-id')
                        || el.getAttribute('data-sitekey');
                    result.className = el.className;
                    result.id = el.id;
                }
                // Tim iframe src co chua sitekey
                const iframes = document.querySelectorAll('iframe');
                for (const ifr of iframes) {
                    const src = ifr.src || '';
                    if (src.includes('turnstile') || src.includes('cloudflare')) {
                        const m = src.match(/sitekey=([^&]+)/);
                        if (m) result.sitekey = m[1];
                        result.iframeSrc = src.slice(0, 200);
                    }
                }
                return result;
            }
        """)
        if sitekey_info and sitekey_info.get("sitekey"):
            log(f"  Tim thay sitekey: {sitekey_info['sitekey']}")
            log(f"  iframeSrc: {sitekey_info.get('iframeSrc','')}")
        elif ts_handles:
            # Thu doc content cua iframe dau tien
            iframe_el = ts_handles[0][0]
            try:
                fl = page.frame_locator(iframe_el)
                body_content = await fl.locator("body").inner_text(timeout=3000)
                log(f"  Iframe content: {body_content[:200]}")
            except Exception:
                pass
    except Exception as e:
        log(f"  sitekey detection loi: {e}")

    # ── Buoc 6: Cho token xuat hien (30s) ────────────────────────────────
    log("Cho Turnstile xu ly (toi da 30s)...")
    try:
        await page.wait_for_function(
            r"""
            () => {
                const h = document.querySelector('input[name="cf-turnstile-response"]');
                if (h && h.value && h.value.length > 0) return true;
                const all = document.querySelectorAll('input[type="hidden"]');
                for (const x of all) {
                    if (x.value && x.value.length > 50 &&
                        (x.name.toLowerCase().includes('turnstile') ||
                         x.id.toLowerCase().includes('turnstile'))) return true;
                }
                return false;
            }
            """,
            timeout=30,
        )
        ok("Turnstile OK!")
        return True
    except PlaywrightTimeout:
        warn("Turnstile cho qua 30s")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# GMAIL IMAP — DOC EMAIL TU PROFILES SHEET
# ═══════════════════════════════════════════════════════════════════════════════

import imaplib
import email
from email.header import decode_header
import time


def _decode_header_value(raw: bytes) -> str:
    """Decode email header value (subject, from)."""
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



# ═══════════════════════════════════════════════════════════════════════════════
# REGISTER ONE LINK
# ═══════════════════════════════════════════════════════════════════════════════

async def register_one(link: str, profile: dict, context, pw, ws_endpoint, browser) -> dict:
    """
    Mo link → Turnstile → SCAN → MAP → DIEN → SUBMIT → KET QUA.
    Su dung page da co san (Genlogin browser context).
    Tra ve dict. CDP browser co the bi reconnect neu page chet.
    """
    result = {
        "status":        "Failed",
        "email":         profile.get("Email", ""),
        "account_link":  "",
        "registered_at": "",
        "error":         "",
    }

    page = None
    try:
        step(f"Mo: {link}")

        # ── Dong new-tab-page / profile-picker neu co, lay page chinh ───────
        existing_pages = list(context.pages)
        for pg in existing_pages:
            url = pg.url or ""
            if any(x in url for x in ("new-tab-page", "profile-picker", "newtab", "new_tab")):
                try:
                    await pg.close()
                except Exception:
                    pass

        # Su dung page dau tien con lai, hoac tao page moi
        if context.pages:
            page = context.pages[0]
        else:
            # Khong tao page moi vi Genlogin CDP khong ho tro
            # Thay vao do: reconnect browser
            try:
                await browser.disconnect()
            except Exception:
                pass
            new_browser = await pw.chromium.connect_over_cdp(ws_endpoint)
            context = new_browser.contexts[0] if new_browser.contexts else await new_browser.new_context()
            await asyncio.sleep(2)
            if context.pages:
                page = context.pages[0]
            else:
                page = None

        # ── Mo trang ────────────────────────────────────────────────────────
        await page.goto(link, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(random.uniform(1.5, 2.5))

        # ── Kiem tra Cloudflare challenge truoc ────────────────────────────
        cloudflare_blocked = await page.evaluate(r"""
            () => {
                // Cloudflare challenge overlay hoac doi redirect
                const title = document.title || '';
                const body  = document.body ? document.body.innerText.slice(0, 200) : '';
                const url   = window.location.href;
                if (url.includes('cloudflare') || url.includes('challenge')) return true;
                if (title.includes('Just a moment') || body.includes('Checking your browser')) return true;
                const overlay = document.querySelector('#cf-challenge-hidden, .cf-overlay, #challenge-body');
                if (overlay) return true;
                return false;
            }
        """)
        if cloudflare_blocked:
            log("Cloudflare challenge — cho doi...")
            await asyncio.sleep(8)

        # ── Cho form san sang (co retry neu bi redirect / Cloudflare) ────────
        log("Cho form san sang...")
        form_found = False
        for attempt in range(3):
            try:
                # Tim input thuc su trong form (khong phai Cloudflare overlay)
                await page.wait_for_selector(
                    "input:not([type='hidden']):not([disabled])",
                    timeout=20,
                )
                # Xac nhan khong phai cloudflare
                is_clear = await page.evaluate(r"""
                    () => {
                        const body = document.body ? document.body.innerText.slice(0,300) : '';
                        if (body.includes('Checking your browser')) return false;
                        if (body.includes('Just a moment')) return false;
                        return document.querySelector('form, .affiliate-form, [class*="register"]') !== null
                            || document.querySelectorAll('input:not([type="hidden"])').length > 0;
                    }
                """)
                if is_clear:
                    form_found = True
                    break
            except PlaywrightTimeout:
                pass

            # Thu click vao trang de unlock Cloudflare
            try:
                await page.mouse.click(400, 300)
            except Exception:
                pass
            await asyncio.sleep(3)

            # Thu lai
            try:
                await page.wait_for_selector(
                    "input:not([type='hidden']):not([disabled])",
                    timeout=15,
                )
                form_found = True
                break
            except PlaywrightTimeout:
                pass

        if not form_found:
            result["error"] = "Form timeout"
            err("Form timeout!")
            return result

        log("Form san sang!")
        await asyncio.sleep(random.uniform(0.5, 1.0))

        # ═══════════════════════════════════════════════════════════════════════
        # BUOC 1 — SCAN FORM
        # ═══════════════════════════════════════════════════════════════════════
        log("Scan form tren trang...")

        try:
            form_data = await page.evaluate(r"""
                () => {
                    // Xac dinh label goc (chua strip *)
                    const getRawLabel = (input) => {
                        if (input.id) {
                            const l = document.querySelector('label[for="' + CSS.escape(input.id) + '"]');
                            if (l) return l.textContent.trim();
                        }
                        const td = input.closest('td');
                        if (td && td.previousElementSibling) {
                            return td.previousElementSibling.textContent.trim();
                        }
                        const wrapper = input.closest('[class*="field"], [class*="input"], [class*="group"]');
                        if (wrapper) {
                            const lbl = wrapper.querySelector('label, span, p, b');
                            if (lbl) return lbl.textContent.trim().replace(input.value || '', '').trim();
                        }
                        return '';
                    };

                    const getSelectRawLabel = (sel) => {
                        if (sel.id) {
                            const l = document.querySelector('label[for="' + CSS.escape(sel.id) + '"]');
                            if (l) return l.textContent.trim();
                        }
                        const td = sel.closest('td');
                        if (td && td.previousElementSibling) {
                            return td.previousElementSibling.textContent.trim();
                        }
                        const wrapper = sel.closest('[class*="field"], [class*="input"], [class*="group"]');
                        if (wrapper) {
                            const lbl = wrapper.querySelector('label, span, p, b');
                            if (lbl) return lbl.textContent.trim().replace(sel.value || '', '').trim();
                        }
                        return '';
                    };

                    const inputs = Array.from(document.querySelectorAll(
                        'input:not([type="hidden"]):not([type="submit"])'
                    ));
                    const selects = Array.from(document.querySelectorAll('select'));
                    const fields = [];

                    for (const input of inputs) {
                        if (!input.offsetParent && input.type !== 'hidden') continue;
                        const id      = input.id || '';
                        const name    = input.name || '';
                        const type    = input.type || 'text';
                        const rawLabel = getRawLabel(input);
                        const label    = rawLabel || '';
                        const ph       = input.placeholder || '';
                        const aria     = input.getAttribute('aria-label') || '';
                        // Label hien thi (khong co *)
                        const displayLabel = label.replace(/\s*\*+\s*/g, '').trim();
                        const isRequired   = rawLabel.includes('*');
                        // Prefix: @, https://, fb.com/, linkedin.com/, youtube.com/@, etc.
                        const prefix = displayLabel || ph || '';
                        fields.push({
                            id, name, type, rawLabel, label: displayLabel,
                            placeholder: ph, ariaLabel: aria,
                            isRequired,
                            prefix,
                            el: `INPUT#${id}[name=${name}]`,
                        });
                    }

                    for (const sel of selects) {
                        if (!sel.offsetParent) continue;
                        const id        = sel.id || '';
                        const name      = sel.name || '';
                        const rawLabel  = getSelectRawLabel(sel);
                        const label     = (rawLabel || '').replace(/\s*\*+\s*/g, '').trim();
                        const isRequired = rawLabel.includes('*');
                        fields.push({
                            id, name, type: 'select', rawLabel, label,
                            placeholder: sel.getAttribute('aria-label') || '',
                            ariaLabel: '', isRequired,
                            prefix: label,
                            el: `SELECT#${id}[name=${name}]`,
                        });
                    }

                    return fields;
                }
            """)
        except Exception as e:
            warn(f"Scan form loi: {e}")
            form_data = []

        # In tat ca fields
        for i, f in enumerate(form_data):
            req = "(*)" if f.get("isRequired") else ""
            log(f"  [{i}] {req} type={f['type']:8s} label='{f['label']}' "
                f"ph='{f['placeholder']}' id='{f['id']}' name='{f['name']}'")

        if not form_data:
            result["error"] = "Khong phat hien truong nao"
            err("Scan form that bai!")
            return result

        # ═══════════════════════════════════════════════════════════════════════
        # BUOC 2 — MAP TRUONG
        # ═══════════════════════════════════════════════════════════════════════
        field_assignments = {}   # {el: (value, ftype, index)}
        assigned_types = set()

        def try_assign(el, ftype, sheet_key, idx):
            val = profile.get(sheet_key, "")
            if val and ftype not in assigned_types:
                field_assignments[el] = (val, ftype, idx)
                assigned_types.add(ftype)
                return True
            return False

        for idx, f in enumerate(form_data):
            lbl    = f["label"].lower()
            raw    = f["rawLabel"].lower()
            ph     = f["placeholder"].lower()
            ids_nm = (f["id"] + " " + f["name"]).lower()

            # ── 1. Exact label match (exact label text) ─────────────────────
            exact = lbl.strip()

            if exact == "email" or exact == "email *":
                try_assign(f["el"], "email", "Email", idx)
            elif exact == "password" or exact == "password *" or exact == "contraseña":
                try_assign(f["el"], "password", "Password", idx)
            elif exact in ("name", "name *") and "first" not in raw and "last" not in raw:
                try_assign(f["el"], "full_name", "Full Name", idx)
            elif exact in ("nombre", "nombre *", "nom", "nom *",
                           "nome", "nome completo", "nombre completo",
                           "nume", "ime") and "first" not in raw and "last" not in raw:
                try_assign(f["el"], "full_name", "Full Name", idx)
            elif exact in ("first name", "first name *", "given name", "prénom",
                           "nombre de pila", "nome próprio", "Vorname"):
                try_assign(f["el"], "first_name", "First Name", idx)
            elif exact in ("last name", "last name *", "surname", "family name",
                           "apellido", "apellidos", "sobrenome",
                           "cognome", "Nachname", "nom de famille"):
                try_assign(f["el"], "last_name", "Last Name", idx)
            elif exact in ("phone", "phone *", "tel", "tel *", "mobile",
                           "teléfono", "telefono", "teléfono móvil",
                           "número de teléfono", "téléphone", "celular",
                           "móvil", "fone", "Numéro de téléphone"):
                try_assign(f["el"], "phone", "Phone", idx)
            elif exact in ("company", "company name", "company name *", "company *",
                           "organization", "empresa", "compañía", "razón social",
                           "razón social *", "razon social", "raison sociale",
                           "razao social", "Empresa", "Organisation", "organización"):
                try_assign(f["el"], "company", "Company", idx)
            elif exact in ("address", "address *", "street address",
                           "dirección", "adresse", "endereço", "calle",
                           "Straße", "indirizzo", "Addresse"):
                try_assign(f["el"], "address", "Address", idx)
            elif exact in ("city", "city *", "town",
                           "ciudad", "ville", "cidade", "città",
                           "Stadt", "Localidade"):
                try_assign(f["el"], "city", "City", idx)
            elif exact in ("zip", "zip *", "zip code", "zip code *",
                           "postal code", "postal",
                           "código postal", "code postal", "CEP",
                           "PLZ", "Postleitzahl", "codice postal",
                           "Postal", "Index", " индекс", "código postal"):
                try_assign(f["el"], "zip", "Zip Code", idx)
            elif exact in ("state", "state *", "province", "region",
                           "provincia", "région", "región", "regione",
                           "Estado", "Região", "Bundesland", "état"):
                try_assign(f["el"], "state", "State", idx)
            elif exact in ("country", "country *",
                           "país", "pays", "paese", "Land", "Paese"):
                try_assign(f["el"], "country", "Country", idx)
            elif exact == "@":
                try_assign(f["el"], "instagram", "instagram", idx)
            elif exact == "https://":
                try_assign(f["el"], "website", "Blog", idx)
            elif exact == "fb.com/":
                try_assign(f["el"], "facebook", "Facebook", idx)
            elif "linkedin" in exact:
                try_assign(f["el"], "linkedin", "Linkedin", idx)
            elif "youtube" in exact:
                try_assign(f["el"], "youtube", "youtube", idx)
            elif "pinterest" in exact:
                try_assign(f["el"], "pinterest", "pinterest", idx)
            elif "twitter" in exact or "x.com" in exact:
                try_assign(f["el"], "twitter", "twitter/X", idx)
            elif "tiktok" in exact:
                try_assign(f["el"], "tiktok", "Tiktok", idx)
            elif exact in ("birthday", "birthday *", "date of birth", "dob", "ngay sinh",
                           "fecha de nacimiento", "date de naissance", "data de nascimento",
                           "Geburtsdatum", "data di nascita", "fecha nacimiento"):
                try_assign(f["el"], "birthday", "Birthday", idx)
            elif exact in ("username", "user name", "username *",
                           "nombre de usuario", "nom d'utilisateur",
                           "Benutzername", "nome utente", "Usuário"):
                try_assign(f["el"], "username", "User Name", idx)
            elif exact in ("full name", "full name *", "your name"):
                try_assign(f["el"], "full_name", "Full Name", idx)

            # ── 2. Fallback: type="tel" nhung chua map Phone ──────────────────
            elif f["type"] == "tel" and "phone" not in assigned_types:
                try_assign(f["el"], "phone", "Phone", idx)

        log(f"Map: {len(field_assignments)} truong")
        for el, (val, t, i) in sorted(field_assignments.items(), key=lambda x: x[1][2]):
            log(f"  [{t}] = '{val}'")

        if not field_assignments:
            result["error"] = "Khong map duoc truong nao"
            err("Khong map duoc truong nao!")
            return result

        # ═══════════════════════════════════════════════════════════════════════
        # BUOC 3 — DIEN TUNG TRUONG
        # ═══════════════════════════════════════════════════════════════════════

        def strip_prefix(val: str, ftype: str) -> str:
            """Strip label prefix khoi value (VD: @ -> instagram, https:// -> website)."""
            if ftype == "instagram":
                # Bo @ va domain
                for prefix in ("@", "https://www.instagram.com/", "http://www.instagram.com/",
                               "https://instagram.com/", "http://instagram.com/", "instagram.com/"):
                    if val.startswith(prefix):
                        return val[len(prefix):]
            elif ftype == "website":
                for prefix in ("https://", "http://", "https://www.", "http://www."):
                    if val.startswith(prefix):
                        return val[len(prefix):]
            elif ftype == "facebook":
                for prefix in ("https://www.facebook.com/", "http://www.facebook.com/",
                               "https://facebook.com/", "http://facebook.com/",
                               "fb.com/", "facebook.com/"):
                    if val.startswith(prefix):
                        return val[len(prefix):]
            elif ftype == "linkedin":
                for prefix in ("https://www.linkedin.com/in/",
                               "http://www.linkedin.com/in/",
                               "https://linkedin.com/in/",
                               "linkedin.com/in/", "www.linkedin.com/in/"):
                    if val.startswith(prefix):
                        return val[len(prefix):]
            elif ftype == "youtube":
                for prefix in ("https://www.youtube.com/@", "http://www.youtube.com/@",
                               "https://youtube.com/@", "youtube.com/@"):
                    if val.startswith(prefix):
                        return val[len(prefix):]
            elif ftype == "pinterest":
                for prefix in ("https://www.pinterest.com/", "http://www.pinterest.com/",
                               "https://pinterest.com/", "pinterest.com/", "https://pin.it/"):
                    if val.startswith(prefix):
                        return val[len(prefix):]
            elif ftype == "twitter":
                for prefix in ("https://www.twitter.com/", "http://www.twitter.com/",
                               "https://twitter.com/", "twitter.com/",
                               "https://x.com/", "x.com/"):
                    if val.startswith(prefix):
                        return val[len(prefix):]
            elif ftype == "tiktok":
                for prefix in ("https://www.tiktok.com/@", "http://www.tiktok.com/@",
                               "https://tiktok.com/@", "tiktok.com/@"):
                    if val.startswith(prefix):
                        return val[len(prefix):]
            return val

        filled = []
        failed_fields = []

        for el, (val, ftype, orig_idx) in sorted(field_assignments.items(), key=lambda x: x[1][2]):
            if not val:
                continue

            # Tim field goc trong form_data
            f = next((fd for fd in form_data if fd["el"] == el), None)
            if not f:
                continue

            inp_type = f["type"]
            sel_id   = f["id"]
            sel_name = f["name"]

            # Strip prefix theo ftype
            display_val = strip_prefix(val, ftype)

            # Build selector
            selector = ""
            if sel_id:
                selector = f"#{sel_id}"
            elif sel_name:
                selector = f"[name='{sel_name}']"

            try:
                if inp_type == "select":
                    # Su dung selector theo id/name, fallback = tat ca selects roi loc
                    inp = page.locator(f"select{selector}").first if selector else page.locator("select").first
                    try:
                        inp_cnt = await inp.count()
                    except TypeError:
                        inp_cnt = 1  # .first co the khong co count()
                    if inp_cnt > 0 and await inp.is_visible():
                        dv = display_val.lower()
                        matched = False
                        try:
                            await inp.select_option(label=display_val, timeout=2000)
                            matched = True
                        except Exception:
                            try:
                                await inp.select_option(value=display_val, timeout=2000)
                                matched = True
                            except Exception:
                                # Lay cac options de tim gia tri phu hop
                                opts = await inp.locator("option").all()
                                for opt in opts:
                                    opt_text = (await opt.text_content() or "").strip()
                                    if dv in opt_text.lower() or opt_text.lower() in dv or dv.split()[0] in opt_text.lower():
                                        await opt.click()
                                        matched = True
                                        break
                        if matched:
                            filled.append(ftype)
                            log(f"  OK [{ftype}] select='{display_val}'")
                        else:
                            failed_fields.append(ftype)
                    else:
                        failed_fields.append(ftype)
                else:
                    if selector:
                        inp = page.locator(f"input{selector}").first
                    else:
                        inp = page.locator(
                            "input:not([type='hidden']):not([type='submit'])"
                            ":not([type='button']):not([type='reset'])"
                        ).nth(orig_idx)

                    if await inp.count() > 0 and await inp.is_visible():
                        await inp.click()
                        await inp.clear()
                        for ch in display_val:
                            await inp.type(ch, delay=random.uniform(30, 90))
                        await inp.dispatch_event("input")
                        await inp.dispatch_event("change")
                        await inp.dispatch_event("blur")
                        filled.append(ftype)
                        log(f"  OK [{ftype}] = '{display_val}'")
                    else:
                        failed_fields.append(ftype)
                        log(f"  SKIP [{ftype}] hidden/disabled")
            except Exception as e_fill:
                failed_fields.append(ftype)
                log(f"  LOI [{ftype}]: {e_fill}")

        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        ok(f"Da dien {len(filled)} truong, {len(failed_fields)} loi: {failed_fields}")

        if not filled:
            result["error"] = "Khong dien duoc truong nao"
            err("Khong dien duoc truong nao!")
            return result

        # ═══════════════════════════════════════════════════════════════════════
        # BUOC 4 — CLOUDFLARE (sau khi dien form)
        # ═══════════════════════════════════════════════════════════════════════
        turnstile_ok = await handle_turnstile(page)
        if not turnstile_ok:
            warn("Cloudflare chua xong — thu retry Turnstile...")
            await asyncio.sleep(5)
            turnstile_ok = await handle_turnstile(page)
        if not turnstile_ok:
            warn("Cloudflare chua xong — van thu submit...")

        # ═══════════════════════════════════════════════════════════════════════
        # BUOC 5 — TIM VA CLICK NUT SUBMIT (VERSION MOI, CHINH XAC HON)
        # ═══════════════════════════════════════════════════════════════════════
        import re

        # SCAN toan bo button trong DOM, chi tiet nhat
        button_info = await page.evaluate(r"""
            () => {
                const buttons = Array.from(document.querySelectorAll(
                    'button, input[type="submit"], input[type="button"], a[role="button"], [role="button"]'
                ));
                return buttons.map(b => {
                    const disabled = b.disabled || b.getAttribute('aria-disabled') === 'true';
                    const type = b.tagName.toLowerCase();
                    let text = (b.textContent || b.value || '').trim().slice(0, 80).toLowerCase();
                    let visible = b.offsetParent !== null && getComputedStyle(b).display !== 'none' && getComputedStyle(b).visibility !== 'hidden';
                    let rect = b.getBoundingClientRect();
                    let covered = false;
                    if (visible && rect.width > 0 && rect.height > 0) {
                        const elemAt = document.elementFromPoint(rect.left + rect.width/2, rect.top + rect.height/2);
                        if (elemAt && elemAt !== b && !b.contains(elemAt)) covered = true;
                    }
                    return { type, text, disabled, visible, covered, tag: b.tagName, id: b.id, className: b.className };
                });
            }
        """)
        log(f"TIM BUTTON: {len(button_info)} button tim duoc:")
        for i, b in enumerate(button_info):
            log(f"  [{i}] {b['type']} text='{b['text']}' disabled={b['disabled']} visible={b['visible']} covered={b['covered']}")

        # Tim button Create Account
        submit_btn = None
        submit_btn_found = False
        submit_text = ""

        for b in button_info:
            if not b['visible'] or b['covered']:
                continue
            if 'google' in b['text'] or 'facebook' in b['text'] or 'continue with' in b['text']:
                continue
            if 'create' in b['text'] and 'account' in b['text']:
                submit_btn_found = True
                submit_text = b['text']
                log(f"CHON BUTTON: '{b['text']}' (disabled={b['disabled']}, covered={b['covered']})")
                break
            if b['text'] in ('create account', 'submit', 'register', 'sign up', 'signup', 'register now'):
                submit_btn_found = True
                submit_text = b['text']
                log(f"CHON BUTTON: '{b['text']}' (disabled={b['disabled']}, covered={b['covered']})")
                break

        if not submit_btn_found:
            err("KHONG TIM THAY nut 'Create Account'!")
            # Debug: in tat ca button text
            log("DEBUG - Tat ca button tren trang:")
            for b in button_info:
                if b['visible']:
                    log(f"  text='{b['text']}' disabled={b['disabled']} covered={b['covered']}")
            result["error"] = "Khong tim thay nut Create Account"
            return result

        # ── CLICK BUTTON (JS click chinh, Playwright fallback) ─────────────────

        # Thu 1: JavaScript click (to nhat, khong bi overlay che)
        try:
            clicked = await page.evaluate(r"""
                () => {
                    const allBtns = Array.from(document.querySelectorAll(
                        'button, input[type="submit"], input[type="button"], [role="button"]'
                    ));
                    for (const btn of allBtns) {
                        const txt = (btn.textContent || btn.value || '').trim().toLowerCase();
                        if ((txt.includes('create') && txt.includes('account')) ||
                            txt === 'create account') {
                            btn.disabled = false;
                            btn.removeAttribute('disabled');
                            btn.click();
                            return 'clicked: ' + txt;
                        }
                    }
                    return 'not found';
                }
            """)
            if clicked != 'not found':
                log(f"JS CLICK thanh cong: {clicked}")
                submitted = True
            else:
                submitted = False
        except Exception as e:
            log(f"JS click loi: {e}")
            submitted = False

        # Thu 2: Playwright click (neu JS that bai)
        if not submitted:
            log("Thu Playwright click...")
            try:
                # Tim bang locator
                btn_locator = page.get_by_role("button", name=re.compile(r"create\s*account", re.IGNORECASE))
                cnt = await btn_locator.count()
                if cnt > 0:
                    btn_el = btn_locator.first
                    if await btn_el.is_visible():
                        await btn_el.scroll_into_view_if_needed()
                        await asyncio.sleep(0.5)
                        box = await btn_el.bounding_box()
                        if box and box["width"] > 0 and box["height"] > 0:
                            await page.mouse.click(
                                box["x"] + box["width"] / 2,
                                box["y"] + box["height"] / 2
                            )
                            submitted = True
                            ok("Playwright click thanh cong!")
            except Exception as e2:
                log(f"Playwright click that bai: {e2}")

        # Thu 3: Force click bang mouse
        if not submitted:
            log("Thu Force mouse click...")
            try:
                clicked = await page.evaluate(r"""
                    () => {
                        const allBtns = Array.from(document.querySelectorAll(
                            'button, input[type="submit"]'
                        ));
                        for (const btn of allBtns) {
                            const txt = (btn.textContent || btn.value || '').trim().toLowerCase();
                            if ((txt.includes('create') && txt.includes('account'))) {
                                const rect = btn.getBoundingClientRect();
                                const evt = new MouseEvent('click', {
                                    view: window, bubbles: true,
                                    cancelable: true,
                                    clientX: rect.left + rect.width / 2,
                                    clientY: rect.top + rect.height / 2
                                });
                                btn.dispatchEvent(evt);
                                return 'force-clicked: ' + txt;
                            }
                        }
                        return 'not found';
                    }
                """)
                if clicked != 'not found':
                    log(f"FORCE CLICK thanh cong: {clicked}")
                    submitted = True
            except Exception as e:
                log(f"Force click loi: {e}")

        if not submitted:
            result["error"] = "Click Create Account that bai (tat ca phuong phap)"
            err("Click that bai!")
            return result

        # ═══════════════════════════════════════════════════════════════════════
        # BUOC 6 — KET QUA
        # ═══════════════════════════════════════════════════════════════════════
        log("Cho redirect sau submit...")
        url_before = page.url

        # ── Debug: lay thong tin ngay sau khi click ────────────────────────────
        await asyncio.sleep(3)
        try:
            url_after_click = page.url
            page_after = (await page.inner_text("body")).lower()
            # Lay cac error/success message tren page
            error_elements = await page.query_selector_all(
                "[class*='error'], [class*='alert'], [class*='notice'], "
                "[class*='message'], [role='alert'], .toast, .error"
            )
            for el in error_elements[:5]:
                try:
                    txt = (await el.inner_text()).strip()
                    if txt:
                        log(f"  [PAGE MSG] {txt[:200]}")
                except Exception:
                    pass
            log(f"  URL sau click: {url_after_click}")
        except Exception as e2:
            log(f"Debug loi: {e2}")

        try:
            await page.wait_for_url(
                lambda u: u != url_before and "/create-account" not in u,
                timeout=60000,
            )
            ok(f"Redirect! URL: {page.url}")
        except PlaywrightTimeout:
            log("Khong co redirect trong 60s")

        await asyncio.sleep(2)

        try:
            url_final = page.url
        except Exception:
            url_final = url_before

        try:
            page_text = (await page.inner_text("body")).lower()
        except Exception:
            page_text = ""

        result["account_link"] = url_final

        url_final_clean = url_final.strip("/").split("?")[0]
        url_before_clean = url_before.strip("/").split("?")[0]
        url_changed = url_final_clean != url_before_clean

        error_kws = [
            # Lỗi ngay tại chỗ nút Create Account
            "this email", "email already", "already registered", "already exist",
            "account already", "already have an account",
            "you already", "ya tienes", "already exists",
            "invalid email", "enter a valid email",
            "password must", "the password must", "passwords do not match",
            "required field", "this field is required",
            "please fill", "please enter",
            "campos obligatorios", "erro", "erreur", "erreur",
            "khong hop le", "that bai",
            # Các ngôn ngữ khác
            "click here to login", "haga clic aquí para ingresar",
            "iniciar sesión", "iniciar session", "iniciar sesion",
            "already have an account",
            "user already", "user exists",
            "taken", "already taken", "is not available",
            "does not match", "mismatch",
            "invalid", "incorrect",
        ]

        # Sau submit: chi can URL doi → la thanh cong
        # Lỗi = URL không đổi (lỗi hiển thị ngay tại chỗ nút Create Account)
        has_specific_error = any(k in page_text for k in error_kws)

        if url_changed and "/create-account" not in url_final:
            # URL đổi → thành công
            # Kiểm tra page mới: có chữ "pending" → Pending, có "approved" → Approved
            if any(k in page_text for k in ("pending", "chờ duyệt", "waiting for",
                    "under review", "will review", "đang chờ", "pending approval",
                    "pending review")):
                result["status"] = "Pending"
                ok(f"DANG KY XONG — Pending (doi Brand duyet)! URL: {url_final[:80]}")
            elif any(k in page_text for k in ("approved", "đã duyệt", "congratulations",
                    "welcome", "approved affiliate", "active", "approved by")):
                result["status"] = "Approved"
                ok(f"DANG KY XONG — Approved (Brand da duyet)! URL: {url_final[:80]}")
            else:
                # Không rõ → vẫn là Pending, email check sẽ xác nhận
                result["status"] = "Pending"
                warn(f"DANG KY XONG — Pending (page khong ro)! URL: {url_final[:80]}")
            result["registered_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            # URL không đổi → có lỗi hiển thị tại chỗ nút Create Account
            result["status"] = "Failed"
            result["error"]  = "Loi ngay tai nut Create Account"
            err("URL khong doi — loi hien thi tai nut Create Account!")

    except Exception as e:
        result["error"] = str(e)
        err(f"Loi: {e}")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    _p("=" * 60, C_BOLD)
    _p("  GoAffPro Auto-Register v4.2  (Genlogin)", C_MAGENTA, bold=True)
    _p("=" * 60, C_BOLD)

    # ── Google Sheet ────────────────────────────────────────────────────────────
    step("Ket noi Google Sheet...")
    try:
        client = get_client()
        ok("Ket noi thanh cong!")
    except FileNotFoundError:
        err(f"Khong tim thay: {SERVICE_ACCOUNT_JSON}")
        return
    except Exception as e:
        err(f"Loi ket noi: {e}")
        return

    step("Doc sheet Profiles...")
    profile = read_profiles(client)
    if not profile:
        err("Sheet Profiles trong!")
        return
    log(f"  Profile: {profile.get('Full Name', '')} | {profile.get('Email', '')}")

    step("Doc sheet Links...")
    links = read_links(client)
    if not links:
        warn("Khong co link nao!")
        return
    links_to_run = links[:MAX_LINKS]

    # ── Skip logic: chi chay link moi (Status = rong) ──────────────────────────
    skipped_list = []
    pending_list = []
    for row_num, brand, link, status in links_to_run:
        status_upper = status.upper() if status else ""
        # Chi skip neu da co Status ro rang (da xu ly), rong = link moi = chay
        if status_upper in ("SUCCESS", "FAILED", "UNKNOWN", "APPROVED", "REJECTED", "PENDING"):
            skipped_list.append((status, brand, link))
        else:
            pending_list.append((row_num, brand, link, status))

    if skipped_list:
        warn(f"{len(skipped_list)} link da duoc xu ly truoc do — skip (chi chay 1 lan):")
        for st, lk, lnk in skipped_list:
            log(f"  [{st}] {lnk}")
    if pending_list:
        ok(f"Se chay {len(pending_list)} link moi/chua xu ly")
    else:
        ok("Tat ca link da duoc xu ly! Thoat.")
        return

    ok(f"Tim thay {len(links)} link — se chay {len(pending_list)} link!")

    # ── Genlogin ───────────────────────────────────────────────────────────────
    step("Genlogin: Dang nhap...")
    try:
        token = genlogin_auth()
        ok("Genlogin OK!")
    except Exception as e:
        err(f"Genlogin auth that bai: {e}")
        return

    step("Genlogin: Tim profile...")
    email_keyword = profile.get("Email", "")
    profile_id = genlogin_find_profile(token, email_keyword, profile)
    if not profile_id:
        err(f"Khong tim thay profile cho email: {email_keyword}")
        return
    ok(f"Tim thay profile ID: {profile_id}")

    step("Genlogin: Start browser...")
    try:
        # Stop neu dang chay
        genlogin_stop(token, profile_id)
        await asyncio.sleep(2)
        profile_info = genlogin_start(token, profile_id)
        ws_endpoint = profile_info["wsEndpoint"]
        ok(f"Browser da start! CDP: {ws_endpoint}")
    except Exception as e:
        err(f"Start profile that bai: {e}")
        return

    # ── Playwright CDP ─────────────────────────────────────────────────────────
    step("Playwright: Ket noi qua CDP...")
    pw = None
    browser = None
    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.connect_over_cdp(ws_endpoint)
        ok("CDP ket noi thanh cong!")
    except Exception as e:
        err(f"CDP connect that bai: {e}")
        if pw:
            await pw.stop()
        return

    # Su dung context cua Genlogin
    context = browser.contexts[0] if browser.contexts else await browser.new_context()
    # Cho browser khoi dong xong
    await asyncio.sleep(3)
    try:
        if context.pages:
            page = context.pages[0]
            # Verify page con song
            _ = page.url
        else:
            page = await context.new_page()
    except Exception:
        # Page bi dong, tao moi
        page = await context.new_page()

    # ── Chay tung link ─────────────────────────────────────────────────────────
    total = len(pending_list)
    stats = {"Success": 0, "Failed": 0, "Unknown": 0}

    for idx, (row_num, brand, link, _) in enumerate(pending_list, 1):
        _p(f"\n{'─' * 60}", C_BOLD)
        _p(f"  [{idx}/{total}] {link}", C_YELLOW, bold=True)

        # Lay brand, neu khong co thi extract tu url
        if not brand:
            from urllib.parse import urlparse
            domain = urlparse(link).netloc.replace("www.", "")
            if ".goaffpro." in domain:
                brand = domain.split(".goaffpro.")[0]
            elif domain.startswith("partners."):
                brand = domain.replace("partners.", "")
            else:
                brand = domain.split(".")[0]
        brand_norm = re.sub(r"[^a-z0-9]", "", brand.lower())

        # Lay Gmail credentials
        gmail_email    = profile.get("Gmail", "")
        gmail_password = profile.get("GmailPassword", "")

        try:
            res = await register_one(link, profile, context, pw, ws_endpoint, browser)
        except Exception as e:
            err(f"Loi browser: {e}")
            try:
                genlogin_stop(token, profile_id)
                await asyncio.sleep(3)
                profile_info = genlogin_start(token, profile_id)
                ws_endpoint = profile_info["wsEndpoint"]
                try:
                    await browser.close()
                except Exception:
                    pass
                browser = await pw.chromium.connect_over_cdp(ws_endpoint)
                context = browser.contexts[0] if browser.contexts else await browser.new_context()
                page = context.pages[0] if context.pages else await context.new_page()
                res = await register_one(link, profile, context, pw, ws_endpoint, browser)
            except Exception as e2:
                err(f"Khoi phuc that bai: {e2}")
                res = {
                    "status": "Failed",
                    "email": profile.get("Email", ""),
                    "account_link": "",
                    "registered_at": "",
                    "error": f"Khoi phuc that bai: {e2}",
                }

        result_status = res.get("status", "Unknown")

        # Ghi ket qua (Pending)
        try:
            update_result(client, row_num, res)
            ok("Da ghi ket qua vao Google Sheet!")
        except Exception as e:
            err(f"Loi ghi sheet: {e}")

        # ── Email check cho link nay (neu co Gmail) ─────────────────────────────
        if result_status == "Pending" and gmail_email and gmail_password and brand_norm:
            ok(f"Dang doi email tu Brand [{brand}]...")
            email_final_status, email_msg = await _wait_and_check_email(
                gmail_email, gmail_password, brand_norm, row_num, client
            )
            if email_final_status == "Approved":
                result_status = "Approved"
                stats["Success"] += 1
                ok(f"  BRAND APPROVED! {email_msg}")
            elif email_final_status == "Rejected":
                result_status = "Rejected"
                stats["Failed"] += 1
                warn(f"  BRAND REJECTED: {email_msg}")
            else:
                stats["Unknown"] += 1
                warn(f"  Van Pending — {email_msg}")
        else:
            if result_status in ("Success", "Approved"):
                stats["Success"] += 1
                ok(f"  KET QUA: {result_status}")
            elif result_status == "Failed":
                stats["Failed"] += 1
                err(f"  KET QUA: FAILED")
            else:
                stats["Unknown"] += 1
                warn(f"  KET QUA: PENDING")

        if idx < total:
            d = random.uniform(5, 12)
            log(f"Cho {d:.1f}s truoc link tiep theo...")
            await asyncio.sleep(d)

    # ── Ket thuc ───────────────────────────────────────────────────────────────
    step("Dong browser...")
    try:
        await browser.close()
    except Exception:
        pass
    try:
        genlogin_stop(token, profile_id)
        ok("Genlogin profile da stop!")
    except Exception:
        pass
    if pw:
        await pw.stop()

    _p(f"\n{'=' * 60}", C_BOLD)
    _p(f"  APPROVED:  {stats['Success']}", C_GREEN, bold=True)
    _p(f"  FAILED:    {stats['Failed']}", C_RED)
    _p(f"  PENDING:   {stats['Unknown']}", C_YELLOW)
    _p(f"{'=' * 60}", C_BOLD)
    ok("Hoan thanh!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        err("Dung boi Ctrl+C")
        sys.exit(0)
    except Exception as e:
        err(f"Loi: {e}")
        sys.exit(1)
