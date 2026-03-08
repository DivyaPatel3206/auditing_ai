from fastapi import FastAPI, Request, Form, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from db import (
    init_db,
    list_companies,
    get_company,
    create_company,
    delete_company,
    list_ledgers,
    create_ledger,
    delete_ledger,
    list_vouchers,
    recent_vouchers,
    create_voucher,
    delete_voucher,
    dashboard_summary,
    ai_dashboard_data,
)
from ai_rules import analyze_voucher

app = FastAPI(title="Tally Basic Clone with AI Scrutiny")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

DEFAULT_LEDGER_GROUPS = [
    "Capital Account",
    "Current Assets",
    "Current Liabilities",
    "Sales Accounts",
    "Purchase Accounts",
    "Bank Accounts",
    "Indirect Income",
    "Indirect Expenses",
]


@app.on_event("startup")
def startup():
    init_db()


def active_company_from_request(request: Request):
    raw = request.cookies.get("active_company_id")
    if raw and raw.isdigit():
        return get_company(int(raw))
    return None


def base_context(request: Request, screen: str, message: str = "", message_type: str = ""):
    active_company = active_company_from_request(request)
    return {
        "request": request,
        "screen": screen,
        "message": message,
        "message_type": message_type,
        "active_company": active_company,
        "companies": list_companies(),
        "default_ledger_groups": DEFAULT_LEDGER_GROUPS,
    }


@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    active_company = active_company_from_request(request)
    company_id = active_company["id"] if active_company else None
    summary = dashboard_summary(company_id)
    recent = recent_vouchers(company_id, 10) if company_id else []

    context = base_context(request, "dashboard")
    context.update({
        "summary": summary,
        "recent_vouchers": recent,
    })
    return templates.TemplateResponse("dashboard.html", context)


@app.get("/company", response_class=HTMLResponse)
def company_page(request: Request):
    return templates.TemplateResponse("company.html", base_context(request, "company"))


@app.post("/company")
def company_create(
    request: Request,
    name: str = Form(...),
    mailing_name: str = Form(""),
    address: str = Form(""),
    state: str = Form(""),
    country: str = Form("India"),
    phone: str = Form(""),
    email: str = Form(""),
    financial_year_start: str = Form(...),
    books_from: str = Form(...),
    currency: str = Form("₹"),
    maintain_inventory: str = Form("Yes"),
    enable_gst: str = Form("Yes"),
):
    company_id = create_company({
        "name": name.strip(),
        "mailing_name": mailing_name.strip(),
        "address": address.strip(),
        "state": state.strip(),
        "country": country.strip() or "India",
        "phone": phone.strip(),
        "email": email.strip(),
        "financial_year_start": financial_year_start,
        "books_from": books_from,
        "currency": currency.strip() or "₹",
        "maintain_inventory": maintain_inventory,
        "enable_gst": enable_gst,
    })

    response = RedirectResponse(url="/company", status_code=status.HTTP_303_SEE_OTHER)
    if not request.cookies.get("active_company_id"):
        response.set_cookie("active_company_id", str(company_id))
    return response


@app.get("/company/select/{company_id}")
def select_company(company_id: int):
    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie("active_company_id", str(company_id))
    return response


@app.get("/company/delete/{company_id}")
def remove_company(request: Request, company_id: int):
    delete_company(company_id)
    response = RedirectResponse(url="/company", status_code=status.HTTP_303_SEE_OTHER)
    active = request.cookies.get("active_company_id")
    if active == str(company_id):
        response.delete_cookie("active_company_id")
    return response


@app.get("/ledger", response_class=HTMLResponse)
def ledger_page(request: Request):
    active_company = active_company_from_request(request)
    ledgers = list_ledgers(active_company["id"]) if active_company else []

    context = base_context(request, "ledger")
    context["ledgers"] = ledgers
    return templates.TemplateResponse("ledger.html", context)


@app.post("/ledger")
def ledger_create(
    request: Request,
    ledger_name: str = Form(...),
    group_name: str = Form(...),
    opening_balance: float = Form(0),
    balance_type: str = Form("Debit"),
    gst_applicable: str = Form("No"),
    gst_number: str = Form(""),
    address: str = Form(""),
    phone: str = Form(""),
    email: str = Form(""),
):
    active_company = active_company_from_request(request)
    if not active_company:
        return RedirectResponse(url="/ledger", status_code=status.HTTP_303_SEE_OTHER)

    create_ledger({
        "company_id": active_company["id"],
        "ledger_name": ledger_name.strip(),
        "group_name": group_name,
        "opening_balance": opening_balance,
        "balance_type": balance_type,
        "gst_applicable": gst_applicable,
        "gst_number": gst_number.strip(),
        "address": address.strip(),
        "phone": phone.strip(),
        "email": email.strip(),
    })
    return RedirectResponse(url="/ledger", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/ledger/delete/{ledger_id}")
def remove_ledger(ledger_id: int):
    delete_ledger(ledger_id)
    return RedirectResponse(url="/ledger", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/voucher", response_class=HTMLResponse)
def voucher_page(request: Request):
    active_company = active_company_from_request(request)
    ledgers = list_ledgers(active_company["id"]) if active_company else []
    vouchers = list_vouchers(active_company["id"]) if active_company else []

    context = base_context(request, "voucher")
    context.update({
        "ledgers": ledgers,
        "vouchers": vouchers,
    })
    return templates.TemplateResponse("voucher.html", context)


@app.post("/voucher")
async def voucher_create(request: Request):
    active_company = active_company_from_request(request)
    if not active_company:
        return RedirectResponse(url="/voucher", status_code=status.HTTP_303_SEE_OTHER)

    form = await request.form()
    voucher_number = str(form.get("voucher_number", "")).strip()
    date = str(form.get("date", "")).strip()
    voucher_type = str(form.get("type", "")).strip()
    narration = str(form.get("narration", "")).strip()

    ledger_ids = form.getlist("ledger_id")
    debits = form.getlist("debit")
    credits = form.getlist("credit")

    cleaned_entries = []
    for ledger_id, debit, credit in zip(ledger_ids, debits, credits):
        if not ledger_id:
            continue
        debit_value = float(debit or 0)
        credit_value = float(credit or 0)
        if debit_value <= 0 and credit_value <= 0:
            continue
        cleaned_entries.append({
            "ledger_id": int(ledger_id),
            "debit": debit_value,
            "credit": credit_value,
        })

    if len(cleaned_entries) < 2:
        return RedirectResponse(url="/voucher", status_code=status.HTTP_303_SEE_OTHER)

    debit_total = sum(x["debit"] for x in cleaned_entries)
    credit_total = sum(x["credit"] for x in cleaned_entries)

    if round(debit_total, 2) != round(credit_total, 2):
        return RedirectResponse(url="/voucher", status_code=status.HTTP_303_SEE_OTHER)

    voucher_data = {
        "voucher_number": voucher_number,
        "date": date,
        "type": voucher_type,
        "narration": narration,
    }

    ai_result = analyze_voucher(active_company["id"], voucher_data, cleaned_entries)
    create_voucher(active_company["id"], voucher_data, cleaned_entries, ai_result)

    return RedirectResponse(url="/voucher", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/voucher/delete/{voucher_id}")
def remove_voucher(voucher_id: int):
    delete_voucher(voucher_id)
    return RedirectResponse(url="/voucher", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/ai-dashboard", response_class=HTMLResponse)
def ai_page(request: Request):
    active_company = active_company_from_request(request)
    context = base_context(request, "ai")

    if active_company:
        ai_data = ai_dashboard_data(active_company["id"])
        context.update(ai_data)
    else:
        context.update({
            "stats": {
                "total": 0,
                "high_count": 0,
                "medium_count": 0,
                "low_count": 0,
                "avg_score": 0,
            },
            "risky_vouchers": [],
        })

    return templates.TemplateResponse("ai_dashboard.html", context)
