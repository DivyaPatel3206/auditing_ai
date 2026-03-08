from db import existing_voucher_numbers, average_voucher_amount_by_type, get_ledger_map


def analyze_voucher(company_id: int, voucher_data: dict, entries: list) -> dict:
    """
    Low-cost rule-based AI scrutiny engine.
    Time complexity is effectively O(n) over voucher lines.
    """
    flags = []
    risk_score = 0

    total_debit = sum(float(e["debit"]) for e in entries)
    total_credit = sum(float(e["credit"]) for e in entries)
    total_amount = max(total_debit, total_credit)

    if existing_voucher_numbers(company_id, voucher_data["voucher_number"]) > 0:
        flags.append("Duplicate voucher number detected")
        risk_score += 25

    if total_amount >= 10000 and float(total_amount).is_integer() and int(total_amount) % 1000 == 0:
        flags.append("Large rounded transaction amount")
        risk_score += 10

    avg_amount = average_voucher_amount_by_type(company_id, voucher_data["type"])
    if avg_amount > 0 and total_amount > avg_amount * 3:
        flags.append(f"Voucher amount is unusually high vs average {voucher_data['type']} amount")
        risk_score += 30

    ledger_map = get_ledger_map(company_id)
    ledger_names = []
    has_cash_bank = False

    for e in entries:
        ledger = ledger_map.get(e["ledger_id"])
        if not ledger:
            continue
        ledger_names.append(ledger["ledger_name"].lower())
        group_name = (ledger["group_name"] or "").lower()
        name = (ledger["ledger_name"] or "").lower()

        if "bank" in group_name or "bank" in name or "cash" in name:
            has_cash_bank = True

    if has_cash_bank and total_amount > 50000:
        flags.append("High cash/bank linked transaction")
        risk_score += 20

    non_zero_lines = 0
    same_side_count = 0
    for e in entries:
        if e["debit"] > 0 or e["credit"] > 0:
            non_zero_lines += 1
        if e["debit"] == 0 or e["credit"] == 0:
            same_side_count += 1

    if non_zero_lines >= 4:
        flags.append("Voucher has many accounting lines; review manually")
        risk_score += 8

    if voucher_data["type"] in ("Sales", "Purchase") and not voucher_data.get("narration", "").strip():
        flags.append("Narration missing for sales/purchase voucher")
        risk_score += 7

    if risk_score >= 50:
        risk_level = "High"
    elif risk_score >= 20:
        risk_level = "Medium"
    else:
        risk_level = "Low"

    if not flags:
        flags.append("No major scrutiny issue detected")

    return {
        "risk_score": min(risk_score, 100),
        "risk_level": risk_level,
        "flags": flags,
        "total_amount": total_amount,
    }
