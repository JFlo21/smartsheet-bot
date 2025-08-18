#!/usr/bin/env python
"""
monitor_and_duplicate.py
• Loops through every sheet group in mapping.json
• For each 'latest' sheet in that group, checks error conditions
• If errors ⇒ duplicates the sheet, wipes its rows, tags it with OriginalSheetId
"""
import os, json, asyncio
from datetime import datetime as dt
from aiolimiter import AsyncLimiter
import smartsheet

# ---------- CONFIGURABLE THRESHOLDS ----------
ERR_CELL_LIMIT       = int(os.getenv("ERR_CELL_LIMIT", 1))
ERR_REF_LIMIT        = int(os.getenv("ERR_REF_LIMIT", 95))
ERR_CELLCOUNT_LIMIT  = int(os.getenv("ERR_CELLCOUNT_LIMIT", 4_800_000))
RATE = AsyncLimiter(250, 60)           # stay under 300 req/min
SDK  = smartsheet.Smartsheet(os.getenv("SMARTSHEET_TOKEN"))
WS_ID = int(os.getenv("WORKSPACE_ID", "0"))

# ---------- HELPERS ----------
async def safe(fn, *a, **kw):
    async with RATE:
        loop = asyncio.get_running_loop()
        from functools import partial
        func = partial(fn, *a, **kw)
        return await loop.run_in_executor(None, func)

async def duplicate_blank(src_id: int) -> int:
    """Copy sheet, then wipe all rows (blank duplicate)."""
    name = f"{dt.now():%Y‑%m‑%d} Duplicate of {src_id}"
    
    # Create copy request - use basic dict instead of model class
    copy_request = {
        "destination_type": "workspace",
        "destination_id": WS_ID,
        "new_name": name,
        "include": ["data"]
    }
    
    try:
        copy_result = await safe(SDK.Sheets.copy_sheet, src_id, copy_request)
        new_id = copy_result.result.id if hasattr(copy_result, 'result') else copy_result.id
    except Exception as e:
        print(f"Error copying sheet: {e}")
        return None

    # tag summary with OriginalSheetId using dict instead of model
    summary_request = {
        "summaryFields": [{
            "title": "OriginalSheetId",
            "type": "TEXT_NUMBER",
            "value": str(src_id)
        }]
    }
    
    try:
        await safe(SDK.Sheets.update_sheet_summary_fields, new_id, summary_request)
    except Exception as e:
        print(f"Error adding OriginalSheetId tag: {e}")

    # delete all rows (blank it)
    try:
        sheet = await safe(SDK.Sheets.get_sheet, new_id, level=1)
        if hasattr(sheet, 'rows') and sheet.rows:
            await safe(SDK.Sheets.delete_rows, new_id, [r.id for r in sheet.rows])
    except Exception as e:
        print(f"Error deleting rows: {e}")
        
    print(f"⚠️  Sheet {src_id} duplicated → {new_id} (blank) due to errors")
    return new_id

async def needs_rollover(sheet_id: int) -> bool:
    """Return True if the sheet trips any error thresholds."""
    try:
        sheet = await safe(SDK.Sheets.get_sheet, sheet_id, include="formulas,data", level=1)
        
        # 1️⃣ cell errors
        err_cells = sum(
            1 for r in sheet.rows for c in r.cells
            if c.display_value and str(c.display_value).startswith('#')
        )
        if err_cells >= ERR_CELL_LIMIT:
            return True

        # 2️⃣ reference utilisation
        try:
            refs = await safe(SDK.Sheets.list_cross_sheet_references, sheet_id)
            ref_ct = refs.total_count if hasattr(refs, 'total_count') else (len(refs.data) if hasattr(refs, 'data') else 0)
            if ref_ct >= ERR_REF_LIMIT:
                return True
        except Exception:
            # If we can't check references, assume 0
            pass

        # 3️⃣ cell capacity
        if (sheet.total_row_count or len(sheet.rows)) * len(sheet.columns) >= ERR_CELLCOUNT_LIMIT:
            return True

        return False
        
    except Exception as e:
        print(f"Error checking sheet {sheet_id}: {e}")
        return False

async def monitor_group(template_id: str, sheets: list[int]):
    latest = max(sheets)    # highest sheet‑ID -> newest duplicate
    if await needs_rollover(latest):
        # template_id could be a sheet ID (numeric string) or normalized name
        # Use the latest sheet ID (which is always the actual original sheet)
        # If template_id is numeric, use it; otherwise use the first sheet in the list
        try:
            original_sheet_id = int(template_id)
        except ValueError:
            # template_id is a normalized name, use the first sheet ID from the group
            original_sheet_id = min(sheets)  # lowest ID is typically the original
        
        await duplicate_blank(original_sheet_id)

async def main():
    mapping = json.load(open("mapping.json"))
    await asyncio.gather(*(monitor_group(tid, sids) for tid, sids in mapping.items()))

if __name__ == "__main__":
    asyncio.run(main())
