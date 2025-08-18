#!/usr/bin/env python
"""
updater.py
Clones *active* cross‑sheet references for every duplicate sheet
and appends the new token to the formulas that already use the old token.
"""
import os, re, json, asyncio, argparse
from itertools import islice
from aiolimiter import AsyncLimiter
import smartsheet

SDK = smartsheet.Smartsheet(os.getenv("SMARTSHEET_TOKEN"))
RATE = AsyncLimiter(300, 60)          # 300 calls / min  :contentReference[oaicite:1]{index=1}

async def safe(fn, *a, **kw):
    async with RATE:
        loop = asyncio.get_running_loop()
        from functools import partial
        func = partial(fn, *a, **kw)
        return await loop.run_in_executor(None, func)

def chunked(it, n=490):               # 500 rows max / request  :contentReference[oaicite:2]{index=2}
    it = iter(it)
    return iter(lambda: list(islice(it, n)), [])

async def process_rollup(rollup_id: int, mapping: dict, dry: bool):
    # --- 1️⃣ pull refs & formulas (very small payload) ---
    refs_result = await safe(SDK.Sheets.list_cross_sheet_references, rollup_id)
    refs = refs_result.data if hasattr(refs_result, 'data') else refs_result
    
    sheet = await safe(SDK.Sheets.get_sheet, rollup_id, include="formulas", level=1)

    # active tokens present in formulas
    formulas = [c.formula for r in sheet.rows for c in r.cells if c.formula]
    active_tokens = set(re.findall(r'\{([^{}]+)\}', " ".join(formulas)))

    # map sourceSheetId -> [reference objects used in formulas]
    src_to_refs = {}
    for r in refs:
        if r.name in active_tokens:
            src_to_refs.setdefault(r.source_sheet_id, []).append(r)

    # --- 2️⃣ clone missing references ---
    repl_map = {}  # oldName -> [newName, …]
    for src_id, ref_list in src_to_refs.items():
        dup_ids = mapping.get(str(src_id), [])[1:]           # skip template itself
        for dup in dup_ids:
            if dup in [r.source_sheet_id for r in refs]:
                continue  # ref already exists
            for ref in ref_list:     # same bounds for every dup
                new_ref = await safe(
                    SDK.Sheets.create_cross_sheet_reference,
                    rollup_id,
                    SDK.models.CrossSheetReference({
                        "name": f"{ref.name}-dup-{dup[-4:]}",
                        "source_sheet_id": dup,
                        "start_row_id": ref.start_row_id,
                        "end_row_id": ref.end_row_id,
                        "start_column_id": ref.start_column_id,
                        "end_column_id": ref.end_column_id
                    })
                )                    # only *create* is allowed, not edit :contentReference[oaicite:3]{index=3}
                repl_map.setdefault(ref.name, []).append(new_ref.data.name)

    if not repl_map:
        print(f"Roll‑up {rollup_id}: nothing to update"); return

    # --- 3️⃣ patch formulas in‑memory ---
    def patched(text: str) -> str:
        out = text
        for old, news in repl_map.items():
            for new in news:
                out = out if f"{{{new}}}" in out else out.replace(
                    f"{{{old}}}", f"{{{old}}}, {{{new}}}")
        return out

    updates = []
    for row in sheet.rows:
        changed = False
        for cell in row.cells:
            if cell.formula:
                newf = patched(cell.formula)
                if newf != cell.formula:
                    cell.formula = newf
                    changed = True
        if changed:
            updates.append(row)

    # --- 4️⃣ send row updates in 490‑row chunks ---
    if dry:
        print(f"Roll‑up {rollup_id}: {len(updates)} rows would be updated (dry‑run)")
        return

    for chunk in chunked(updates):
        await safe(SDK.Sheets.update_rows, rollup_id, chunk)
    print(f"Roll‑up {rollup_id}: ✓ updated {len(updates)} rows")

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mapping", default="mapping.json")
    ap.add_argument("--rollups", 
                    help="comma‑separated sheet IDs, @file.txt, or 'auto' to use auto-detected")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    mapping = json.load(open(args.mapping))
    rollups = []
    
    if not args.rollups:
        print("⚠️ No rollups specified - nothing to update")
        return
    elif args.rollups == "auto":
        # Use auto-detected rollup IDs
        try:
            with open("auto_rollup_config.json") as f:
                config = json.load(f)
                rollups = config.get("auto_detected_rollup_ids", [])
                print(f"📋 Using {len(rollups)} auto-detected rollup sheets")
        except FileNotFoundError:
            print("❌ No auto_rollup_config.json found. Run discovery with --detect-rollups first")
            return
    elif args.rollups.startswith("@"):
        rollups = [int(x.strip()) for x in open(args.rollups[1:])]
    else:
        rollups = [int(x) for x in args.rollups.split(",")]

    if not rollups:
        print("⚠️ No rollup sheets found - nothing to update")
        return

    await asyncio.gather(*(process_rollup(r, mapping, args.dry_run) for r in rollups))

if __name__ == "__main__":
    asyncio.run(main())
