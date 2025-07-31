#!/usr/bin/env python
"""
discovery.py
Builds mapping.json  {templateId: [templateId, dup1Id, dup2Id, …]}
and last_seen.json   {sheetId: modifiedAt_iso}
Optimised: only hits the API for sheets modified since last run.
"""
import os, re, json, time, argparse, datetime as dt
from pathlib import Path
from dotenv import load_dotenv
import smartsheet
load_dotenv()

SDK = smartsheet.Smartsheet(os.getenv("SMARTSHEET_TOKEN"))
WS_ID = int(os.getenv("WORKSPACE_ID", "0"))

def normalise(name: str) -> str:
    """Strip '‑ Copy (1)' etc.  Fallback when no summary field exists."""
    return re.sub(r' - Copy.*$', '', name, flags=re.I).strip()

def load_cache(path: Path) -> dict:
    if path.exists() and path.stat().st_size > 0:
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}

def save_json(path: Path, obj):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2))
    tmp.replace(path)

def get_all_sheets_recursive(workspace_id: int) -> list:
    """Recursively get all sheets from workspace and all nested folders"""
    all_sheets = []
    
    # Get workspace with sheets and folders
    ws = SDK.Workspaces.get_workspace(workspace_id, include="sheets,folders")
    
    # Add direct sheets in workspace
    if hasattr(ws, 'sheets') and ws.sheets:
        all_sheets.extend(ws.sheets)
    
    # Recursively process folders
    if hasattr(ws, 'folders') and ws.folders:
        for folder in ws.folders:
            all_sheets.extend(get_folder_sheets_recursive(folder.id))
    
    return all_sheets

def get_folder_sheets_recursive(folder_id: int) -> list:
    """Recursively get all sheets from a folder and its subfolders"""
    sheets = []
    
    try:
        folder_contents = SDK.Folders.get_folder(folder_id, include="sheets,folders")
        
        # Add sheets in this folder
        if hasattr(folder_contents, 'sheets') and folder_contents.sheets:
            sheets.extend(folder_contents.sheets)
        
        # Recursively process subfolders
        if hasattr(folder_contents, 'folders') and folder_contents.folders:
            for subfolder in folder_contents.folders:
                sheets.extend(get_folder_sheets_recursive(subfolder.id))
                
    except Exception as e:
        print(f"Warning: Could not access folder {folder_id}: {e}")
    
    return sheets

def detect_rollup_sheets(all_sheets: list) -> list:
    """Automatically detect sheets with cross-sheet formulas (rollup sheets)"""
    rollup_sheets = []
    
    for sheet in all_sheets:
        try:
            # Get sheet with formulas to check for cross-sheet references
            sheet_data = SDK.Sheets.get_sheet(sheet.id, include="formulas", level=1)
            
            # Look for formulas with cross-sheet references (contain {})
            has_cross_sheet_formulas = False
            
            if hasattr(sheet_data, 'rows') and sheet_data.rows:
                for row in sheet_data.rows:
                    if hasattr(row, 'cells') and row.cells:
                        for cell in row.cells:
                            if hasattr(cell, 'formula') and cell.formula:
                                # Check if formula contains cross-sheet references {}
                                if '{' in cell.formula and '}' in cell.formula:
                                    has_cross_sheet_formulas = True
                                    break
                    if has_cross_sheet_formulas:
                        break
            
            if has_cross_sheet_formulas:
                rollup_sheets.append(sheet.id)
                print(f"🔗 Auto-detected rollup sheet: {sheet.name} (ID: {sheet.id})")
                
        except Exception as e:
            # Skip sheets we can't access (permissions, etc.)
            continue
    
    return rollup_sheets

def save_rollup_config(rollup_ids: list):
    """Save detected rollup IDs to a config file"""
    import json
    config = {
        "auto_detected_rollup_ids": rollup_ids,
        "detected_at": dt.datetime.now().isoformat(),
        "count": len(rollup_ids)
    }
    
    with open("auto_rollup_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"✅ Auto-detected {len(rollup_ids)} rollup sheets saved to auto_rollup_config.json")

def build_index(workspace_id: int, since_cache: dict) -> tuple[dict, dict]:
    # Get ALL sheets recursively from workspace and nested folders
    all_sheets = get_all_sheets_recursive(workspace_id)
    mapping, last_seen = {}, {}
    
    for s in all_sheets:
        mod = s.modified_at.isoformat()
        last_seen[str(s.id)] = mod
        # skip unchanged sheets
        if since_cache.get(str(s.id)) == mod:
            continue

        # 1️⃣ look for Sheet‑Summary field "OriginalSheetId"
        src_id = None
        try:
            meta = SDK.Sheets.get_sheet_summary_fields(s.id)
            if hasattr(meta, 'data'):
                summary_fields = meta.data
            else:
                summary_fields = meta
            summary = {f.title: getattr(f, 'display_value', getattr(f, 'value', None)) for f in summary_fields}
            src_id = summary.get("OriginalSheetId")
        except Exception:
            pass

        # 2️⃣ fallback: naming convention
        if not src_id:
            src_id = str(s.id) if 'template' in s.name.lower() else normalise(s.name)

        mapping.setdefault(src_id, []).append(int(s.id))
    return mapping, last_seen

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", type=int, default=WS_ID)
    ap.add_argument("--out", default="mapping.json")
    ap.add_argument("--since-cache", default="last_seen.json")
    ap.add_argument("--detect-rollups", action="store_true", 
                    help="Auto-detect sheets with cross-sheet formulas")
    args = ap.parse_args()

    since_cache = load_cache(Path(args.since_cache))
    mapping, last_seen = build_index(args.workspace, since_cache)

    save_json(Path(args.out), mapping)
    save_json(Path(args.since_cache), last_seen)
    print(f"✓ mapping.json refreshed — {len(mapping)} template groups")
    
    # Auto-detect rollup sheets if requested
    if args.detect_rollups:
        print("🔍 Auto-detecting rollup sheets with cross-sheet formulas...")
        all_sheets = get_all_sheets_recursive(args.workspace)
        rollup_ids = detect_rollup_sheets(all_sheets)
        save_rollup_config(rollup_ids)
