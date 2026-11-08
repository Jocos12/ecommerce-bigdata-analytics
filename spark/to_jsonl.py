# spark/to_jsonl.py : convertit les gros JSON (tableau) en JSON Lines
import json, os, glob

os.makedirs("data/jsonl", exist_ok=True)

for path in sorted(glob.glob("data/sessions_*.json")) + ["data/transactions.json"]:
    name = os.path.basename(path).replace(".json", ".jsonl")
    with open(path, encoding="utf-8") as f:
        records = json.load(f)
    with open(os.path.join("data/jsonl", name), "w", encoding="utf-8") as out:
        for r in records:
            out.write(json.dumps(r) + "\n")
    print(f"{path} -> data/jsonl/{name} ({len(records)} lignes)")
    del records