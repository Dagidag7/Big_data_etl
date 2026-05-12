import json

# Read original JSON
with open("data/airports.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Convert to JSON lines
with open("data/airports_clean.json", "w", encoding="utf-8") as f:
    for code, airport in data.items():

        airport["airport_code"] = code

        f.write(json.dumps(airport) + "\n")

print("Conversion completed successfully!")