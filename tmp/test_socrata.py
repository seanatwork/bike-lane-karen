import requests
import os

s = requests.Session()
token = os.getenv("AUSTINAPIKEY", "")
headers = {"Accept": "application/json", "User-Agent": "test"}
if token:
    headers["X-App-Token"] = token
s.headers.update(headers)

# Build the request manually to see the URL
req = requests.Request(
    "GET",
    "https://data.austintexas.gov/resource/t99n-5ib4.json",
    params={
        "$select": "date_trunc_y(date_of_incident) as year, count(*) as cnt",
        "$group": "year",
        "$order": "year ASC",
        "$limit": 50,
    },
)
prepared = s.prepare_request(req)
print("URL:", prepared.url)

# Now actually send it
resp = s.send(prepared, timeout=30)
print("Status:", resp.status_code)
if resp.ok:
    data = resp.json()
    print("Rows:", len(data))
    print(data[:3])
else:
    print("Error:", resp.text[:500])