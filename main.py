import uvicorn
import pyodbc
import json
from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import time
import functools


def setupDb():
    f = open("cred.env")
    username = f.readline().strip()
    pwd = f.readline().strip()

    cnxn = pyodbc.connect(
        'Driver={ODBC Driver 17 for SQL Server};Server=tcp:benjymaps.database.windows.net,1433;Database=OsloMap;Uid=' + username + ';Pwd=' + pwd+ ';')
    cursor = cnxn.cursor()
    return cursor

@functools.lru_cache(1)
def map_bydeler(cursor) -> dict:
    bydeler = {}
    cursor.execute("SELECT * FROM Bydel;")
    db_data = cursor.fetchall()
    for row in db_data:
        bydeler[row[0]] = {"BydelNavn" : row[1], "BydelFarge" : row[2]}
    return bydeler

@functools.lru_cache(1)
def map_grunnkretser(cursor) -> dict:
    grunnkretser = {}
    cursor.execute("SELECT * FROM Grunnkrets;")
    db_data = cursor.fetchall()
    for row in db_data:
        grunnkretser[row[0]] = {"GrunnkretsNavn" : row[1], "BydelID" : row[3]}
    return grunnkretser

cursor = setupDb()

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/grense/{grense_id}")
def get_grense(grense_id: int):
    grenser = {"Grenser" : []}
    cursor.execute("SELECT * FROM GrenseKoordinat WHERE GrenseID = " + str(grense_id) + ";")
    db_data = cursor.fetchall()
    d = {"GrenseID" : grense_id, "Koordinater" : []}
    for row in db_data:
        d["Koordinater"].append((float(row[2]), float(row[3])))
    grenser["Grenser"].append(d)
    return json.dumps(grenser)

cached_grenser = None
@app.get("/grenser/grunnkrets")
def get_grunnkrets_grenser():
    global cached_grenser
    if cached_grenser == None:
        tic = time.perf_counter()
        grenser = {"Grenser": []}
        cursor.execute("SELECT * FROM GrenseKoordinat WHERE GrenseID IN (Select GrenseID From Grense Where OmraadeType = 1);")
        db_data = cursor.fetchall()
        grense_dict = {}
        for row in db_data:
            if row[0] not in grense_dict:
                grense_dict[row[0]] = []
            grense_dict[row[0]].append((float(row[2]), float(row[3])))
        cursor.execute("SELECT GrenseId, OmraadeID FROM Grense Where OmraadeType = 1;")
        db_data = cursor.fetchall()
        grense_mapping = {}
        for mapping in db_data:
            grense_mapping[mapping[0]] = mapping[1]
        grunnkretser = map_grunnkretser(cursor)
        bydeler = map_bydeler(cursor)
        for grense in grense_dict:
            grunnkretsID = grense_mapping[grense]
            bydelID = grunnkretser[grunnkretsID]["BydelID"]
            d = {"GrunnkretsID": grunnkretsID, "Koordinater": [],
                 "GrunnkretsNavn" : grunnkretser[grunnkretsID]["GrunnkretsNavn"], "BydelID" : bydelID,
                 "BydelNavn": bydeler[bydelID]["BydelNavn"], "BydelFarge" : bydeler[bydelID]["BydelFarge"]}
            for row in grense_dict[grense]:
                d["Koordinater"].append(row)
            grenser["Grenser"].append(d)
        toc = time.perf_counter()
        print(f"Downloaded the tutorial in {toc - tic:0.4f} seconds")
        cached_grenser = json.dumps(grenser)
    return cached_grenser

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=5000, log_level="info")
