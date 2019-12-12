import os
import json
import sqlite3
import csv
from yelpapi import YelpAPI
import requests
import math
import plotly.express as px
import plotly.graph_objects as go
from tabulate import tabulate

ZOMATO_APIKEY = "" #SET API KEY
YELP_APIKEY = "" #SET API KEY

yelp_api = YelpAPI(YELP_APIKEY)

def main():
    conn = sqlite3.connect("yemato.db")
    cur = conn.cursor()
    readDataFromFile(conn,cur, "worldcities.csv")
    while True:
        option = ""
        cityName = ""
        cityId = None
        text = """Welcome to Yemato!\nTo find the best restaurants validated by both Zomato and Yelp, press 1.(RECOMMENDED FOR CALLS TO RECORDED CITIES. SEARCH DEERFIELD FOR EXAMPLE.) \nTo get rid of the call limit(20 calls by the project spec) and produce result for option 1, press 1.5.\nTo see visualization #1, press 2.\nTo see visualization #2, press 3.\nTo see visualization #3, press 4.\nTo output a calculation text file, press 5 and check the directory.\n"""
        
        stateText = """ Please type in the state name to search for!(Full Name, case irrelevant)\n"""

        cityText = """ Please type in city to search for!(e.g. Ann Arbor, Deerfield, etc)\n"""
        
        while option not in [1.0, 1.5, 2.0, 3.0, 4.0, 5.0]:
            option = float(input(text))
            text = "Wrong Option! Please try again.\n"
        
        if option == 1:
            while cityId == None:
                cityName = input(cityText)
                stateName = input(stateText)
                cityId = getCoordinate(cityName.lower(), stateName.lower(), cur)
                cityText = "We don't have that city in our database:( Please try again. "
            checkOverlappingRestaurants(cur, conn, cityId)
        elif option ==1.5:
            while cityId == None:
                cityName = input(cityText)
                stateName = input(stateText)
                cityId = getCoordinate(cityName.lower(), stateName.lower(), cur)
                cityText = "We don't have that city in our database:( Please try again. "
            checkOverlappingRestaurants(cur, conn, cityId, limiter=50)
        elif option == 2:
            while cityId == None:
                cityName = input(cityText)
                stateName = input(stateText)
                cityId = getCoordinate(cityName.lower(), stateName.lower(), cur)
                cityText = "We don't have that city in our database:( Please try again. "
            visualization1(cur, conn, cityId, cityName)
        elif option == 3:
            while cityId == None:
                cityName = input(cityText)
                stateName = input(stateText)
                cityId = getCoordinate(cityName.lower(), stateName.lower(), cur)
                cityText = "We don't have that city in our database:( Please try again. "
            visualization2(cityId, cur, cityName)
        elif option == 4:
            visualization3(cur)
        elif option == 5:
            calculations(cur)
        else:
            print("Error occured.") 
            break


def readDataFromFile(conn, cur, filename):
    cur.execute("DROP TABLE IF EXISTS Cities")
    cur.execute("CREATE TABLE Cities (locationid INTEGER PRIMARY KEY, city TEXT, lat REAL, lng REAL, state TEXT, country TEXT, iso TEXT)")
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path, 'r')
    csv_reader = csv.DictReader(f)
    for row in csv_reader:
        #2nd=City, 3rd=lat, 4th=lng, 5th=country, 7th=iso3
        city = row["city_ascii"].lower()
        lat = float(row["lat"])
        lng = float(row["lng"])
        country = row["country"]
        state = row["admin_name"].lower()
        iso = row["iso3"]
        if iso == "USA":
            cur.execute("INSERT INTO Cities (city, lat, lng, state, country, iso) VALUES (?, ?, ?, ?, ?, ?)", (city, lat, lng, state, country, iso))
    f.close()
    conn.commit()

def zomatoAPI_queryCity(cur, con, coordinate, page=0, limit=10):
    offset = page * 20
    cur.execute("CREATE TABLE IF NOT EXISTS zomatoPlaces (zomatoId INTEGER PRIMARY KEY, locationid INTEGER, name TEXT, rating REAL, address TEXT, lat REAL, lng REAL, UNIQUE(zomatoId))")
    cur.execute("CREATE TABLE IF NOT EXISTS zomatoPlacesInfo (zomatoId INTEGER PRIMARY KEY, cuisine TEXT, address TEXT , UNIQUE(zomatoId))")

    headers = {'Accept': 'application/json', 'user-key': ZOMATO_APIKEY}
    base_url = "https://developers.zomato.com/api/v2.1/"
    r = (requests.get(base_url + "search?lat=" + str(coordinate[0]) + "&start=" + str(offset) + "&lon=" + str(coordinate[1]) + "&count=" + str(limit) + "&sort=rating&order=desc", headers=headers).content).decode("utf-8")
    data = json.loads(r)
    for x in data["restaurants"]:
        place = x["restaurant"]
        name = place["name"]
        address = place["location"]["address"]
        rating = place["user_rating"]["aggregate_rating"]
        cuisine = place["cuisines"]
        lat = float(place["location"]["latitude"])
        lng = float(place["location"]["longitude"])
        zomatoId = int(place["id"])
        cur.execute("INSERT OR IGNORE INTO zomatoPlaces (zomatoId, locationid, name, rating, address, lat, lng) VALUES (?, ?, ?, ?, ?, ?, ?)", (zomatoId, coordinate[2], name, rating, address, lat, lng))
        cur.execute("INSERT OR IGNORE INTO zomatoPlacesInfo (zomatoId, cuisine, address) VALUES (?, ?, ?)", (zomatoId, cuisine, address))
    con.commit()
    
def yelpAPI_queryCity(cur, con, coordinate, page=0, limit=10):
    offset = page * 20
    cur.execute("CREATE TABLE IF NOT EXISTS yelpPlaces (yelpId TEXT PRIMARY KEY, locationid INTEGER, name TEXT, rating REAL, address TEXT, lat REAL, lng REAL, UNIQUE(yelpId))")
    cur.execute("CREATE TABLE IF NOT EXISTS yelpPlacesInfo (yelpId TEXT PRIMARY KEY, cuisine TEXT, address TEXT, UNIQUE(yelpId))")

    res = yelp_api.search_query(latitude=coordinate[0], longitude=coordinate[1], offset=offset, sort_by="rating", limit=limit, categories="restaurants")
    for x in res["businesses"]:
        name = x["name"]
        rating = x["rating"]
        try:
            address = x["location"]["address1"] + ", " + x["location"]["city"] + " " + x["location"]["zip_code"]
        except: 
            address = ""
    
        cuisine = x["categories"][0]["alias"]
        lat = float(x["coordinates"]["latitude"])
        lng = float(x["coordinates"]["longitude"])
        yelpId = x["id"]
        cur.execute("INSERT OR IGNORE INTO yelpPlaces (yelpId, locationid, name, rating, address, lat, lng) VALUES ( ?, ?, ?, ?, ?, ?, ?)", (yelpId, coordinate[2], name, rating, address, lat, lng))
        cur.execute("INSERT OR IGNORE INTO yelpPlacesInfo (yelpId, cuisine, address) VALUES (?, ?, ?)", (yelpId, cuisine, address))
    con.commit()

def getCoordinate(q, state, cur):
    cur.execute("SELECT lat, lng, locationid FROM Cities WHERE city=? AND state=?", (q, state))
    return cur.fetchone()

def checkOverlappingRestaurants(cur, con, coord, limiter=1):
    res = []
    iteration = 0
    while len(res) <= 5 and iteration < limiter:
        if iteration == 0:
            print("fetching data...")
        elif iteration % 15 == 0:
            print("still fetching data...")
        if len(res) >= 5:
            break
        try:
            yelpAPI_queryCity(cur, con, coord, page=iteration)
            zomatoAPI_queryCity(cur, con, coord, page=iteration)
        except:
            cur.execute("SELECT zomatoPlaces.name, zomatoPlaces.address, zomatoPlaces.rating, yelpPlaces.rating FROM zomatoPlaces INNER JOIN yelpPlaces ON ((zomatoPlaces.lat = yelpPlaces.lat AND zomatoPlaces.lng = yelpPlaces.lng) OR zomatoPlaces.name = yelpPlaces.name OR zomatoPlaces.address = yelpPlaces.address) AND yelpPlaces.locationid={} AND zomatoPlaces.locationid={} AND yelpPlaces.locationid = zomatoPlaces.locationid".format(coord[2], coord[2]))
            for x in cur.fetchall():
                if x not in res:
                    res.append(x)
            break 
        cur.execute("SELECT zomatoPlaces.name, zomatoPlaces.address, zomatoPlaces.rating, yelpPlaces.rating FROM zomatoPlaces INNER JOIN yelpPlaces ON ((zomatoPlaces.lat = yelpPlaces.lat AND zomatoPlaces.lng = yelpPlaces.lng) OR zomatoPlaces.name = yelpPlaces.name OR zomatoPlaces.address = yelpPlaces.address) AND yelpPlaces.locationid={} AND zomatoPlaces.locationid={} AND yelpPlaces.locationid = zomatoPlaces.locationid".format(coord[2], coord[2]))
        for x in cur.fetchall():
            if x not in res:
                res.append(x)
        iteration += 1
    if len(res) == 0:
        print("No Matching Data! Zomato and Yelp does not seem to have much matching data...")
    else:
        ratings = []
        names = []
        address = []
        for x in res: 
            names.append(x[0])
            address.append(x[1])
            ratings.append((float(x[3])+float(x[2]))/2)
        print("****** TOP RESTAURANTS ******")   
        print(tabulate({"Name": names, "Address": address, "True Rating": ratings}, headers="keys"))

def visualization1Data(cur, con, coord, numData=100, limiter=1):
    res = []
    iteration = 0
    while len(res) <= numData and iteration < limiter:
        if iteration == 0:
            print("fetching data...")
        elif iteration % 15 == 0:
            print("still fetching data...")
        
        try:
            yelpAPI_queryCity(cur, con, coord, page=iteration)
            zomatoAPI_queryCity(cur, con, coord, page=iteration)
        except:
            cur.execute("SELECT zomatoPlaces.name, zomatoPlaces.address, zomatoPlaces.rating, yelpPlaces.rating FROM zomatoPlaces INNER JOIN yelpPlaces ON ((zomatoPlaces.lat = yelpPlaces.lat AND zomatoPlaces.lng = yelpPlaces.lng) OR zomatoPlaces.name = yelpPlaces.name OR zomatoPlaces.address = yelpPlaces.address) AND yelpPlaces.locationid={} AND zomatoPlaces.locationid={} AND yelpPlaces.locationid = zomatoPlaces.locationid".format(coord[2], coord[2]))
            for x in cur.fetchall():
                if x not in res:
                    res.append(x)
            break
        cur.execute("SELECT zomatoPlaces.name, zomatoPlaces.address, zomatoPlaces.rating, yelpPlaces.rating FROM zomatoPlaces INNER JOIN yelpPlaces ON ((zomatoPlaces.lat = yelpPlaces.lat AND zomatoPlaces.lng = yelpPlaces.lng) OR zomatoPlaces.name = yelpPlaces.name OR zomatoPlaces.address = yelpPlaces.address) AND yelpPlaces.locationid={} AND zomatoPlaces.locationid={} AND yelpPlaces.locationid = zomatoPlaces.locationid".format(coord[2], coord[2]))
        for x in cur.fetchall():
            if x not in res:
                res.append(x)
        iteration += 1
    return res

def visualization1(cur, con, coord, q):
    data = visualization1Data(cur, con, coord)
    x_axis = "Rating Difference"
    y_axis = "# of restaurants"
    title = "Rating Difference in {}".format(q)
    dic = {0.0:0, 0.5:0, 1.0:0, 1.5:0, 2.0:0, 2.5:0, 3.0:0, 3.5:0, 4.0:0, 4.5:0, 5.0:0 }
    for x in data:
        diff = roundtofirstDecimal(abs(float(x[2]) - float(x[3])))
        dic[diff] = dic.get(diff, 0) + 1

    fig = go.Figure([go.Bar(x=list(dic.keys()), y=list(dic.values()))])
    fig.update_traces(marker_color='rgb(158,202,225)', marker_line_color='rgb(20,248,140)',
                  marker_line_width=2, opacity=1)
    fig.update_xaxes(title_text=x_axis)
    fig.update_yaxes(title_text=y_axis)
    fig.update_layout(title=title, yaxis_zeroline=True, xaxis_zeroline=True)
    fig.show()

def visualization2(coord, cur, q):
    yelpCommand = "SELECT rating, COUNT(*) FROM yelpPlaces WHERE locationid ={} GROUP BY rating".format(coord[2])
    zomatoCommand = "SELECT rating, COUNT(*) FROM yelpPlaces WHERE locationid ={} GROUP BY rating".format(coord[2])
    cur.execute(yelpCommand)
    yelpRating = cur.fetchall()
    cur.execute(zomatoCommand)
    zomatoRating = cur.fetchall()
    temp = yelpRating + zomatoRating
    res = {}
    for x in temp:
        res[x[0]] = res.get(x[0],0) + x[1]
    x_axis = "ratings"
    y_axis = "# of restaurants"
    title = "Distribution of Ratings in {}".format(q)
    fig = go.Figure([go.Bar(x=list(res.keys()), y=list(res.values()))])
    fig.update_xaxes(title_text=x_axis)
    fig.update_yaxes(title_text=y_axis)
    fig.update_traces(marker_color='rgb(30,40,220)', marker_line_color='rgb(30,40,220)',
                  marker_line_width=1.5, opacity=1)
    fig.update_layout(title=title, yaxis_zeroline=True, xaxis_zeroline=True)
    fig.show()
    
    

def visualization3(cur):
    unmatchedCmd1 = "SELECT COUNT(*) FROM yelpPlaces"
    unmatchedCmd2 = "SELECT COUNT(*) FROM zomatoPlaces"
    matchedCmd = "SELECT  COUNT(*) FROM zomatoPlaces INNER JOIN yelpPlaces ON (zomatoPlaces.lat = yelpPlaces.lat AND zomatoPlaces.lng = yelpPlaces.lng) OR zomatoPlaces.name = yelpPlaces.name OR zomatoPlaces.address = yelpPlaces.address"
    cur.execute(unmatchedCmd1)
    unmatched = cur.fetchone()[0]
    cur.execute(unmatchedCmd2)
    um2 = cur.fetchone()[0]
    cur.execute(matchedCmd)
    matched = cur.fetchone()[0]
    
    labels = ['Unmatched Records','Matched Records']
    values = [unmatched+um2-matched, matched]

    fig = go.Figure(data=[go.Pie(labels=labels, values=values)])
    colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
    fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=20,
                  marker=dict(colors=colors, line=dict(color='#000000', width=2)))
    fig.update_layout(
    title_text="Matching Restaurants in Database"
                  )
    fig.show()

def calculations(cur):
    unmatchedCmd1 = "SELECT COUNT(*) FROM yelpPlaces"
    unmatchedCmd2 = "SELECT COUNT(*) FROM zomatoPlaces"
    matchedCmd = "SELECT  COUNT(*) FROM zomatoPlaces INNER JOIN yelpPlaces ON (zomatoPlaces.lat = yelpPlaces.lat AND zomatoPlaces.lng = yelpPlaces.lng) OR zomatoPlaces.name = yelpPlaces.name OR zomatoPlaces.address = yelpPlaces.address"
    cur.execute(unmatchedCmd1)
    unmatched = cur.fetchone()[0]
    cur.execute(unmatchedCmd2)
    um2 = cur.fetchone()[0]
    cur.execute(matchedCmd)
    matched = cur.fetchone()[0]
    
    yelpCommand = "SELECT rating, COUNT(*) FROM yelpPlaces WHERE locationid =4386 GROUP BY rating"
    zomatoCommand = "SELECT rating, COUNT(*) FROM zomatoPlaces WHERE locationid =4386 GROUP BY rating"
    cur.execute(yelpCommand)
    yelpRating = cur.fetchall()
    cur.execute(zomatoCommand)
    zomatoRating = cur.fetchall()
    temp = yelpRating + zomatoRating
    res = {}
    for x in temp:
        res[x[0]] = res.get(x[0],0) + x[1]

    yelpCommand = "SELECT rating, COUNT(*) FROM yelpPlaces GROUP BY rating"
    zomatoCommand = "SELECT rating, COUNT(*) FROM zomatoPlaces GROUP BY rating"
    cur.execute(yelpCommand)
    yelpRating2 = cur.fetchall()
    cur.execute(zomatoCommand)
    zomatoRating2 = cur.fetchall()
    temp2 = yelpRating2 + zomatoRating2
    res2 = {}
    for x in temp2:
        res2[x[0]] = res2.get(x[0],0) + x[1]
    
    allR = unmatched + um2
    um = unmatched + um2 - matched
    firstTitle = "Performance Overview\n"
    output = tabulate({"Percentage Of Matched Restaurants": [str(round(matched/allR, 5)) + "%"],"Matched Number of Restaurants":[matched], "Unmatched Number of Restuarants": [um], "Total Number of Restaurants": [allR]}, headers="keys")
    
    first = 0
    second = 0
    third = 0
    fourth = 0
    fifth = 0
    total = 0
    for x in res.items():
        val = int(x[1])
        rating = int(x[0])
        total += val
        if rating < 1:
            first += val
        elif rating < 2:
            second += val
        elif rating < 3:
            third += val
        elif rating < 4:
            fourth += val
        else:
            fifth += val
    
    first2 = 0
    second2 = 0
    third2 = 0
    fourth2 = 0
    fifth2 = 0
    total2 = 0
    for x in res2.items():
        val = int(x[1])
        rating = int(x[0])
        total2 += val
        if rating < 1:
            first2 += val
        elif rating < 2:
            second2 += val
        elif rating < 3:
            third2 += val
        elif rating < 4:
            fourth2 += val
        else:
            fifth2 += val

    

    secondTitle = "Rating Percentages for Ann Arbor\n"
    dic = {"0-1": [str(round(first/total * 100, 2 )) + "%"], "1-2": [str(round(second/total * 100, 2 )) + "%"], "2-3": [str(round(third/total * 100, 2 )) + "%"], "3-4": [str(round(fourth/total * 100, 2 )) + "%"], "4-5": [str(round(fifth/total * 100, 2 )) + "%"]}
    output2 = tabulate(dic, headers="keys")
    
    thirdTitle = "Rating Percentages for All Recorded Restaurants\n"
    dic2 = {"0-1": [str(round(first2/total2 * 100, 2 )) + "%"], "1-2": [str(round(second2/total2 * 100, 2 )) + "%"], "2-3": [str(round(third2/total2 * 100, 2 )) + "%"], "3-4": [str(round(fourth2/total2 * 100, 2 )) + "%"], "4-5": [str(round(fifth2/total2 * 100, 2 )) + "%"]}
    output3 = tabulate(dic2, headers="keys")
    with open("calculations.txt", "w") as f:
        f.write(firstTitle)
        f.write(output)

        f.write("\n\n\n")

        f.write(secondTitle)
        f.write(output2)

        f.write("\n\n\n")

        f.write(thirdTitle)
        f.write(output3)

def roundtofirstDecimal(num):
    return round(num,1)

def roundtoHalf(num):
    return round(num*2) / 2

main()