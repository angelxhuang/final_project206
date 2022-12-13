import requests
import json
import sqlite3
from xml.sax import parseString
from bs4 import BeautifulSoup
import re
import os
import csv
import unittest
import matplotlib.pyplot as plt
import numpy as np


# Input: Database name
# Output: Creates a database
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

# Input: Dictionaries that you want to combine
# Output: List
def combine(dict1, dict2, dict3, dict4, dict5, dict6):
    combine_list = []
    for i in dict1['businesses']:
        combine_list.append(i)
    for i in dict2['businesses']:
        combine_list.append(i)
    for i in dict3['businesses']:
        combine_list.append(i)    
    for i in dict4['businesses']:
        combine_list.append(i)
    for i in dict5['businesses']:
        combine_list.append(i)  
    for i in dict6['businesses']:
        combine_list.append(i)          
    return combine_list

# Input: Location, Offset
# Output: Dictionary
def yelp_cafes(location, offset):
    url = f'https://api.yelp.com/v3/businesses/search?location={location}&categories=cafe&sort_by=best_match&limit=25&offset={offset}'
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer NHV7eR1EelF7RQYwG61LN1ODa4pLvqxqs6LMCpoxaQ4vEboO91EEyH7jdTgpgrG2GkT5p8dagosuw9FGeNF_rAwlvgKS0UeMyIpr2fqE74o4Lj6Hk6suedQx2cqQY3Yx"
    }
    response = requests.get(url, headers=headers)
    data = response.text
    cafes_lst = json.loads(data)
    return cafes_lst

# Input: Dictionary, Cur, Conn
# Output: A table based off Yelp cafes data, sorted by city, cafe name, rating, price level
def create_yelp_cafes(cafes, cur, conn):
    cur.execute("DROP TABLE IF EXISTS cafes")
    cur.execute("CREATE TABLE cafes (city TEXT, name TEXT, rating INTEGER, price TEXT)")
    # print(len(cafes))
    for i in cafes:
        city = i["location"]["city"]
        name = i["name"]
        rating = i["rating"]
        if i.get("price", "none") == "$":
            price = 1
        elif i.get("price", "none") == "$$":
            price = 2
        elif i.get("price", "none") == "$$$":
            price = 3
        elif i.get("price", "none") == "$$$$":
            price = 4
        else:
            price = 0
        cur.execute("INSERT INTO cafes (city,name,rating,price) VALUES (?,?,?,?)",(city, name, rating, price))  
    conn.commit()
    

# Input: Location, Offset
# Output: Dictionary
def yelp_restaurants(location, offset):
    url = f'https://api.yelp.com/v3/businesses/search?location={location}&categories=restaurants&sort_by=best_match&limit=25&offset={offset}'
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer NHV7eR1EelF7RQYwG61LN1ODa4pLvqxqs6LMCpoxaQ4vEboO91EEyH7jdTgpgrG2GkT5p8dagosuw9FGeNF_rAwlvgKS0UeMyIpr2fqE74o4Lj6Hk6suedQx2cqQY3Yx"
    }
    response = requests.get(url, headers=headers)
    data = response.text
    restaurant_lst = json.loads(data)
    return restaurant_lst

# Input: Dictionary, Cur, Conn
# Output: A table based off Yelp restaurants data, sorted by city, restaurant name, rating, price level
def create_yelp_restaurants(restaurants, cur, conn):
    cur.execute("DROP TABLE IF EXISTS restaurants")
    cur.execute("CREATE TABLE restaurants (city TEXT, name TEXT, rating INTEGER, price TEXT)")
    # print(len(cafes))
    for i in restaurants:
        city = i["location"]["city"]
        name = i["name"]
        rating = i["rating"]
        if i.get("price", "none") == "$":
            price = 1
        elif i.get("price", "none") == "$$":
            price = 2
        elif i.get("price", "none") == "$$$":
            price = 3
        elif i.get("price", "none") == "$$$$":
            price = 4
        else:
            price = 0
        cur.execute("INSERT INTO restaurants (city,name,rating,price) VALUES (?,?,?,?)",(city, name, rating, price))  
    conn.commit()

# Input: None
# Output: Dictionary 
# About: We extracted the first 100 ranked california cities & their population
def cali_ratings():
    url = "https://www.california-demographics.com/cities_by_population"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    cali_dict = {}
    tag = soup.find("table", class_= "ranklist")
    info = tag.find_all("tr")
    for tag in info[1:101]:
        lst = tag.find_all("td")
        city = lst[1].text.strip()
        pop = int(lst[2].text.strip().replace(",", ""))
        cali_dict[city] = pop
    return cali_dict

# Input: Dictionary, Cur, Conn
# Output: A table based off our california city population data, sorted by city and population
def population_table(pop_dict, cur, conn):
    cur.execute("DROP TABLE IF EXISTS population")
    cur.execute("CREATE TABLE population (city TEXT, pop INTEGER)")
    for i in pop_dict:
        cur.execute("INSERT INTO population (city,pop) VALUES (?,?)",(i, pop_dict[i]))  
    conn.commit()

# Input: Cur, Conn
# Output: Dictionary 
# About: We selected city + cafe rating from our cafe data and calculated the average ratings of cafes in each city
def avg_yelp_cafes(cur, conn):
    cur.execute('SELECT city,rating FROM cafes ORDER BY rating DESC')
    avg_cafes_ratings = cur.fetchall()
    conn.commit()
    avg_ratings_dict = {}
    for i in avg_cafes_ratings:
        if i[0] in avg_ratings_dict.keys():
            avg_ratings_dict[i[0]].append(i[1])
        else:
            avg_ratings_dict[i[0]] = [i[1]]
    for x in avg_ratings_dict:
        avg_ratings_dict[x] = round((sum(avg_ratings_dict[x])) / len(avg_ratings_dict[x]), 2)
    avg_ratings_dict = dict(sorted(avg_ratings_dict.items(), key=lambda item: item[1], reverse=True))
    return avg_ratings_dict

# Input: Cur, Conn
# Output: Dictionary 
# About: We selected city + restaurant rating from our restaurant data and calculated the average ratings of restaurants in each city
def avg_yelp_restaurants(cur, conn):
    cur.execute('SELECT city,rating FROM restaurants ORDER BY rating DESC')
    avg_res_ratings = cur.fetchall()
    conn.commit()
    avg_res_dict = {}
    for i in avg_res_ratings:
        if i[0] in avg_res_dict.keys():
            avg_res_dict[i[0]].append(i[1])
        else:
            avg_res_dict[i[0]] = [i[1]]
    for x in avg_res_dict:
        avg_res_dict[x] = round((sum(avg_res_dict[x])) / len(avg_res_dict[x]), 2)
    avg_res_dict = dict(sorted(avg_res_dict.items(), key=lambda item: item[1], reverse=True))
    return avg_res_dict

# Input: Cur, Conn
# Output: Dictionary 
# About: We selected city + cafe price level from our cafe data and calculated the average price level of cafes in each city
def avg_yelp_cafes_price(cur, conn):
    cur.execute('SELECT city,price FROM cafes ORDER BY price DESC')
    avg_cafes_price = cur.fetchall()
    conn.commit()
    c_avg_price_dict = {}
    for i in avg_cafes_price:
        if i[0] in c_avg_price_dict.keys():
            c_avg_price_dict[i[0]].append(int(i[1]))
        else:
            c_avg_price_dict[i[0]] = [int(i[1])]
    for x in c_avg_price_dict:
        c_avg_price_dict[x] = round((sum(c_avg_price_dict[x])) / len(c_avg_price_dict[x]), 2)
    c_avg_price_dict = dict(sorted(c_avg_price_dict.items(), key=lambda item: item[1], reverse=True))
    return c_avg_price_dict

# Input: Cur, Conn
# Output: Dictionary 
# About: We selected city + restaurant price level from our restaurant data and calculated the average price level of restaurants in each city
def avg_yelp_restaurants_price(cur, conn):
    cur.execute('SELECT city,price FROM restaurants ORDER BY price DESC')
    avg_res_price = cur.fetchall()
    conn.commit()
    r_avg_price_dict = {}
    for i in avg_res_price:
        if i[0] in r_avg_price_dict.keys():
           r_avg_price_dict[i[0]].append(int(i[1]))
        else:
            r_avg_price_dict[i[0]] = [int(i[1])]
    print(r_avg_price_dict)
    for x in r_avg_price_dict:
        r_avg_price_dict[x] = round((sum(r_avg_price_dict[x])) / len(r_avg_price_dict[x]), 2)
    r_avg_price_dict = dict(sorted(r_avg_price_dict.items(), key=lambda item: item[1], reverse=True))
    return r_avg_price_dict


# Input: Cur, Conn
# Output: List
# About: Joins restaurants data and population data in a tuple format: (City name, Population, Average restaurant ratings)
def res_pop_join(cur, conn):
    cur.execute('SELECT restaurants.city, population.pop, ROUND(AVG(restaurants.rating), 2) FROM restaurants JOIN population ON restaurants.city = population.city GROUP BY restaurants.city')
    result = cur.fetchall()
    # print(result)
    return result

# Input: Cur, Conn
# Output: Dictionary
# About: Joins restaurants data and population data in a dictionary format: {City name: (Population, Average restaurant price level) 
def res_price_pop_join(cur, conn):
    cur.execute('SELECT restaurants.city, population.pop, ROUND(AVG(restaurants.price), 2) FROM restaurants JOIN population ON restaurants.city = population.city GROUP BY restaurants.city')
    result = cur.fetchall()
    return result


# Input: Cur, Conn
# Output: List
# About: Joins cafe data and population data in a tuple format: (City name, Population, Average cafe ratings)
def cafe_pop_join(cur, conn):
    cur.execute('SELECT cafes.city, population.pop, ROUND(AVG(cafes.rating), 2) FROM cafes JOIN population ON cafes.city = population.city GROUP BY cafes.city')
    result = cur.fetchall()
    print(result)
    return result

# Input: Cur, Conn
# Output: Dictionary
# About: Joins cafe data and population data in a dictionary format: {City name: (Population, Average cafe price level) 
def cafe_price_pop_join(cur, conn):
    cur.execute('SELECT cafes.city, population.pop, ROUND(AVG(cafes.price), 2) FROM cafes JOIN population ON cafes.city = population.city GROUP BY cafes.city')
    result = cur.fetchall()
    return result

# Input: Data table, File name
# Output: CSV File
# About: Writes the csv file for dot plot of "Average Ratings of California Cities Places vs. Population"
def write_csv_dot(data1, filename):
    f = open(filename, "w")
    f.write("Average Ratings of California Cities Places vs. Population")
    f.write('\n')
    f.write("Population, Average Ratings")
    f.write('\n')
    for i in data1:
        f.write(str(i[1]) + "," + str(i[2]))
        f.write('\n')

# Input: Data table, File name
# Output: CSV File
# About: Writes the csv file for dot plot of "Average Price Levels of California Cities Places vs. Population"
def write_csv_dot_price(data1, filename):
    f = open(filename, "w")
    f.write("Average Price Levels of California Cities Places vs. Population")
    f.write('\n')
    f.write("Population, Average Price Levels")
    f.write('\n')
    for i in data1:
        f.write(str(i[1]) + "," + str(i[2]))
        f.write('\n')

# Input: Data table, File name
# Output: CSV File
# About: Writes the csv file for bar plot of "Average Ratings of California Cities Cafes vs. Restaurants"
def write_csv_bar(data1, data2, filename):
    f = open(filename, "w")
    f.write("Average Ratings of California Cities Cafes vs. Restaurants")
    f.write('\n')
    f.write("City, Average Cafe Rating, Average Restaurant Rating")
    f.write('\n')
    for i in data1:
        if (i in data1.keys()) and (i in data2.keys()):
            f.write(i + "," + str(data1[i]) + "," + str(data2[i]))
            f.write('\n')

# Input: Data table, File name
# Output: CSV File
# About: Writes the csv file for bar plot of "Average Price Levels of California Cities Cafes vs. Restaurants"
def write_csv_bar_price(data1, data2, filename):
    f = open(filename, "w")
    f.write("Average Price Levels of California Cities Cafes vs. Restaurants")
    f.write('\n')
    f.write("City, Average Cafe Price Levels, Average Restaurant Price Levels")
    f.write('\n')
    print(data1)
    for i in data1:
        if (i in data1.keys()) and (i in data2.keys()):
            f.write(i + "," + str(data1[i]) + "," + str(data2[i]))
            f.write('\n')

#ratings

# Input: File
# Output: Bar graph
# About: Creates a bar graph of 'Average Cafe and Restaurant Ratings By City'
def cali_bar_graph(file):
    f = open(file)
    lines = f.readlines()
    city = []
    cafe_ratings = []
    res_ratings = []
    for row in lines[2:]:
        value = row.split(",")
        city.append(value[0].strip())
        cafe_ratings.append(float(value[1].strip()))
        res_ratings.append(float(value[2].strip()))
    
    x = np.arange(len(city))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, cafe_ratings, width, label='Cafe Ratings', color=['#3e60ab'])
    rects2 = ax.bar(x + width/2, res_ratings, width, label='Restaurant Ratings', color=['#bd64bd'])

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Ratings')
    ax.set_title('Average Cafe and Restaurant Ratings By City')
    ax.set_xticks(x, city)
    ax.set_xticklabels(ax.get_xticklabels(), rotation = 90)

    ax.legend()

    # ax.bar_label(rects1, padding=10)
    # ax.bar_label(rects2, padding=10)

    fig.tight_layout()

    plt.show()

# Input: Cafe file and Restaurant file
# Output: Scatter plot
# About: Creates a scatterplot of 'City Population vs. Average Cafe and Restaurant Ratings'
def cali_dot_plot(file1, file2):
    f1 = open(file1)
    lines1 = f1.readlines()
    cafes_population = []
    cafes_avg_ratings = []
    for row in lines1[2:]:
        value = row.split(",")
        cafes_population.append(int(value[0].strip()))
        cafes_avg_ratings.append(float(value[1].strip()))
    # print(cafes_population)
    # print(cafes_avg_ratings)
    f1.close()

    f2 = open(file2)
    lines2 = f2.readlines()
    res_population = []
    res_avg_ratings = []
    for row in lines2[2:]:
        value = row.split(",")
        res_population.append(int(value[0].strip()))
        res_avg_ratings.append(float(value[1].strip()))
    # print(res_population)
    # print(res_avg_ratings)
    f2.close()

    # First Scatter plot
    fig, ax = plt.subplots()
    ax.scatter(cafes_population, cafes_avg_ratings, edgecolor ="black", c=['#3e60ab'])

    # Second Scatter plot
    ax.scatter(res_population, res_avg_ratings, edgecolor ="red", c=['#bd64bd'])

    ax.legend(["Cafe", "Restaurant"])
    ax.set_title('City Population vs. Average Cafe and Restaurant Ratings')
    ax.set_xlabel('City Population (in millions)')
    ax.set_ylabel('Average Ratings')
    plt.show()

#price

# Input: File
# Output: Bar graph
# About: Creates a bar graph of 'Average Cafe and Restaurant Price Level By City'
def cali_price_bar_graph(file):
    f = open(file)
    lines = f.readlines()
    city = []
    cafe_price = []
    res_price = []
    for row in lines[2:]:
        value = row.split(",")
        city.append(value[0].strip())
        cafe_price.append(float(value[1].strip()))
        res_price.append(float(value[2].strip()))
    
    x = np.arange(len(city))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, cafe_price, width, label='Cafe Price Level', color=['#6bd186'])
    rects2 = ax.bar(x + width/2, res_price, width, label='Restaurant Price Level', color=['#eddb3b'])

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Price Level')
    ax.set_title('Average Cafe and Restaurant Price Level By City')
    ax.set_xticks(x, city)
    ax.set_xticklabels(ax.get_xticklabels(), rotation = 90)

    
    ax.legend()

    # ax.bar_label(rects1, padding=10)
    # ax.bar_label(rects2, padding=10)

    fig.tight_layout()

    plt.show()


# Input: Cafe file and Restaurant file
# Output: Scatter plot
# About: Creates a scatterplot of 'City Population vs. Average Cafe and Restaurant Price Level'
def cali_price_dot_plot(file1, file2):
    f1 = open(file1)
    lines1 = f1.readlines()
    cafes_population = []
    cafes_avg_price = []
    for row in lines1[2:]:
        value = row.split(",")
        cafes_population.append(int(value[0].strip()))
        cafes_avg_price.append(float(value[1].strip()))
    print(cafes_population)
    print(cafes_avg_price)
    f1.close()

    f2 = open(file2)
    lines2 = f2.readlines()
    res_population = []
    res_avg_price = []
    for row in lines2[2:]:
        value = row.split(",")
        res_population.append(int(value[0].strip()))
        res_avg_price.append(float(value[1].strip()))
    print(res_population)
    print(res_avg_price)
    f2.close()

    # First Scatter plot
    fig, ax = plt.subplots()
    ax.scatter(cafes_population, cafes_avg_price, edgecolor ="black", c=['#6bd186'])

    # Second Scatter plot
    ax.scatter(res_population, res_avg_price, edgecolor ="red", c=['#eddb3b'])

    ax.legend(["Cafe", "Restaurant"])
    ax.set_title('City Population vs. Average Cafe and Restaurant Price Level')
    ax.set_xlabel('City Population (in millions)')
    ax.set_ylabel('Average Price Level')
    plt.show()

# Calls all of our main functions
def main():
    cur, conn = setUpDatabase("california.db")
    first_cafe = yelp_cafes("san+francisco", 0)
    second_cafe = yelp_cafes("silicon+valley", 25)
    third_cafe = yelp_cafes("southern+california", 25)
    fourth_cafe = yelp_cafes("bay+area", 25)
    fifth_cafe = yelp_cafes("california", 25)
    sixth_cafe = yelp_cafes("los+angeles", 25)
    cafes = combine(first_cafe, second_cafe, third_cafe, fourth_cafe, fifth_cafe, sixth_cafe)
    first_restaurant = yelp_restaurants("san+francisco", 0)
    second_restaurant = yelp_restaurants("silicon+valley", 25)
    third_restaurant = yelp_restaurants("bay+area", 25)
    fourth_restaurant = yelp_restaurants("southern+california", 25)
    fifth_restaurant = yelp_restaurants("california", 25)
    sixth_restaurant = yelp_restaurants("los+angeles", 25)
    restaurants = combine(first_restaurant, second_restaurant, third_restaurant, fourth_restaurant, fifth_restaurant, sixth_restaurant)
    pop_dict = cali_ratings()
    population_table(pop_dict, cur, conn)
    create_yelp_cafes(cafes, cur, conn)
    create_yelp_restaurants(restaurants, cur, conn)
    cafes_avg = avg_yelp_cafes(cur, conn)
    restaurants_avg = avg_yelp_restaurants(cur, conn)
    res_pop = res_pop_join(cur, conn)
    cafe_pop = cafe_pop_join(cur, conn)
    write_csv_dot(res_pop, "res_population.csv")
    write_csv_dot(cafe_pop, "cafes_population.csv")
    write_csv_bar(cafes_avg, restaurants_avg, "ratings_by_city.csv")
    cali_bar_graph("ratings_by_city.csv")
    cali_dot_plot("cafes_population.csv", "res_population.csv")
    res_price_avg = avg_yelp_restaurants_price(cur, conn)
    cafe_price_avg = avg_yelp_cafes_price(cur, conn)
    res_price = res_price_pop_join(cur, conn)
    cafe_price = cafe_price_pop_join(cur, conn)
    write_csv_dot_price(res_price, "res_price_pop.csv")
    write_csv_dot_price(cafe_price, "cafe_price_pop.csv")
    write_csv_bar_price(cafe_price_avg, res_price_avg, "price_by_city.csv")
    cali_price_bar_graph("price_by_city.csv")
    cali_price_dot_plot("cafe_price_pop.csv", "res_price_pop.csv")

if __name__ == "__main__":
    main()
