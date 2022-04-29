from bs4 import BeautifulSoup
import requests
import re


def get_movie_information():
    print("scrapper started to mine the data...")
    data = []
    url = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"
    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content, "lxml")
    gdp_table = soup.find("table", attrs={"class": "wikitable"})
    gdp_table_data = gdp_table.tbody.find_all("tr")  # contains 2 rows
    gdp_table_data.pop(0)
    data.append(['MovieName','ReleaseYear','Collection'])
    collection = ''
    movie_name = ''
    release_year = ''
    for table_body in gdp_table_data:
        for idx,td in enumerate(table_body.find_all("td")):
            if idx==2:
                if td.string is None:
                    collection = re.sub(r'^.*?[$]', '', td.text.strip())
                else:
                    collection = td.string.replace('$', '').strip()
            elif idx==3:
                release_year=td.string.strip()
        for idx,name in enumerate(table_body.find_all("i")):
            movie_name=name.string
        data.append([movie_name,release_year,collection])

    return data


