import requests
from bs4 import BeautifulSoup as bs
import re
import time
from tqdm import trange
import json
import sqlite3


class Krisha:

    CITIES_URL = {
        0: "",
        1: "almaty/",
        2: "astana/",
        3: "shymkent/",
        4: "abay-oblast/",
        5: "akmolinskaja-oblast/",
        6: "aktjubinskaja-oblast/",
        7: "almatinskaja-oblast/",
        8: "atyrauskaja-oblast/",
        9: "vostochno-kazahstanskaja-oblast/",
        10: "zhambylskaja-oblast/",
        11: "jetisyskaya-oblast/",
        12: "zapadno-kazahstanskaja-oblast/",
        13: "karagandinskaja-oblast/",
        14: "kostanajskaja-oblast/",
        15: "kyzylordinskaja-oblast/",
        16: "mangistauskaja-oblast/",
        17: "pavlodarskaja-oblast/",
        18: "severo-kazahstanskaja-oblast/",
        19: "juzhno-kazahstanskaja-oblast/",
        20: "ulitayskay-oblast/",
    }

    USER_AGENT = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/90.0.4430.212 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(self, city=1, has_photo=True, rooms=None, price_from=1, price_to=1000, owner=True):

        self.SALE_URL = "https://krisha.kz/prodazha/kvartiry/" + Krisha.CITIES_URL.get(city)
        self.has_photo = has_photo
        self.rooms = rooms or [1, 2, 3, 4, 5]
        self.price_from = price_from
        self.price_to = price_to
        self.owner = owner
        self.get_url = self.get_url()
        self.run_crawler()

    def get_url(self):
        params = []
        if self.has_photo:
            params.append("[_sys.hasphoto]=1")
        if len(self.rooms) == 1:
            params.append("=".join(["[live.rooms]", str(*self.rooms)]))
        if len(self.rooms) > 1:
            rooms = sorted(self.rooms)
            room_data = ["[]=".join(["[live.rooms]", str(i)]) for i in rooms]
            params.append(re.sub(r"\b5\b", "5.100", "&das".join(room_data)))
        if self.price_from:
            params.append(f"[price][from]={int(self.price_from * 1000000)}")
        if self.price_to:
            params.append(f"[price][to]={int(self.price_to * 1000000)}")
        if self.owner:
            params.append("[who]=1")
        search_str = "&das".join(i for i in params if i is not None)
        if search_str:
            return self.SALE_URL + "?das" + search_str
        return self.SALE_URL

    def run_crawler(self):
        response = self.get_response(self.get_url)
        content = bs(response.text, "html.parser")
        subtitle = content.find("div", class_="a-search-subtitle")
        ads_count = int("".join(re.findall(r"\d+", subtitle.text.strip())))
        page_count = 1
        if ads_count > 20:
            paginator = content.find("nav", class_="paginator")
            page_count = int(paginator.text.split()[-2])

        for num in trange(1, page_count + 1):
        #for num in trange(1, 3):
            ads_section = content.find("section", class_="a-search-list")
            ads_on_page = ads_section.find_all("div", attrs={"data-id": True})

            ads_urls = []
            for ad in ads_on_page:
                title = ad.find("a", class_="a-card__title")
                ad_url = title.get("href")
                ads_urls.append("https://krisha.kz" + ad_url)

            flats_data = self.get_flats_data_on_page(ads_urls)
            validated_data = [i for i in flats_data if i is not None]
            insert_flats_data_db(validated_data)
            time.sleep(2)

            if num < page_count:
                next_btn = content.find("a", class_="paginator__btn--next")
                next_btn_url = next_btn.get("href")
                next_url = "https://krisha.kz" + next_btn_url
                response = self.get_response(next_url)
                content = bs(response.text, "html.parser")

    @staticmethod
    def get_response(url):
        for delay in (15, 60, 300, 1200, 3600):
            try:
                response = requests.get(url, headers=Krisha.USER_AGENT, timeout=20)
                if response.status_code == requests.codes.ok:
                    return response
            except requests.RequestException as error:
                print(error)

            time.sleep(delay)

    def get_flats_data_on_page(self, ads_urls):

        flats_data = []
        for url in ads_urls:
            try:
                response = self.get_response(url)
            except Exception as e:
                print(e)
            else:
                content = bs(response.text, "html.parser")
                flats_data.append(self.flat(content, url))
            time.sleep(2)
        return flats_data

    @staticmethod
    def flat(content, url):
        # content = bs(content.text, "html.parser")
        script = content.find("script", id="jsdata")
        string = script.text.strip()
        start_index = string.find("{")
        end_index = string.rfind("}")
        json_string = string[start_index:end_index + 1]
        data = json.loads(json_string)
        advert = data.get("advert")
        adverts = data.get("adverts")
        address = adverts[0].get("fullAddress")
        lat_lon = advert.get("map")
        photos = advert.get("photos")

        # print(id_, uuid, url, room, square, city, lat, lon, description, photo, price)
        data_dict = {
            'id': advert.get('id'),
            'uuid': adverts[0].get('uuid'),
            'url': url,
            'room': advert.get('rooms'),
            'square': advert.get('square'),
            'city': address.split(",")[0] if address else None,
            'lat': lat_lon.get("lat") if lat_lon else None,
            'lon': lat_lon.get("lon") if lat_lon else None,
            'description': adverts[0].get("description"),
            'photo': photos[0].get("src") if photos else None,
            'price': advert.get("price")
        }
        return data_dict


def check_db_exists(con):
    """Check DB."""
    # con = sqlite3.connect('db.db')
    cursor = con.cursor()
    query = """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='flats';
            """
    with con:
        cursor.execute(query)
        if cursor.fetchone():
            return

    """Create DB table."""
    query = """
            CREATE TABLE IF NOT EXISTS flats
            (
                id          INTEGER NOT NULL PRIMARY KEY,
                id_flat     INTEGER,
                uuid        TEXT    NOT NULL UNIQUE,
                url         TEXT    NOT NULL,
                room        INTEGER,
                square      INTEGER,
                city        TEXT,
                lat         REAL,
                lon         REAL,
                description TEXT,
                photo       TEXT,
                date     DATE DEFAULT (DATE('now', 'localtime')),
                price    INTEGER             NOT NULL
            );
            """

    with con:
        con.executescript(query)


def insert_flats_data_db(flats_data):
    """Insert flats data to DB."""
    con = sqlite3.connect('db.db')
    insert_flats_query = """
        INSERT OR IGNORE
        INTO flats(id_flat,
                   uuid,
                   url,
                   room,
                   square,
                   city,
                   lat,
                   lon,
                   description,
                   photo,
                   price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

    flats_value = [
        (
            flat.get('id'),
            flat.get('uuid'),
            flat.get('url'),
            flat.get('room'),
            flat.get('square'),
            flat.get('city'),
            flat.get('lat'),
            flat.get('lon'),
            flat.get('description'),
            flat.get('photo'),
            flat.get('price')
        )
        for flat in flats_data
    ]

    with con:
        con.executemany(insert_flats_query, flats_value)


def krisha():
    con = sqlite3.connect('db.db')
    check_db_exists(con)
    Krisha()


if __name__ == '__main__':
    krisha()
    # kz = Krisha(rooms=[1])
    # print(kz.get_url)

