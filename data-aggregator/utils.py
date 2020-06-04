import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup


class NewsAggregator:
    def read_nytimes(self, category, days):
        """
        reading news from New York Times
        :param category:
        :param days:
        :return:
        """
        if category == "facebook":
            query_string = "https://api.nytimes.com/svc/mostpopular/v2/shared/" + days + "/facebook.json?api-key=0qlfIiB4jEyyrEkuZhqFe6jy5Ai7BAr6"
        else:
            query_string = "https://api.nytimes.com/svc/mostpopular/v2/" + category + "/" + days + ".json?api-key=0qlfIiB4jEyyrEkuZhqFe6jy5Ai7BAr6"
        r = requests.get(query_string)
        data = r.json()
        for item in data["results"]:
            page = urlopen(item["url"])
            soup = BeautifulSoup(page)
            page_filling = ''.join(['%s' % x for x in soup.body.contents])
            print(page_filling)
