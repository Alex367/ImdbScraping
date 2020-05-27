from bs4 import BeautifulSoup
import requests
import time
import csv
import multiprocessing
import collections


class Pages:
    def __init__(self):
        self.all_data_main = []
        self.all_data_inner = []
        self.all_data = []
        self.cnt_film_checked = 0
        self.multi_data_main = []
        self.multi_data_inner = []

    @staticmethod
    def find_links():
        # Find all links
        url = "https://www.imdb.com/search/title/?groups=top_250&sort=user_rating"
        main_links = []
        inner_links = []
        all_links = []

        # Find main links
        main_links.append(url)
        while 1:
            # Request to web page
            html = requests.get(url)
            # Create BS object
            soup = BeautifulSoup(html.text, "lxml")
            # Find link to the next page
            next_page_tag = soup.find('a', class_='next-page')
            if next_page_tag is None:
                break
            # Full link to the next page
            url = 'https://www.imdb.com/' + next_page_tag['href']
            main_links.append(url)

        # Find inner links
        for i in main_links:
            html = requests.get(i)
            soup = BeautifulSoup(html.text, "lxml")
            # Neighbor of inner link
            neighbor_inner = soup.find_all("span", class_="text-primary")  # limit=2
            for j in neighbor_inner:
                inner_links.append('https://www.imdb.com/' + j.next_sibling.next_sibling['href'])

        # all links to the one list
        all_links.append(main_links)
        all_links.append(inner_links)
        # print(self.all_links)
        return all_links

    def save_csv_data(self):
        # Save data to csv file
        try:
            file = open('new_exc.csv', 'a+', newline='', encoding='utf-8')
        except OSError:
            print("Could not open/read file 'new_exc.csv'")
            raise

        with file:
            write = csv.writer(file)
            write.writerows(self.all_data)

    def get_rating(self, soup):
        # Rating
        film_rating = soup.select('.global-sprite.rating-star.imdb-rating')  # limit=2
        j = self.cnt_film_checked
        for i in film_rating:
            self.all_data_main[j].append(i.next_sibling.next_sibling.next_element.replace('.', ','))
            j += 1

    def get_genre(self, soup):
        # Genre
        film_genre = soup.select('.lister-item-content p.text-muted span.genre')  # limit=2
        j = self.cnt_film_checked
        for i in film_genre:
            self.all_data_main[j].append(i.next_element.replace('\n', '').replace(' ', ''))
            j += 1

    def get_director(self, soup):
        # Director
        film_director = soup.select(".lister-item-content")  # limit=2
        j = self.cnt_film_checked
        for i in film_director:
            self.all_data_main[j].append(i.find('p', class_='').find('a').next_element.replace(' ', ''))
            j += 1

    def get_pos_title_year(self, soup):
        # Position, title, year
        position_film = soup.find_all('span', class_='text-primary')  # limit=2
        for i in position_film:
            # Find
            position = i.next_element.replace('.', '')
            title = i.next_sibling.next_sibling.next_element.replace('\t', '')
            year = i.next_sibling.next_sibling.next_sibling.next_sibling.next_element
            year_clean = year.replace('(', '').replace(')', '').replace('I', '').replace(' ', '')
            # Add to list
            position_list_inner = [position, title, year_clean]
            self.all_data_main.append(position_list_inner)

    @staticmethod
    def get_country_loc_budget(soup):
        # Country
        film_country_sibling = soup.find("h4", string="Country:")
        if film_country_sibling is not None:
            film_country = film_country_sibling.next_sibling.next_sibling.next_element.replace('.', ' ')
        else:
            film_country = 'Nan'

        # Filming Locations
        film_location_sibling = soup.find("h4", string="Filming Locations:")
        if film_location_sibling is not None:
            film_location = film_location_sibling.next_sibling.next_sibling.next_element.replace('.', ' ')
        else:
            film_location = 'Nan'

        # Budget
        film_budget_sibling = soup.find("h4", string="Budget:")
        if film_budget_sibling is not None:
            film_budget = film_budget_sibling.next_sibling
            film_budget_clean = film_budget.replace('\n', '').replace(' ', '').replace('.', ' ')
        else:
            film_budget_clean = 'Nan'

        return film_country, film_location, film_budget_clean

    def main_scraping(self, main_link):
        # Request by link
        html = requests.get(main_link)
        soup = BeautifulSoup(html.text, "lxml")

        # Position, title, year
        self.get_pos_title_year(soup)
        # Rating
        self.get_rating(soup)
        # Genre
        self.get_genre(soup)
        # Director
        self.get_director(soup)

        # Check how much pages were scraped
        self.cnt_film_checked += 50  # 2 Turn!

    def inner_scraping(self, inner_link):
        # Request by link
        html = requests.get(inner_link)
        soup = BeautifulSoup(html.text, "lxml")

        film_country, film_location, film_budget_clean = self.get_country_loc_budget(soup)

        inside_list = [film_country, film_location, film_budget_clean]
        self.all_data_inner.append(inside_list)

    def start_one(self, all_links_one):
        # all_links_one = [ [main], [inner] ]
        # Time
        start_time = time.time()

        # Scraping on the main page
        print('Start scraping main pages')
        for i in range(len(all_links_one[0])):
            # main link -> scraping
            print("Scraping of main page: ", i + 1)
            self.main_scraping(all_links_one[0][i])

        # Scraping on the inner page
        print('Start scraping inner pages')
        for i in range(len(all_links_one[1])):
            # inner link -> scraping
            if i % 10 == 0:
                print("Scraping of inner page: ", i)
            self.inner_scraping(all_links_one[1][i])

        # Merge main and inner to one
        for i in range(len(self.all_data_main)):
            self.all_data.append(self.all_data_main[i] + self.all_data_inner[i])

        self.save_csv_data()

        print("\nFinale time for 1 case: ", time.time() - start_time)

    def multi_main_scraping(self, m_links, index, return_dict):
        # Request by link
        html = requests.get(m_links)
        soup = BeautifulSoup(html.text, "lxml")

        # Position, title, year
        self.get_pos_title_year(soup)
        # Rating
        self.get_rating(soup)
        # Genre
        self.get_genre(soup)
        # Director
        self.get_director(soup)

        # Check how much pages were scraped
        self.cnt_film_checked += 50  # 2 Turn!
        # Save data
        return_dict[index] = self.all_data_main

    def multi_inner_scraping(self, i_links, index, return_dict):
        if index % 10 == 0:
            print('Number of inner pages: ', index)
        # Request by link
        html = requests.get(i_links)
        soup = BeautifulSoup(html.text, "lxml")

        film_country, film_location, film_budget_clean = self.get_country_loc_budget(soup)

        inside_list = [film_country, film_location, film_budget_clean]
        self.all_data_inner.append(inside_list)
        # Save data
        return_dict[index] = self.all_data_inner

    def start_multi(self, multi_links):
        # Time
        start_time = time.time()

        # Main links, all main links = [,]
        print('Start main part multi')
        all_main_links = multi_links[0]

        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        jobs = []
        for index, lnk in enumerate(all_main_links):
            p = multiprocessing.Process(target=multi.multi_main_scraping, args=(lnk, index, return_dict))
            jobs.append(p)
            p.start()

        for proc in jobs:
            # Block the calling thread until the process
            proc.join()

        # Order dictionary
        od = collections.OrderedDict(sorted(return_dict.items()))

        for i in od:
            # Save all info to list
            self.multi_data_main.extend(od[i])

        # Inner links
        print('Start inner part multi')
        all_inner_links = multi_links[1]

        return_dict_inner = manager.dict()
        jobs_inner = []

        for index, lnk in enumerate(all_inner_links):
            p = multiprocessing.Process(target=multi.multi_inner_scraping, args=(lnk, index, return_dict_inner))
            jobs_inner.append(p)
            p.start()

        for proc in jobs_inner:
            # Block the calling thread until the process
            proc.join()

        od = collections.OrderedDict(sorted(return_dict_inner.items()))

        for i in od:
            # Save data to list
            self.multi_data_inner.extend(od[i])

        # Merge main and inner list to one
        for i in range(len(self.multi_data_main)):
            self.all_data.append(self.multi_data_main[i] + self.multi_data_inner[i])

        self.save_csv_data()

        print("\nFinale time for 2 case: ", time.time() - start_time)


if __name__ == '__main__':
    # One process
    # one = Pages()
    # links_one = one.find_links()
    # one.start_one(links_one)
    # Multiprocessing
    multi = Pages()
    links = multi.find_links()
    multi.start_multi(links)
