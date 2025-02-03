import os
import re
from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
from datetime import datetime
from time import sleep
from server.data import connect_to_mongo


class DantriCrawler:
    root_url = 'https://dantri.com.vn'
    web_name = 'dantri'
    categories = [
        "xa-hoi", "phap-luat", "the-gioi", "kinh-doanh",
        "giai-tri", "the-thao", "giao-duc", "suc-khoe",
        "du-lich", "o-to-xe-may", "khoa-hoc-cong-nghe", "suc-manh-so"
    ]

    @staticmethod
    def get_category_name(category: str):
        """
        Map real category name to database category name.

        Parameters
        ----------
        category : str
            Real category name.

        Returns
        ----------
        str
            Database category name.
        """

        if category in ['xa-hoi', 'phap-luat']:
            return 'thoi-su'
        elif category in ['o-to-xe-may']:
            return 'xe'
        elif category in ['suc-manh-so']:
            return 'khoa-hoc-cong-nghe'
        else:
            return category

    @staticmethod
    def get_all_links(unique=True):
        """
        Get all article ids from database (extracted from link).

        Use extracted id instead of link to prevent some redirected links (same article but different link).

        Returns
        ----------
        set
            Set of id.
        """

        with connect_to_mongo() as client:
            db = client['Ganesha_News']
            collection = db['newspaper']
            cursor = collection.find({"web": DantriCrawler.web_name}, {"link": 1, "_id": 0})
            if unique:
                return set(DantriCrawler.extract_id(doc['link']) for doc in cursor)
            else:
                return [DantriCrawler.extract_id(doc['link'])  for doc in cursor]

    @staticmethod
    def get_all_black_links(unique=True):
        """
        Get all article ids from the black list in database (extracted from link).

        Use extracted id instead of link to prevent some redirected links (same article but different link).

        Returns
        ----------
        set
            Set of id.
        """

        with connect_to_mongo() as client:
            db = client['Ganesha_News']
            collection = db['black_list']
            cursor = collection.find({"web": DantriCrawler.web_name}, {"link": 1, "_id": 0})
            if unique:
                return set(DantriCrawler.extract_id(doc['link']) for doc in cursor)
            else:
                return [DantriCrawler.extract_id(doc['link'])  for doc in cursor]
            
    @staticmethod
    def extract_id(link: str):
        """
        Extract the article id from the url.

        """

        link = link.removesuffix('.htm')
        link = link.removesuffix('.html')
        article_id = link.split('-')[-1]
        if not article_id.isnumeric():
            matches = re.findall(r"\d+", link)
            return matches[-1]
        else:
            return article_id

    @staticmethod
    def crawl_article_links(category: str, max_page=30, limit=10 ** 9):
        """
        Crawl all article link for a specific category.

        Returns
        ----------
        tuple
            A tuple containing:
            - List of (link, thumbnail_link)
            - Set of black links (links that can't be crawled)
        """

        print(f'Crawl links for category: {category}/{DantriCrawler.web_name}')
        article_link_ids = DantriCrawler.get_all_links()
        article_black_list_ids = DantriCrawler.get_all_black_links()

        link_and_thumbnails = []
        black_list = set()
        page_num = 1

        # dantri has maximum 30 page
        max_page = min(max_page, 30)
        founded_links = 0
        while page_num <= max_page and founded_links < limit:
            sleep(0.1)
            found_new_link = False
            url = f'{DantriCrawler.root_url}/{category}/trang-{page_num}.htm'
            page_num += 1

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # find all the link
                article_tags = soup.find_all('article', class_='article-item')
                for article_tag in article_tags:
                    a_tag = article_tag.find('a')
                    article_link = a_tag["href"]
                    if not article_link.startswith(DantriCrawler.root_url):
                        article_link = DantriCrawler.root_url + a_tag["href"]
                    img_tag = article_tag.find('img')
                    article_id = DantriCrawler.extract_id(article_link)

                    # if the category is wrong -> skip
                    if category not in article_link:
                        continue

                    # no img tag mean no thumbnail -> skip
                    if img_tag is None:
                        if article_id not in article_black_list_ids:
                            black_list.add(article_link)
                        continue

                    # thumbnail
                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-src', '').startswith('http'):
                        image_link = img_tag['data-src']

                    # check for duplicated and "black" link
                    if article_id not in article_link_ids and article_id not in article_black_list_ids:
                        found_new_link = True
                        founded_links += 1
                        article_link_ids.add(article_id)
                        link_and_thumbnails.append((article_link, image_link))

                    if founded_links >= limit:
                        print(f"\nFounded links passed the {limit} limit, terminate the searching!")
                        break
                        
                if not found_new_link:
                    print(f"\nNo new link found, terminate the searching!")
                    break

            except Exception as e:
                pass

        print(f"\nFind {len(link_and_thumbnails)} links")
        return link_and_thumbnails, black_list

    @staticmethod
    def crawl_article_content(link: str):
        """
        Crawl article content.

        Returns
        ----------
        Article
            The crawled article content.
        Or
        Tuple[Link, Exception]
            The link and exception if an error occurs.
        """

        try:
            response = requests.get(link)
            soup = BeautifulSoup(response.content, 'html.parser')

            content_list = []
            article_tag = soup.find('article')
            h1_title = article_tag.find('h1')

            # DMAGAZINE has no h1 -> can't crawl title -> skip
            if len(h1_title.get_text().strip()) == 0:
                raise Exception("NO TITLE")

            # extract date info
            time = article_tag.find('time')
            published_date = datetime.strptime(time['datetime'], '%Y-%m-%d %H:%M')

            # normal
            if 'singular-container' in article_tag.get('class', []):
                description_tag = article_tag.find(class_="singular-sapo")
                div_content = article_tag.find('div', class_='singular-content')

                # clean the description
                description = description_tag.get_text().strip().removeprefix('(Dân trí)')
                description = description.removeprefix(' - ')

                # loop through all content, only keep p (text) and figure(img)
                for element in div_content:
                    if not isinstance(element, Tag):
                        continue

                    # only keep text content (remove author text)
                    if element.name == 'p' and 'text-align:right' not in element.get('style', []):
                        content_list.append(element.get_text().strip())

                    elif element.name == 'figure' and 'image' in element.get('class', []):
                        # extract image link and caption
                        img_tag = element.find('img')

                        image_link = None
                        if img_tag.get('src', '').startswith('http'):
                            image_link = img_tag['src']
                        elif img_tag.get('data-src', '').startswith('http'):
                            image_link = img_tag['data-src']

                        fig_caption = element.find('figcaption')
                        caption = ''
                        if fig_caption is not None:
                            caption = fig_caption.get_text().strip()

                        img_content = f'IMAGECONTENT:{image_link};;{caption}'
                        content_list.append(img_content)

            # dnews and photo-story
            elif 'e-magazine' in article_tag.get('class', []):
                description_tag = article_tag.find(class_="e-magazine__sapo")
                div_content = article_tag.find('div', class_='e-magazine__body')

                # clean the description
                description = description_tag.get_text().strip().removeprefix('(Dân trí)')
                description = description.removeprefix(' - ')

                # loop through all content, only keep text and image
                for element in div_content:
                    if not isinstance(element, Tag):
                        continue

                    # only keep text content (remove author text)
                    if element.name in ['p', 'h1', 'h2', 'h3', 'h4'] and 'text-align:right' not in element.get('style', []):
                        content_list.append(element.get_text().strip())

                    elif element.name == 'figure' and 'image' in element.get('class', []):
                        # extract image link and caption
                        img_tag = element.find('img')

                        image_link = None
                        if img_tag.get('src', '').startswith('http'):
                            image_link = img_tag['src']
                        elif img_tag.get('data-src', '').startswith('http'):
                            image_link = img_tag['data-src']

                        fig_caption = element.find('figcaption')
                        caption = ''
                        if fig_caption is not None:
                            caption = fig_caption.get_text().strip()

                        img_content = f'IMAGECONTENT:{image_link};;{caption}'
                        content_list.append(img_content)

                    # photo grid
                    elif element.name == 'div' and 'photo-grid' in element.get('class', []):
                        image_list = []
                        for row_index, row in enumerate(element.find_all('div', class_="photo-row")):
                            for col_index, img_tag in enumerate(row.find_all('img')):
                                image_link = None
                                if img_tag.get('src', '').startswith('http'):
                                    image_link = img_tag['src']
                                elif img_tag.get('data-src', '').startswith('http'):
                                    image_link = img_tag['data-src']

                                img_content = f'IMAGECONTENT:{image_link};;{row_index + 1},{col_index + 1}'
                                image_list.append(img_content)

                        if len(image_list) > 0:
                            content_list.append(image_list)

            # content list <= 3 -> crawling process is broken, q/a article ...
            if len(content_list) > 3:
                return {
                    'link': link,
                    'category': '',
                    'published_date': published_date,
                    'thumbnail': '',
                    'title': h1_title.get_text().strip(),
                    'description': description.strip(),
                    'content': content_list,
                    'web': DantriCrawler.web_name,
                    'index': -1
                }
            else:
                raise Exception('NO CONTENT')

        except Exception as e:
            return (link, e)

    @staticmethod
    def crawl_articles(category: str, links_limit=10 ** 9):
        """
        Crawl all articles for the given category and log all errors.

        Returns
        ----------
        tuple
            - list: List of articles.
            - set: Set of blacklisted links (links that couldn't be crawled).
        """

        fail_attempt = 0
        articles = []
        article_links, black_list = DantriCrawler.crawl_article_links(category, limit=links_limit)
        fail_list = []
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):            
            sleep(0.25)
            article = DantriCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = DantriCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(article)

                # add the link to black list except for Connection issue
                if not isinstance(article[1], requests.RequestException):
                    black_list.add(link)

        print(f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n')

        # log all the fail attempt
        error_log_dir = f'error_log/{DantriCrawler.web_name}'
        error_file_path = f'{error_log_dir}/error-{category}.txt'
        os.makedirs(error_log_dir, exist_ok=True)
        
        with open(error_file_path, 'w') as file:
            file.writelines([f'Link: {item[0]} ;; Exception: {str(item[1])}\n' for item in fail_list])

        return articles, black_list


    @staticmethod
    def test_number_of_links():
        print('Black list')
        print(f'All: {len(DantriCrawler.get_all_black_links(False))}')
        print(f'Unique: {len(DantriCrawler.get_all_black_links())}\n')

        print('All link')
        print(f'All: {len(DantriCrawler.get_all_links(False))}')
        print(f'Unique: {len(DantriCrawler.get_all_links())}\n')


    @staticmethod
    def test_crawl_content(link=''):
        article = DantriCrawler.crawl_article_content(link)
        print(*article['content'], sep='\n')


if __name__ == '__main__':
    DantriCrawler.test_number_of_links()

