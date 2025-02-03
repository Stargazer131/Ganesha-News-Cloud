import os
import re
from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
from datetime import datetime
from time import sleep
from server.data import connect_to_mongo


class VnexpressCrawler:
    root_url = 'https://vnexpress.net'
    web_name = 'vnexpress'
    categories = [
        'phap-luat', 'thoi-su', 'the-gioi', 'kinh-doanh',
        'giai-tri', 'the-thao', 'giao-duc', 'suc-khoe',
        'du-lich', 'oto-xe-may', 'khoa-hoc', 'so-hoa'
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

        if category in ['phap-luat']:
            return 'thoi-su'
        elif category in ['oto-xe-may']:
            return 'xe'
        elif category in ['khoa-hoc', 'so-hoa']:
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
            cursor = collection.find({"web": VnexpressCrawler.web_name}, {"link": 1, "_id": 0})
            if unique:
                return set(VnexpressCrawler.extract_id(doc['link']) for doc in cursor)
            else:
                return [VnexpressCrawler.extract_id(doc['link'])  for doc in cursor]

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
            cursor = collection.find({"web": VnexpressCrawler.web_name}, {"link": 1, "_id": 0})
            if unique:
                return set(VnexpressCrawler.extract_id(doc['link']) for doc in cursor)
            else:
                return [VnexpressCrawler.extract_id(doc['link'])  for doc in cursor]

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
    def crawl_article_links(category: str, max_page=20, limit=10 ** 9):
        """
        Crawl all article link for a specific category.

        Returns
        ----------
        tuple
            A tuple containing:
            - List of (link, thumbnail_link)
            - Set of black links (links that can't be crawled)
        """

        print(f'Crawl links for category: {category}/{VnexpressCrawler.web_name}')
        article_link_ids = VnexpressCrawler.get_all_links()
        article_black_list_ids = VnexpressCrawler.get_all_black_links()

        link_and_thumbnails = []
        black_list = set()
        page_num = 1

        # vnexpress has maximum 20 page
        max_page = min(max_page, 20)
        founded_links = 0
        while page_num <= max_page and founded_links < limit:
            sleep(0.1)
            found_new_link = False
            url = f'{VnexpressCrawler.root_url}/{category}-p{page_num}/'
            page_num += 1

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # find all the link
                article_tags = soup.find_all('article')
                for article_tag in article_tags:
                    a_tag = article_tag.find('a')
                    article_link = a_tag['href']
                    img_tag = article_tag.find('img')
                    article_id = VnexpressCrawler.extract_id(article_link)

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
            h1_title = soup.find('h1', class_='title-detail')
            p_description = soup.find('p', class_='description')
            span_place = p_description.find('span', class_='location-stamp')
            span_date = soup.find('span', class_='date')

            # some article have different tag for date info
            if span_date is None:
                span_date = soup.find('div', class_='date-new')

            # remove Place Text
            description = p_description.get_text()
            if span_place is not None:
                description = description.removeprefix(span_place.get_text())

            # extract date info
            span_date_info = span_date.get_text().split(',')
            date_str = span_date_info[1].strip()
            time_str = span_date_info[2].strip()[:5]
            published_date = datetime.strptime(date_str + ' ' + time_str, '%d/%m/%Y %H:%M')

            # loop through all content, only keep p (text) and figure(img)
            article_content = soup.find('article', class_='fck_detail')
            for element in article_content:
                if not isinstance(element, Tag):
                    continue

                # skip video content
                if element.find('video') is not None:
                    continue

                # only select p tag with 1 attr -> article text content
                if element.name == 'p' and len(element.attrs) == 1 and element.get('class', [''])[0] == 'Normal':
                    content_list.append(element.get_text())

                # image content
                elif element.name == 'figure':
                    # extract image link and caption
                    img_tag = element.find('img')

                    # some figure tag empty (the figure tag at the end of article)
                    if img_tag is None:
                        continue

                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-src', '').startswith('http'):
                        image_link = img_tag['data-src']

                    p_caption = element.find('p', class_='Image')
                    caption = ''
                    if p_caption is not None:
                        caption = p_caption.get_text()

                    img_content = f'IMAGECONTENT:{image_link};;{caption}'
                    content_list.append(img_content)

                # for image article (different article structure)
                elif element.name == 'div' and 'item_slide_show' in element.get('class', []):
                    # extract image link
                    img_tag = element.find('img')
                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-src', '').startswith('http'):
                        image_link = img_tag['data-src']

                    img_content = f'IMAGECONTENT:{image_link};;'
                    content_list.append(img_content)

                    # extract text content for image
                    div_caption = element.find('div', class_='desc_cation')
                    for p_tag in div_caption.find_all('p', class_='Normal'):
                        content_list.append(p_tag.get_text())

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
                    'web': VnexpressCrawler.web_name,
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
        article_links, black_list = VnexpressCrawler.crawl_article_links(category, limit=links_limit)
        fail_list = []
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):
            sleep(0.2)
            article = VnexpressCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = VnexpressCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(article)

                # add the link to black list except for Connection issue
                if not isinstance(article[1], requests.RequestException):
                    black_list.add(link)

        print(f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n')

        # log all the fail attempt
        error_log_dir = f'error_log/{VnexpressCrawler.web_name}'
        error_file_path = f'{error_log_dir}/error-{category}.txt'
        os.makedirs(error_log_dir, exist_ok=True)
        
        with open(error_file_path, 'w') as file:
            file.writelines([f'Link: {item[0]} ;; Exception: {str(item[1])}\n' for item in fail_list])

        return articles, black_list


    @staticmethod
    def test_number_of_links():
        print('Black list')
        print(f'All: {len(VnexpressCrawler.get_all_black_links(False))}')
        print(f'Unique: {len(VnexpressCrawler.get_all_black_links())}\n')

        print('All link')
        print(f'All: {len(VnexpressCrawler.get_all_links(False))}')
        print(f'Unique: {len(VnexpressCrawler.get_all_links())}\n')


    @staticmethod
    def test_crawl_content(link=''):
        article = VnexpressCrawler.crawl_article_content(link)
        print(*article['content'], sep='\n')


if __name__ == '__main__':
    VnexpressCrawler().test_number_of_links()

