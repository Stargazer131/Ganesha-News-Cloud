import os
import re
from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
from datetime import datetime
from time import sleep
from server.data import connect_to_mongo


class VtcnewsCrawler:
    categories = [
        "thoi-su-28", "the-gioi-30", "kinh-te-29", "giai-tri-33",
        "the-thao-34", "giao-duc-31", "suc-khoe-35",
        "oto-xe-may-37", "khoa-hoc-cong-nghe-82"
    ]
    web_name = 'vtcnews'
    root_url = 'https://vtcnews.vn'

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

        category = category[:-3]
        if category in ['oto-xe-may']:
            return 'xe'
        elif category in ['kinh-te']:
            return 'kinh-doanh'
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
            cursor = collection.find({"web": VtcnewsCrawler.web_name}, {"link": 1, "_id": 0})
            if unique:
                return set(VtcnewsCrawler.extract_id(doc['link']) for doc in cursor)
            else:
                return [VtcnewsCrawler.extract_id(doc['link'])  for doc in cursor]

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
            cursor = collection.find({"web": VtcnewsCrawler.web_name}, {"link": 1, "_id": 0})
            if unique:
                return set(VtcnewsCrawler.extract_id(doc['link']) for doc in cursor)
            else:
                return [VtcnewsCrawler.extract_id(doc['link'])  for doc in cursor]
    
    @staticmethod
    def extract_id(link: str):
        """
        Extract the article id from the url.

        """

        link = link.removesuffix('.htm')
        link = link.removesuffix('.html')
        article_id = link.split('-')[-1]
        if not article_id.startswith('ar'):
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

        print(f'Crawl links for category: {category}/{VtcnewsCrawler.web_name}')
        article_link_ids = VtcnewsCrawler.get_all_links()
        article_black_list_ids = VtcnewsCrawler.get_all_black_links()

        link_and_thumbnails = []
        black_list = set()
        page_num = 1

        # vtc news has maximum 30 page
        max_page = min(max_page, 30)
        founded_links = 0
        while page_num <= max_page and founded_links < limit:
            sleep(0.1)
            found_new_link = False
            url = f'{VtcnewsCrawler.root_url}/{category}/trang-{page_num}.html'
            page_num += 1

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # find all the link
                article_tags = soup.find_all('article')

                for article_tag in article_tags:
                    a_tag = article_tag.find('a')
                    article_link = f'{VtcnewsCrawler.root_url}{a_tag["href"]}'
                    img_tag = article_tag.find('img')
                    article_id = VtcnewsCrawler.extract_id(article_link)

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
                    if article_id not in article_link_ids and article_link not in article_black_list_ids:
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
            article_tag = soup.find('section', class_='nd-detail')
            span_date = article_tag.find('span', class_='time-update')
            h1_title = article_tag.find('h1')
            description_tag = article_tag.find('h2')

            # clean description
            description = description_tag.get_text().strip().removeprefix('(VTC News)')
            description = description.removeprefix(' - ')

            # extract date info
            span_date_info = span_date.get_text().split(',')[1].strip()
            date_str, time_str, _ = span_date_info.split()
            published_date = datetime.strptime(date_str.strip() + ' ' + time_str.strip(), '%d/%m/%Y %H:%M:%S')

            div_content = article_tag.find('div', class_="edittor-content")
            for element in div_content:
                if not isinstance(element, Tag):
                    continue

                # text content
                if element.name == 'p' and 'expEdit' not in element.get('class', []) and len(element.get_text()) > 0:
                    content_list.append(element.get_text())

                # image content
                elif element.name == 'figure' and 'expNoEdit' in element.get('class', []):
                    # extract image link and caption
                    img_tag = element.find('img')

                    if img_tag is None:
                        continue

                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-src', '').startswith('http'):
                        image_link = img_tag['data-src']

                    fig_caption = element.find('figcaption')
                    caption = ''
                    if fig_caption is not None:
                        caption = fig_caption.get_text()

                    img_content = f'IMAGECONTENT:{image_link};;{caption}'
                    content_list.append(img_content)

                # image article
                elif element.name == 'div' and 'expNoEdit' in element.get('class', []):
                    for child in element:
                        if not isinstance(child, Tag):
                            continue
                        
                        # extract image link (caption may be?)
                        if child.name == 'figure':
                            img_tag = child.find('img')

                            image_link = None
                            if img_tag.get('src', '').startswith('http'):
                                image_link = img_tag['src']
                            elif img_tag.get('data-src', '').startswith('http'):
                                image_link = img_tag['data-src']

                            fig_caption = element.find('figcaption')
                            caption = ''
                            if fig_caption is not None:
                                caption = fig_caption.get_text()

                            img_content = f'IMAGECONTENT:{image_link};;{caption}'
                            content_list.append(img_content)

                        # extract image list
                        elif child.name == 'div' and child.find('p') is None:
                            image_list = []
                            for index, img_tag in enumerate(child.find_all('img')):
                                image_link = None
                                if img_tag.get('src', '').startswith('http'):
                                    image_link = img_tag['src']
                                elif img_tag.get('data-src', '').startswith('http'):
                                    image_link = img_tag['data-src']

                                img_content = f'IMAGECONTENT:{image_link};;1,{index + 1}'
                                image_list.append(img_content)

                            if len(image_list) > 0:
                                content_list.append(image_list)

                        # extract caption (find the direct child - p tag)
                        elif child.name == 'div' and child.find('p') is not None:
                            content_list.append(child.find('p').get_text())

                        # extract caption (maybe missing)
                        elif child.name == 'p':
                            content_list.append(child.get_text().strip())
                    
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
                    'web': VtcnewsCrawler.web_name,
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
        article_links, black_list = VtcnewsCrawler.crawl_article_links(category, limit=links_limit)
        fail_list = []
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):
            sleep(0.2)
            article = VtcnewsCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = VtcnewsCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(article)

                # add the link to black list except for Connection issue
                if not isinstance(article[1], requests.RequestException):
                    black_list.add(link)

        print(f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n')

        # log all the fail attempt
        error_log_dir = f'error_log/{VtcnewsCrawler.web_name}'
        error_file_path = f'{error_log_dir}/error-{category}.txt'
        os.makedirs(error_log_dir, exist_ok=True)
        
        with open(error_file_path, 'w') as file:
            file.writelines([f'Link: {item[0]} ;; Exception: {str(item[1])}\n' for item in fail_list])

        return articles, black_list


    @staticmethod
    def test_number_of_links():
        print('Black list')
        print(f'All: {len(VtcnewsCrawler.get_all_black_links(False))}')
        print(f'Unique: {len(VtcnewsCrawler.get_all_black_links())}\n')

        print('All link')
        print(f'All: {len(VtcnewsCrawler.get_all_links(False))}')
        print(f'Unique: {len(VtcnewsCrawler.get_all_links())}\n')


    @staticmethod
    def test_crawl_content(link):
        article = VtcnewsCrawler.crawl_article_content(link)
        print(*article['content'], sep='\n')


if __name__ == '__main__':
    VtcnewsCrawler.test_number_of_links()

