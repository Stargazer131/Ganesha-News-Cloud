from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
from datetime import datetime
from time import sleep


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
        Map real category name to an unified category name to match other crawlers.

        Parameters
        ----------
        category : str
            Real category name.

        Returns
        ----------
        str
            Mapped category name.
        """

        category = category[:-3]
        if category in ['oto-xe-may']:
            return 'xe'
        elif category in ['kinh-te']:
            return 'kinh-doanh'
        else:
            return category


    @staticmethod
    def crawl_article_links(category: str, max_page=30, limit=10 ** 9):
        """
        Returns
        ----------
        List of (link, thumbnail_link)
        """

        print(f'Crawl links for category: {category}/{VtcnewsCrawler.web_name}')
        link_and_thumbnails = []
        page_num = 1

        # vtc news has maximum 30 page
        max_page = min(max_page, 30)
        founded_links = 0
        while page_num <= max_page and founded_links < limit:
            print(f"\rCrawling links [{page_num} / {max_page}]", end='')

            sleep(0.1)
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

                    # no img tag mean no thumbnail -> skip
                    if img_tag is None:
                        continue

                    # thumbnail
                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-src', '').startswith('http'):
                        image_link = img_tag['data-src']

                    # check for duplicated and "black" link
                    link_and_thumbnails.append((article_link, image_link))
                    founded_links += 1

                    if founded_links >= limit:
                        print(f"\nFounded links passed the {limit} limit, terminate the searching!")
                        break

            except Exception as e:
                pass

        print(f"\nFind {len(link_and_thumbnails)} links")
        return link_and_thumbnails

    @staticmethod
    def crawl_article_content(link: str, min_content_length=4):
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
                    
            if len(content_list) >= min_content_length:
                return {
                    'link': link,
                    'category': '',
                    'published_date': published_date,
                    'thumbnail': '',
                    'title': h1_title.get_text().strip(),
                    'description': description.strip(),
                    'content': content_list,
                    'web': VtcnewsCrawler.web_name,
                }
            else:
                raise Exception('NO CONTENT')

        except Exception as e:
            pass

    @staticmethod
    def crawl_articles(category: str, articles_limit=10 ** 9, delay_time=0.15):
        """
        Crawl all articles for the given category

        Returns
        ----------
        tuple
            - List of articles.
            - List of failed links.
        """

        article_links = VtcnewsCrawler.crawl_article_links(category, limit=articles_limit)
        articles = []
        fail_list = []
        fail_attempt = 0
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):
            print(f"\rCrawling article [{index + 1} / {len(article_links)}], failed: {fail_attempt}", end='')

            sleep(delay_time)
            article = VtcnewsCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = VtcnewsCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(article)

        print(f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n')
        return articles, fail_list


if __name__ == '__main__':
    data = VtcnewsCrawler.crawl_articles(VtcnewsCrawler.categories[0], articles_limit=3)
    print(data[0][0])

