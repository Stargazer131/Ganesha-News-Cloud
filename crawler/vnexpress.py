from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
from datetime import datetime
from time import sleep


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

        if category in ['phap-luat']:
            return 'thoi-su'
        elif category in ['oto-xe-may']:
            return 'xe'
        elif category in ['khoa-hoc', 'so-hoa']:
            return 'khoa-hoc-cong-nghe'
        else:
            return category


    @staticmethod
    def crawl_article_links(category: str, max_page=20, limit=10 ** 9):
        """
        Returns
        ----------
        List of (link, thumbnail_link)
        """

        print(f'Crawl links for category: {category}/{VnexpressCrawler.web_name}')
        link_and_thumbnails = []
        page_num = 1

        # vnexpress has maximum 20 page
        max_page = min(max_page, 20)
        founded_links = 0
        while page_num <= max_page and founded_links < limit:
            print(f"\rCrawling links [{page_num} / {max_page}]", end='')
            
            sleep(0.1)
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

                    # no img tag mean no thumbnail -> skip
                    if img_tag is None:
                        continue

                    # thumbnail
                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-src', '').startswith('http'):
                        image_link = img_tag['data-src']

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

            if len(content_list) >= min_content_length:
                return {
                    'link': link,
                    'category': '',
                    'published_date': published_date,
                    'thumbnail': '',
                    'title': h1_title.get_text().strip(),
                    'description': description.strip(),
                    'content': content_list,
                    'web': VnexpressCrawler.web_name,
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

        article_links = VnexpressCrawler.crawl_article_links(category, limit=articles_limit)
        articles = []
        fail_list = []
        fail_attempt = 0
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):
            print(f"\rCrawling article [{index + 1} / {len(article_links)}], failed: {fail_attempt}", end='')

            sleep(delay_time)
            article = VnexpressCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = VnexpressCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(article)

        print(f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n')
        return articles, fail_list


if __name__ == '__main__':
    data = VnexpressCrawler.crawl_articles(VnexpressCrawler.categories[0], articles_limit=3)
    print(data[0][0])

