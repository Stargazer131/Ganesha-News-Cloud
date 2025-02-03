from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
from datetime import datetime
from time import sleep


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

        if category in ['xa-hoi', 'phap-luat']:
            return 'thoi-su'
        elif category in ['o-to-xe-may']:
            return 'xe'
        elif category in ['suc-manh-so']:
            return 'khoa-hoc-cong-nghe'
        else:
            return category


    @staticmethod
    def crawl_article_links(category: str, max_page=30, limit=10 ** 9):
        """
        Returns
        ----------
        List of (link, thumbnail_link)
        """

        print(f'Crawl links for category: {category}/{DantriCrawler.web_name}')
        link_and_thumbnails = []
        page_num = 1

        # dantri has maximum 30 page
        max_page = min(max_page, 30)
        founded_links = 0
        while page_num <= max_page and founded_links < limit:
            print(f"\rCrawling links [{page_num} / {max_page}]", end='')

            sleep(0.1)
            url = f'{DantriCrawler.root_url}/{category}/trang-{page_num}.htm'
            page_num += 1

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # find all the link
                article_tags = soup.find_all('article', class_='article-item')
                for article_tag in article_tags:
                    a_tag = article_tag.find('a')
                    article_link = f'{DantriCrawler.root_url}{a_tag["href"]}'
                    img_tag = article_tag.find('img')

                    # if the category is wrong -> skip
                    if category not in article_link:
                        continue

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

            if len(content_list) >= min_content_length:
                return {
                    'link': link,
                    'category': '',
                    'published_date': published_date,
                    'thumbnail': '',
                    'title': h1_title.get_text().strip(),
                    'description': description.strip(),
                    'content': content_list,
                    'web': DantriCrawler.web_name,
                }
            else:
                raise Exception('NO CONTENT')

        except Exception as e:
            pass

    @staticmethod
    def crawl_articles(category: str, articles_limit=10 ** 9, delay_time=0.25):
        """
        Crawl all articles for the given category

        Returns
        ----------
        tuple
            - List of articles.
            - List of failed links.
        """

        article_links = DantriCrawler.crawl_article_links(category, limit=articles_limit)
        articles = []
        fail_list = []
        fail_attempt = 0
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):
            print(f"\rCrawling article [{index + 1} / {len(article_links)}], failed: {fail_attempt}", end='')
            
            sleep(delay_time)
            article = DantriCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = DantriCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(link)

        print(f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n')
        return articles, fail_list


if __name__ == '__main__':
    data = DantriCrawler.crawl_articles(DantriCrawler.categories[0], articles_limit=3)
    print(data[0][0])

