from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
from datetime import datetime
from time import sleep


class VietnamnetCrawler:
    categories = [
        "thoi-su", "the-gioi", "kinh-doanh", "giai-tri", 
        "the-thao", "giao-duc", "suc-khoe", "du-lich", 
        "oto-xe-may", "thong-tin-truyen-thong"
    ]
    web_name = 'vietnamnet'
    root_url = 'https://vietnamnet.vn'


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

        if category in ['oto-xe-may', 'o-to-xe-may']:
            return 'xe'
        elif category in ['thong-tin-truyen-thong']:
            return 'khoa-hoc-cong-nghe'
        else:
            return category


    @staticmethod
    def crawl_article_links(category: str, max_page=35, limit=10 ** 9):
        """
        Returns
        ----------
        List of (link, thumbnail_link)
        """
        
        print(f'Crawl links for category: {category}/{VietnamnetCrawler.web_name}')
        link_and_thumbnails = []
        page_num = 1

        # vietnamnet has unlimited pages
        max_page = min(max_page, 25)
        founded_links = 0
        while page_num <= max_page and founded_links < limit:
            print(f"\rCrawling links [{page_num} / {max_page}]", end='')
            
            sleep(0.1)
            url = f'{VietnamnetCrawler.root_url}/{category}-page{page_num - 1}'
            page_num += 1

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # find all the link
                article_tags = soup.find_all('div', class_=['horizontalPost', 'verticalPost'])

                for article_tag in article_tags:
                    a_tag = article_tag.find('a')
                    if a_tag["href"].startswith('http'):
                        article_link = a_tag["href"]
                    else:
                        article_link = f'{VietnamnetCrawler.root_url}{a_tag["href"]}'
                    
                    img_tag = article_tag.find('img')
                    # no img tag mean no thumbnail -> skip
                    if img_tag is None:
                        continue

                    # thumbnail
                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-srcset', '').startswith('http'):
                        image_link = img_tag['data-srcset']

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
            span_date = soup.find('div', class_='bread-crumb-detail__time')
            article_tag = soup.find('div', class_='content-detail')
            h1_title = article_tag.find(class_='content-detail-title')
            description_tag = article_tag.find(class_="content-detail-sapo")

            # extract date info
            span_date_info = span_date.get_text().split(',')[1].strip()
            date_str, time_str = span_date_info.split('-')
            published_date = datetime.strptime(date_str.strip() + ' ' + time_str.strip(), '%d/%m/%Y %H:%M')

            div_content = article_tag.find('div', class_='maincontent')
            for element in div_content:
                if not isinstance(element, Tag):
                    continue
                
                # text content
                if element.name == 'p' and element.find('iframe') is None and len(element.get_text()) > 0:
                    content_list.append(element.get_text())

                # image content
                elif element.name == 'figure' and 'image' in element.get('class', []):
                    # extract image link and caption
                    img_tag = element.find('img')
                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-srcset', '').startswith('http'):
                        image_link = img_tag['data-srcset']

                    fig_caption = element.find('figcaption')
                    caption = ''
                    if fig_caption is not None:
                        caption = fig_caption.get_text()

                    img_content = f'IMAGECONTENT:{image_link};;{caption}'
                    content_list.append(img_content)

                # for image list
                elif element.name == 'figure' and 'vnn-figure-image-gallery' in element.get('class', []):
                    image_list = []
                    for row_index, row in enumerate(element.find_all('tr')):
                        for col_index, img_tag in enumerate(row.find_all('img')):
                            image_link = None
                            if img_tag.get('src', '').startswith('http'):
                                image_link = img_tag['src']
                            elif img_tag.get('data-srcset', '').startswith('http'):
                                image_link = img_tag['data-srcset']

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
                    'description': description_tag.get_text().strip(),
                    'content': content_list,
                    'web': VietnamnetCrawler.web_name,
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

        article_links = VietnamnetCrawler.crawl_article_links(category, limit=articles_limit)
        articles = []
        fail_list = []
        fail_attempt = 0
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):
            print(f"\rCrawling article [{index + 1} / {len(article_links)}], failed: {fail_attempt}", end='')

            sleep(delay_time)
            article = VietnamnetCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = VietnamnetCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(article)

        print(f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n')
        return articles, fail_list
    

if __name__ == '__main__':
    data = VietnamnetCrawler.crawl_articles(VietnamnetCrawler.categories[0], articles_limit=3)
    print(data[0][0])

