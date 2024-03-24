import scrapy
from bookscraper.items import BookItem


class BookspiderSpider(scrapy.Spider):
    name = "bookspider"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com"]

    def parse(self, response):
        # Get all the book elements on the page
        books = response.css("article.product_pod")

        # For every book, extract the name, price and the url
        for book in books:
            relative_book_url = book.css("h3 a").attrib["href"]

            if "catalogue" in relative_book_url:
                book_url = "https://books.toscrape.com/" + relative_book_url
            else:
                book_url = "https://books.toscrape.com/catalogue/" + relative_book_url
            yield response.follow(book_url, callback=self.parse_book_page)
            # yield {
            #     # get the title attribute for the a tag
            #     "name": book.css("h3 a").attrib["title"],
            #     # get the text in the element with the class price_color
            #     "price": book.css(".product_price .price_color::text").get(),
            #     "url": book.css("h3 a").attrib["href"]
            # }

        next_page = response.css("li.next a").attrib["href"]

        if next_page is not None:
            if "catalogue" in next_page:
                next_page_url = "https://books.toscrape.com/" + next_page
            else:
                next_page_url = "https://books.toscrape.com/catalogue/" + next_page
            yield response.follow(next_page_url, callback=self.parse)

    def parse_book_page(self, response):
        table_rows = response.css("table tr")
        book_item = BookItem()
        book_item["url"] = response.url
        book_item["title"] = response.css(".product_main h1::text").get()
        book_item["product_type"] = table_rows[1].css("td::text").get()
        book_item["price_excl_tax"] = table_rows[2].css("td::text").get()
        book_item["price_incl_tax"] = table_rows[3].css("td::text").get()
        book_item["tax"] = table_rows[4].css("td::text").get()
        book_item["availability"] = table_rows[5].css("td::text").get()
        book_item["stars"] = response.css("p.star-rating").attrib["class"].split()[1]
        book_item["category"] = response.xpath("//ul[@class='breadcrumb']/li[@class='active']/preceding-sibling::li[1]/a/text()").get()
        book_item["description"] = response.xpath("//div[@id='product_description']/following-sibling::p/text()").get()
        book_item["price"] = response.css("p.price_color::text").get()

        yield book_item
