# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
import sqlalchemy as db
from sqlalchemy import insert, select, func, exists
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field in field_names:
            if field != "description":
                adapter[field] = adapter.get(field).strip()

        # Switch category and product_type to lowercase
        lowercase_keys = ["category", "product_type"]
        for key in lowercase_keys:
            value = adapter.get(key)
            adapter[key] = value.lower()

        # Convert price to float
        price_keys = ["price", "price_excl_tax", "price_incl_tax", "tax"]
        for key in price_keys:
            value = adapter.get(key)
            value = value.replace("£", "")
            adapter[key] = float(value)

        # availability, extract the number of books in stock
        availability_str = adapter.get("availability")
        stock_count = re.search(r".*\((\d+).*\)", availability_str)
        stock_count = stock_count.group(1)
        if stock_count:
            adapter["availability"] = int(stock_count)
        else:
            adapter["availability"] = 0

        # Convert star rating to number
        rating = adapter.get("stars").lower()
        match rating:
            case "zero":
                adapter["stars"] = 0
            case "one":
                adapter["stars"] = 1
            case "two":
                adapter["stars"] = 2
            case "three":
                adapter["stars"] = 3
            case "four":
                adapter["stars"] = 4
            case "five":
                adapter["stars"] = 5

        return item


class SaveToSQLitePipeline:
    def __init__(self) -> None:
        self.db_name = "books"
        self.db_path = f"sqlite:///{self.db_name}.sqlite"
        self.engine = db.create_engine(self.db_path)
        self.metadata = db.MetaData()
        self.create_tables()

    def create_tables(self):
        """This method creates tables in sqlite database."""
        engine = self.engine
        metadata = self.metadata
        db.Table(
            "book",
            metadata,
            db.Column("book_id", db.Integer, autoincrement=True, primary_key=True),
            db.Column("url", db.String, nullable=False),
            db.Column("title", db.String, nullable=False),
            db.Column("product_type", db.String, nullable=False),
            db.Column("price_excl_tax", db.Float, nullable=False),
            db.Column("price_incl_tax", db.Float, nullable=False),
            db.Column("tax", db.Float, nullable=False),
            db.Column("availability", db.Integer, nullable=False),
            db.Column("stars", db.Integer, nullable=False),
            db.Column("category", db.String, nullable=False),
            db.Column("description", db.String, nullable=False),
            db.Column("price", db.Float, nullable=False)
        )

        metadata.create_all(engine)

    def process_item(self, item, spider):
        engine = self.engine
        table = self.metadata.tables["book"]
        with engine.begin() as conn:
            exist = exists().where(table.c.title == item["title"])
            query = select(func.count()).select_from(table).where(exist)
            result = conn.execute(query)

            if result.first()[0] == 0:
                insert_query = insert(table).values(
                    url = item["url"],
                    title = item["title"],
                    product_type = item["product_type"],
                    price_excl_tax = item["price_excl_tax"],
                    price_incl_tax = item["price_incl_tax"],
                    tax = item["tax"],
                    availability = item["availability"],
                    stars = item["stars"],
                    category = item["category"],
                    description = item["description"],
                    price = item["price"]
                )
                conn.execute(insert_query)
        
        return item

    def close_spider(self, spider):
        # Clean up connections and in-mem objects here
        pass
