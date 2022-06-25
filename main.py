import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import json
import os
from dotenv import load_dotenv


Base = declarative_base()

class Publisher(Base):
    __tablename__ = 'publishers'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=60), unique=True)
    

class Stock(Base):
    __tablename__ = 'stocks'

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('books.id'), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shops.id'), nullable=False)
    count = sq.Column(sq.Integer)

class Shop(Base):
    __tablename__ = 'shops'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=60), unique=True)

    books = relationship(Stock, backref='books')

class Book(Base):
    __tablename__ = 'books'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=100), unique=True)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey('publishers.id'), nullable=False)
    
    publishers = relationship(Publisher, backref='book')
    shops = relationship(Stock, backref='shop')

class Sale(Base):
    __tablename__ = 'sales'

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.DECIMAL)
    date_sale = sq.Column(sq.TIMESTAMP, nullable=False)
    count = sq.Column(sq.Integer)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stocks.id'), nullable=False)

    stocks = relationship(Stock, backref='stocks')

def create_tables(engine):

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

def _load_test_data(session):
    PATH = os.path.join(os.getcwd(), 'tests_data.json')
    # print(PATH)
    with open(PATH, 'r') as td:
        data = json.load(td)

    for d in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale
        }[d.get('model')]
        session.add(model(id=d.get('pk'), **d.get('fields')))
        session.commit()


load_dotenv()

driver = 'postgresql'
path_db = 'localhost:5432'

DNS = f"{driver}://{os.getenv('USER')}:{os.getenv('PASSWORD')}@{path_db}/bookstock_db"
engine = sq.create_engine(DNS)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

_load_test_data(session)

# Выводит издателя (publisher), имя или идентификатор которого принимается через input().
name = input('Input name or id publisher  ')
if name.isnumeric():
    q = session.query(Publisher).filter(Publisher.id == name)
else:
    q = session.query(Publisher).filter(Publisher.name.like(name))
for p in q.all():
    print(p.id, p.name)

# Выводит издателя, у которого были проданы книги в период с 24/10 по 26/10
# subq1 = session.query(Sale).filter(Sale.date_sale.between('2018-10-24', '2018-10-26')).subquery('data_sale_from_sale')
subq = session.query(Stock).join(Sale).filter(Sale.date_sale.between('9:50', '9:59')).subquery('data_sale_from_sale')
q_full = session.query(Publisher).join(Book).filter(Book.id == subq.c.id_book)
for b in q_full.all():
    print(f"\t {b.id}   {b.name}")
    # for p in b.id_book:
    #     print(f"\t \t {p.id}, {p.count}")

session.close()

