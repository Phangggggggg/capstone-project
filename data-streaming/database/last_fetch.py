from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlite3
Base = declarative_base()

class FetchTracker(Base):
    __tablename__ = 'last_fetch'
    symbol = Column(String, primary_key=True)
    last_fetch_date = Column(String)

DATABASE_URL = 'sqlite:///data-streaming/data/last_fetch.db'

# conn = sqlite3.connect('data-streaming/data/last_fetch.db')
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def setup_tracking_database():
    Base.metadata.create_all(engine)

def get_last_fetch_date(symbol):
    fetch_record = session.query(FetchTracker).filter_by(symbol=symbol).first()
    return fetch_record.last_fetch_date if fetch_record else None

def update_last_fetch_date(symbol, date):
    fetch_record = session.query(FetchTracker).filter_by(symbol=symbol).first()
    
    if fetch_record:
        fetch_record.last_fetch_date = date
    else:
        fetch_record = FetchTracker(symbol=symbol, last_fetch_date=date)
        session.add(fetch_record)
    
    session.commit()