from flask import Flask, render_template, request
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)

# Replace 'your_database_url_here' with your SQLite database URL
db_url = 'sqlite:///dataset/nifty'
engine = create_engine(db_url)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/result', methods=['POST'])
def result():
    symbol = request.form.get('symbol')
    # Fetch data from the database using SQLAlchemy
    query = f"SELECT * FROM '{symbol}'"
    df = pd.read_sql(query, engine)
    return render_template('index.html', symbol=symbol, data=df.to_html(classes='table table-striped table-bordered'))


if __name__ == '__main__':
    app.run(debug=True)
