from flask import Flask, render_template_string
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

def db():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASS"),
        database=os.getenv("MYSQL_DB")
    )

@app.route("/")
def index():
    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute("SHOW TABLES;")
    tables = [row[f"Tables_in_{os.getenv('MYSQL_DB')}"] for row in cur.fetchall()]
    cur.close(); cn.close()

    return render_template_string("""
    <h1>Zendesk Backup DB</h1>
    <ul>
    {% for t in tables %}
      <li><a href="/table/{{t}}">{{t}}</a></li>
    {% endfor %}
    </ul>
    """, tables=tables)

@app.route("/table/<name>")
def show_table(name):
    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute(f"SELECT * FROM {name} LIMIT 50;")
    rows = cur.fetchall()
    cur.close(); cn.close()

    return render_template_string("""
    <h2>{{name}}</h2>
    <table border=1>
    <tr>{% for c in rows[0].keys() %}<th>{{c}}</th>{% endfor %}</tr>
    {% for r in rows %}
      <tr>{% for v in r.values() %}<td>{{v}}</td>{% endfor %}</tr>
    {% endfor %}
    </table>
    <p><a href="/">Back</a></p>
    """, name=name, rows=rows)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
