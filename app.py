from flask import Flask, flash, render_template, request, \
    Response, url_for, redirect, send_file
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

@app.route('/', methods=['GET','POST'])
def index():

    if request.method == "POST":
        idx_plt = request.form.get("idx_plt", None)
        if idx_plt!=None:
            return render_template("index.html", idx_plt = idx_plt)

    return render_template('index.html')


if __name__ == '__main__':
    app.run()